import importlib
import sys
import types


def install_hotglue_sdk_stubs():
    """Provide the tiny SDK surface needed to import stream definitions."""
    sdk = types.ModuleType("hotglue_singer_sdk")
    typing = types.ModuleType("hotglue_singer_sdk.typing")
    streams = types.ModuleType("hotglue_singer_sdk.streams")

    class Property:
        def __init__(self, name, type_, **kwargs):
            self.name = name
            self.type_ = type_
            self.kwargs = kwargs

    class PropertiesList:
        def __init__(self, *properties):
            self.properties = properties

        def to_dict(self):
            return {
                "type": "object",
                "properties": {prop.name: {} for prop in self.properties},
            }

    class Stream:
        def __init__(self, tap=None):
            self._tap = tap
            self.config = getattr(tap, "config", {}) if tap is not None else {}
            self.logger = getattr(tap, "logger", None)

        def get_starting_replication_key_value(self, context):
            return None

    for name in [
        "StringType",
        "IntegerType",
        "NumberType",
        "BooleanType",
        "DateTimeType",
    ]:
        setattr(typing, name, type(name, (), {}))

    typing.Property = Property
    typing.PropertiesList = PropertiesList
    streams.Stream = Stream
    sdk.typing = typing

    sys.modules["hotglue_singer_sdk"] = sdk
    sys.modules["hotglue_singer_sdk.typing"] = typing
    sys.modules["hotglue_singer_sdk.streams"] = streams


install_hotglue_sdk_stubs()
stream_module = importlib.import_module("tap_extend.streams")


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


class FakeRequest:
    method = "GET"
    url = "https://example.test/resource"


class FakeHTTPResponse:
    def __init__(self, status_code, text="", headers=None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}
        self.request = FakeRequest()
        self.url = self.request.url

    def raise_for_status(self):
        if self.status_code >= 400:
            raise stream_module.requests.exceptions.HTTPError(response=self)


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return self.responses.pop(0)


class FakeReportStream:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def _request(self, url, params=None):
        self.requests.append({"url": url, "params": dict(params or {})})
        return FakeResponse(self.responses.pop(0))


DEADLOCK_BODY = (
    '{"Message": "Error getting order row list: Transaction (Process ID 232) '
    "was deadlocked on lock resources with another process and has been chosen "
    'as the deadlock victim. Rerun the transaction."}'
)


def test_request_retries_transient_extend_400s_every_configured_wait(monkeypatch):
    class Tap:
        config = {
            "request_timeout_seconds": 10,
        }

    stream_module.ExtendStream._next_request_at = 0
    sleeps = []
    monkeypatch.setattr(stream_module.time, "sleep", lambda seconds: sleeps.append(seconds))

    stream = stream_module.ExtendStream(tap=Tap())
    monkeypatch.setattr(stream, "_apply_client_throttle", lambda: None)
    session = FakeSession([
        FakeHTTPResponse(400, DEADLOCK_BODY),
        FakeHTTPResponse(400, DEADLOCK_BODY),
        FakeHTTPResponse(200, "{}"),
    ])
    stream._session = session

    response = stream._request(
        "https://api.example.test/RESTAPI/reports/TESTCLIENT/OrderRows",
        params={"pageNumber": 11},
    )

    assert response.status_code == 200
    assert len(session.calls) == 3
    assert sleeps == [10.0, 10.0]


def test_request_can_limit_transient_extend_400_attempts(monkeypatch):
    class Tap:
        config = {
            "request_timeout_seconds": 10,
        }

    stream_module.ExtendStream._next_request_at = 0
    sleeps = []
    monkeypatch.setattr(stream_module.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(stream_module, "RETRYABLE_CLIENT_ERROR_MAX_ATTEMPTS", 2)

    stream = stream_module.ExtendStream(tap=Tap())
    monkeypatch.setattr(stream, "_apply_client_throttle", lambda: None)
    session = FakeSession([
        FakeHTTPResponse(400, DEADLOCK_BODY),
        FakeHTTPResponse(400, DEADLOCK_BODY),
    ])
    stream._session = session

    try:
        stream._request(
            "https://api.example.test/RESTAPI/reports/TESTCLIENT/OrderRows",
            params={"pageNumber": 11},
        )
    except stream_module.requests.exceptions.HTTPError:
        pass
    else:
        raise AssertionError("Expected HTTPError after configured retry limit")

    assert len(session.calls) == 2
    assert sleeps == [10.0]


def test_iter_report_days_uses_full_day_window_and_paginates():
    fake_stream = FakeReportStream([
        {
            "orderHeaderList": [
                {"orderNumber": "1", "changeDate": "2026-04-15T12:00:00+02:00"}
            ],
            "paginationInfo": {"currentPage": 1, "totalPages": 2},
        },
        {
            "orderHeaderList": [
                {"orderNumber": "2", "changeDate": "2026-04-15T13:00:00+02:00"}
            ],
            "paginationInfo": {"currentPage": 2, "totalPages": 2},
        },
    ])

    records = list(
        stream_module._iter_report_days(
            fake_stream,
            "https://api.example.test/RESTAPI/reports/TESTCLIENT/OrderHeaders",
            "orderHeaderList",
            "2026-04-15T00:00:00Z",
            "2026-04-15T23:59:59Z",
        )
    )

    assert [record["orderNumber"] for record in records] == ["1", "2"]
    assert fake_stream.requests == [
        {
            "url": "https://api.example.test/RESTAPI/reports/TESTCLIENT/OrderHeaders",
            "params": {
                "pageNumber": 1,
                "changeDate": "2026-04-15T00:00:00",
                "toChangeDate": "2026-04-15T23:59:59",
            },
        },
        {
            "url": "https://api.example.test/RESTAPI/reports/TESTCLIENT/OrderHeaders",
            "params": {
                "pageNumber": 2,
                "changeDate": "2026-04-15T00:00:00",
                "toChangeDate": "2026-04-15T23:59:59",
            },
        },
    ]


def test_reports_order_headers_preserves_observed_header_report_fields(monkeypatch):
    class Tap:
        _extend_sync_upper_bound = "2026-04-15T23:59:59+00:00"
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
            "start_date": "2026-04-15T00:00:00Z",
        }

    sample = {
        "orderNumber": "ORDER-1001",
        "orderNumberExternal": "ORDER-1001",
        "orderNumberEndCustomer": None,
        "orderDate": "2026-04-15T23:52:04.107+02:00",
        "changeDate": "2026-04-15T23:52:05.583+02:00",
        "askedDeliveryDate": "2026-04-15T00:00:00+02:00",
        "orderType": "Normal",
        "orderStatus": "Incoming",
        "orderPaymentStatus": 0,
        "customerNumber": "2705237",
        "orderReference": "Example Customer",
        "invoiceEmail": "customer@example.test",
        "requestedForwarder": "Test Forwarder",
        "requestedTransportMode": "Test Transport Mode",
        "salesChannel": "Test Sales Channel",
        "paymentType": "TestPayment",
        "deliveryName1": "Example Customer",
        "deliveryAddress1": "Example Street 1",
        "deliveryPostalCode": "12345",
        "deliveryCity": "Example City",
        "deliveryCountryId": "SE",
        "invoiceName": "Example Customer",
        "invoiceAddress1": "Example Street 1",
        "invoicePostalCode": "12345",
        "invoiceCity": "Example City",
        "invoiceCountryId": "SE",
    }
    calls = []

    def fake_iter_report_days(
        stream,
        url,
        list_key,
        start_date,
        end_date,
    ):
        calls.append({
            "url": url,
            "list_key": list_key,
            "start_date": start_date,
            "end_date": end_date,
        })
        yield sample

    monkeypatch.setattr(stream_module, "_iter_report_days", fake_iter_report_days)

    stream = stream_module.ReportsOrderHeadersStream(tap=Tap())
    records = list(stream.get_records())

    assert calls == [{
        "url": "https://api.example.test/RESTAPI/reports/TESTCLIENT/OrderHeaders",
        "list_key": "orderHeaderList",
        "start_date": "2026-04-15",
        "end_date": "2026-04-15",
    }]
    assert records[0]["orderNumber"] == "ORDER-1001"
    assert records[0]["orderPaymentStatus"] == "0"
    assert records[0]["orderReference"] == "Example Customer"
    assert records[0]["requestedTransportMode"] == "Test Transport Mode"
    assert records[0]["deliveryAddress1"] == "Example Street 1"
    assert records[0]["invoiceCountryId"] == "SE"
    assert "customerName" not in records[0]
    assert "totalPrice" not in records[0]
    assert "currency" not in records[0]
    assert "warehouse" not in records[0]


def test_reports_order_rows_preserves_observed_row_report_fields(monkeypatch):
    class Tap:
        _extend_sync_upper_bound = "2026-04-15T23:59:59+00:00"
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
            "start_date": "2026-04-15T00:00:00Z",
        }

    sample = {
        "orderRowId": "69b27097-d6bd-450d-92a7-38ed30da5ef3",
        "position": 20,
        "subPosition": 0,
        "supplyMode": "Warehouse",
        "productNumber": "8744",
        "productName": "Test Product Name",
        "productUnitName": "ST",
        "orderQuantity": 1.0000,
        "price": 159.2000,
        "vatPercent": 25.000,
        "currencyId": "TST",
        "currencyExchangeRate": 1.000000000000,
        "expectedDeliveryDate": "2026-04-17T08:00:36+02:00",
        "shipDate": "2026-04-17T08:00:01+02:00",
        "backOrderHandling": "NONE",
        "notes": None,
        "orderRowStatus": "Incoming",
        "shipmentNumber": None,
        "warehouseShortName": "TESTCLIENT2",
        "orderNumber": "ORDER-1001",
        "productNotes": None,
        "handlingMark": None,
        "shippingMark": None,
        "batchNumber": None,
        "salesUnit": "ST",
        "salesUnitQuantity": 1.0000,
        "agreedOrderPickTime": "2026-04-16T08:00:36+02:00",
        "listPrice": 159.2000,
        "ordinalPrice": 159.2000,
        "productSalesUnitPrice": 159.2000,
        "productVisibility": "AlwaysVisible",
        "originalExpectedDeliveryDate": "2026-04-16T08:00:36+02:00",
        "orderReasonCode": "",
        "structuredCost": 0.0000,
        "exciseDutyCost": 0.0000,
        "customerBonusCost": 0.0000,
        "cost": 128.2400,
        "agreeedOrderRowId": "",
        "orderDate": "2026-04-15T23:52:04.107+02:00",
        "orderPriority": 10,
        "getBalanceFromAgreedOrder": True,
        "allocationStatus": "Physical",
        "releaseToWarehouseWhenAllocated": False,
        "changeDate": "2026-04-15T00:00:00+00:00",
    }
    calls = []

    def fake_iter_report_days(
        stream,
        url,
        list_key,
        start_date,
        end_date,
    ):
        calls.append({
            "url": url,
            "list_key": list_key,
            "start_date": start_date,
            "end_date": end_date,
        })
        yield sample

    monkeypatch.setattr(stream_module, "_iter_report_days", fake_iter_report_days)

    stream = stream_module.ReportsOrderRowsStream(tap=Tap())
    records = list(stream.get_records())

    assert calls == [{
        "url": "https://api.example.test/RESTAPI/reports/TESTCLIENT/OrderRows",
        "list_key": "orderRowList",
        "start_date": "2026-04-15",
        "end_date": "2026-04-15",
    }]
    assert records[0]["orderRowId"] == "69b27097-d6bd-450d-92a7-38ed30da5ef3"
    assert records[0]["orderQuantity"] == 1.0000
    assert records[0]["price"] == 159.2000
    assert records[0]["currencyId"] == "TST"
    assert records[0]["warehouseShortName"] == "TESTCLIENT2"
    assert records[0]["allocationStatus"] == "Physical"
    assert "quantity" not in records[0]
    assert "unitPrice" not in records[0]
    assert "currency" not in records[0]
    assert "warehouse" not in records[0]


def test_reports_state_date_range_overrides_start_date_and_sync_upper_bound(monkeypatch):
    class Tap:
        _extend_sync_upper_bound = "2026-04-20T23:59:59+00:00"
        state = {
            "bookmarks": {
                "reports_order_rows": {
                    "replication_key": "changeDate",
                    "replication_key_value": "2026-04-19T00:00:00Z",
                }
            },
            "reports_start_date": "2026-01-01",
            "reports_end_date": "2026-01-31",
        }
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
            "start_date": "2024-01-01T00:00:00Z",
        }

    calls = []

    def fake_iter_report_days(
        stream,
        url,
        list_key,
        start_date,
        end_date,
    ):
        calls.append({
            "url": url,
            "list_key": list_key,
            "start_date": start_date,
            "end_date": end_date,
        })
        return iter(())

    monkeypatch.setattr(stream_module, "_iter_report_days", fake_iter_report_days)

    stream = stream_module.ReportsOrderRowsStream(tap=Tap())
    records = list(stream.get_records())

    assert records == []
    assert calls == [{
        "url": "https://api.example.test/RESTAPI/reports/TESTCLIENT/OrderRows",
        "list_key": "orderRowList",
        "start_date": "2026-01-01",
        "end_date": "2026-01-31",
    }]


def test_reports_state_date_range_reads_sdk_stream_tap_state():
    class Stream:
        name = "reports_order_rows"
        tap_state = {
            "bookmarks": {
                "reports_order_rows": {
                    "replication_key": "changeDate",
                    "replication_key_value": "2026-04-19T00:00:00Z",
                }
            },
            "reports_start_date": "2026-01-01",
            "reports_end_date": "2026-01-31",
        }

    assert stream_module._report_state_date_range(Stream()) == (
        "2026-01-01",
        "2026-01-31",
    )


def test_top_level_reports_state_range_takes_precedence(monkeypatch):
    class Tap:
        _extend_sync_upper_bound = "2026-04-20T23:59:59+00:00"
        state = {
            "reports_start_date": "2026-03-01",
            "reports_end_date": "2026-03-31",
            "bookmarks": {
                "reports_order_headers": {
                    "reports_start_date": "2026-02-01",
                    "reports_end_date": "2026-02-28",
                },
            }
        }
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
            "start_date": "2024-01-01T00:00:00Z",
        }

    calls = []

    def fake_iter_report_days(
        stream,
        url,
        list_key,
        start_date,
        end_date,
    ):
        calls.append({
            "url": url,
            "list_key": list_key,
            "start_date": start_date,
            "end_date": end_date,
        })
        return iter(())

    monkeypatch.setattr(stream_module, "_iter_report_days", fake_iter_report_days)

    stream = stream_module.ReportsOrderHeadersStream(tap=Tap())
    records = list(stream.get_records())

    assert records == []
    assert calls == [{
        "url": "https://api.example.test/RESTAPI/reports/TESTCLIENT/OrderHeaders",
        "list_key": "orderHeaderList",
        "start_date": "2026-03-01",
        "end_date": "2026-03-31",
    }]


def test_purchase_orders_uses_change_date_datetime_range(monkeypatch):
    class Tap:
        _extend_sync_upper_bound = "2026-04-20T17:18:46+00:00"
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
            "start_date": "2026-04-20T00:00:00Z",
        }
        warehouse_codes = None

    captured = []

    def fake_request(url, params=None):
        captured.append({"url": url, "params": dict(params or {})})

        class Response:
            def json(self):
                return {
                    "purchaseOrderList": [],
                    "paginationInfo": {"currentPage": 1, "totalPages": 1},
                }

        return Response()

    stream = stream_module.PurchaseOrdersStream(tap=Tap())
    monkeypatch.setattr(stream, "_request", fake_request)

    assert list(stream.get_records()) == []
    assert captured == [{
        "url": "https://api.example.test/RESTAPI/v1_0/TESTCLIENT/PurchaseOrders",
        "params": {
            "changeDateFrom": "2026-04-20T00:00:00",
            "changeDateTo": "2026-04-20T17:18:46",
            "pageNumber": 1,
        },
    }]


def test_purchase_orders_missing_detail_400_falls_back_to_summary(monkeypatch):
    class Tap:
        _extend_sync_upper_bound = "2026-04-22T11:20:00+00:00"
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
            "start_date": "2026-04-22T00:00:00Z",
        }
        warehouse_codes = None

    purchase_summary = {
        "purchaseNumber": "RP-404",
        "status": "Ordered",
        "createDate": "2026-04-22T10:49:27.887+02:00",
        "warehouse": "TESTWH",
        "isOpen": True,
        "isReceived": False,
        "externalOrderNumber": "",
        "supplierNumber": "109",
        "supplierName": "Supplier Example",
        "supplierOrderNumber": "",
        "shippedDate": None,
        "changeDate": "2026-04-22T10:50:46.963",
    }

    def fake_request(url, params=None):
        if url.endswith("/PurchaseOrders"):
            return FakeResponse(
                {
                    "purchaseOrderList": [purchase_summary],
                    "paginationInfo": {"currentPage": 1, "totalPages": 1},
                }
            )
        response = FakeHTTPResponse(
            400,
            '{"Message": "Error getting purchase order: There is no row at position 0."}',
        )
        response.raise_for_status()
        raise AssertionError("unreachable")

    stream = stream_module.PurchaseOrdersStream(tap=Tap())
    monkeypatch.setattr(stream, "_request", fake_request)

    records = list(stream.get_records())

    assert len(records) == 1
    assert records[0]["purchaseNumber"] == "RP-404"
    assert records[0]["rows"] == "[]"
    assert records[0]["shipments"] == "[]"
    assert records[0]["supplierAgreementNumber"] is None

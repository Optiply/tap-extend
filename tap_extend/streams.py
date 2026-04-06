"""Stream definitions for tap-extend.

Streams:
  - SuppliersStream:                  GET /Supplier                  (FULL_TABLE)
  - SupplierAgreementsStream:         GET /SupplierAgreement         (FULL_TABLE, active=true)
  - ProductSupplierAgreementsStream:  GET /ProductSupplierAgreements (FULL_TABLE)
  - ProductsStream:                   GET /Products                  (INCREMENTAL, modifiedDateFrom)
  - ProductAvailabilityStream:        GET /ProductAvailability       (INCREMENTAL, modifiedDateFrom)
  - CustomerOrdersStream:             GET /CustomerOrders            (INCREMENTAL, modifiedDateFrom)
  - PurchaseOrdersStream:             GET /PurchaseOrders            (INCREMENTAL, createDateFrom)
  - ReportsOrderHeadersStream:        GET /reports/{client}/OrderHeaders  (INCREMENTAL, changeDate day-by-day)
  - ReportsOrderRowsStream:           GET /reports/{client}/OrderRows     (INCREMENTAL, changeDate day-by-day)

All streams share ExtendStream base class for auth/HTTP/state handling.

Pagination:
  - Products, CustomerOrders: pageCount + pageOffset
  - Supplier, SupplierAgreement, ProductSupplierAgreements, PurchaseOrders, ProductAvailability: pageNumber (1-based)
  - ReportsOrderHeaders, ReportsOrderRows: pageNumber (1-based) per day — iterates one day at a time,
    paginating all pages within each day before advancing to the next.
"""

from __future__ import annotations

import base64
import json
import logging
from email.utils import parsedate_to_datetime
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

import backoff
import requests

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.streams import Stream

try:
    from hotglue_singer_sdk.exceptions import InvalidCredentialsError
except ImportError:
    class InvalidCredentialsError(Exception):
        """Raised when API credentials are invalid."""


logger = logging.getLogger(__name__)


class _RetryableError(Exception):
    """Raised for errors that should trigger backoff retry (429, 5xx)."""


# ---------------------------------------------------------------------------
# Shared base class
# ---------------------------------------------------------------------------


class ExtendStream(Stream):
    """Base class for all Extend Commerce streams."""

    _session: Optional[requests.Session] = None
    _next_request_at = 0.0

    @property
    def session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
            credentials = base64.b64encode(
                f"{self.config['username']}:{self.config['password']}".encode("utf-8")
            ).decode("utf-8")
            self._session.headers.update({
                "ExtendBasicAuthorization": f"Basic {credentials}",
                "Accept": "application/json",
            })
            # Extend uses header-based auth only. Disable cookies to prevent
            # accumulation over long syncs (hundreds of pages) which can cause
            # the server to reject requests with 400.
            self._session.cookies.set_policy(
                __import__("http.cookiejar", fromlist=["DefaultCookiePolicy"])
                .DefaultCookiePolicy(allowed_domains=[])
            )
        return self._session

    @property
    def base_url(self) -> str:
        api_url = self.config.get("api_url", "https://s05.extend.se/RESTAPI").rstrip("/")
        return f"{api_url}/v1_0/{self.config['client']}"

    @property
    def requests_per_second(self) -> float:
        raw_value = self.config.get("requests_per_second", 4)
        try:
            requests_per_second = float(raw_value)
        except (TypeError, ValueError):
            logger.warning(
                "Invalid requests_per_second=%r. Falling back to 4 requests/second.",
                raw_value,
            )
            requests_per_second = 4.0
        return max(requests_per_second, 0.1)

    @property
    def request_timeout(self) -> float:
        """Per-request timeout in seconds. Default 120, configurable via request_timeout_seconds."""
        raw = self.config.get("request_timeout_seconds", 120)
        try:
            return max(float(raw), 10.0)
        except (TypeError, ValueError):
            return 120.0

    def _apply_client_throttle(self) -> None:
        min_interval = 1.0 / self.requests_per_second
        now = time.monotonic()
        sleep_for = ExtendStream._next_request_at - now
        if sleep_for > 0:
            time.sleep(sleep_for)
            now = time.monotonic()
        ExtendStream._next_request_at = max(ExtendStream._next_request_at, now) + min_interval

    def _delay_from_retry_after(self, retry_after: Optional[str]) -> Optional[float]:
        if not retry_after:
            return None
        try:
            return max(float(retry_after), 0.0)
        except (TypeError, ValueError):
            try:
                retry_at = parsedate_to_datetime(retry_after)
            except (TypeError, ValueError):
                return None
            return max(retry_at.timestamp() - time.time(), 0.0)

    def _delay_from_reset_header(self, response: requests.Response) -> Optional[float]:
        reset_value = response.headers.get("x-ratelimit-reset")
        if not reset_value:
            return None
        try:
            reset_epoch = float(reset_value)
        except (TypeError, ValueError):
            return None
        return max(reset_epoch - time.time(), 0.0) + 1.0

    def _defer_next_request(self, delay_seconds: Optional[float]) -> None:
        if not delay_seconds or delay_seconds <= 0:
            return
        ExtendStream._next_request_at = max(
            ExtendStream._next_request_at,
            time.monotonic() + delay_seconds,
        )

    def _is_retryable_client_error(self, response: requests.Response) -> bool:
        """Return True for known transient 4xx responses misclassified by Extend."""
        if response.status_code != 400:
            return False

        message = (response.text or "").lower()
        if "deadlock" in message and "rerun the transaction" in message:
            return True
        if "timeout" in message and ("expired" in message or "execution" in message):
            return True
        return False

    def _is_retryable_client_error(self, response: requests.Response) -> bool:
        """Return True for known transient 4xx responses misclassified by Extend."""
        if response.status_code != 400:
            return False

        message = (response.text or "").lower()
        if "deadlock" in message and "rerun the transaction" in message:
            return True
        if "timeout" in message and ("expired" in message or "execution" in message):
            return True
        return False

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.ConnectionError, requests.exceptions.Timeout, _RetryableError),
        max_tries=8,
        factor=2,
        jitter=backoff.full_jitter,
    )
    def _request(self, url: str, params: Optional[dict] = None) -> requests.Response:
        """GET with retry/backoff.  Only retries on 429, 5xx, connection errors, and transient 400s."""
        self._apply_client_throttle()
        response = self.session.get(url, params=params, timeout=self.request_timeout)

        if response.status_code == 401:
            raise InvalidCredentialsError(
                f"Authentication failed (401): {response.text[:300]}"
            )
        if response.status_code == 429:
            retry_after = self._delay_from_retry_after(response.headers.get("Retry-After"))
            reset_delay = self._delay_from_reset_header(response)
            delay_seconds = max(
                retry_after if retry_after is not None else 0.0,
                reset_delay if reset_delay is not None else 0.0,
                30.0,
            )
            logger.warning("Rate limited (429). Sleeping %.1fs.", delay_seconds)
            time.sleep(delay_seconds)
            self._defer_next_request(delay_seconds)
            raise _RetryableError("Rate limited (429)")
        if response.status_code >= 500:
            raise _RetryableError(
                f"Server error ({response.status_code}): {response.text[:300]}"
            )
        if self._is_retryable_client_error(response):
            logger.warning(
                "Retrying transient Extend client error (%s): %s",
                response.status_code,
                response.text[:300],
            )
            raise _RetryableError(
                f"Transient client error ({response.status_code}): {response.text[:300]}"
            )

        if response.headers.get("x-ratelimit-remaining") == "0":
            self._defer_next_request(self._delay_from_reset_header(response))

        # 4xx (except 429 handled above) are client errors — log body for diagnosis, then fail
        if response.status_code >= 400:
            logger.error(
                "%s %s -> %d: %s",
                response.request.method,
                response.url,
                response.status_code,
                response.text[:500],
            )
        response.raise_for_status()
        return response

    def _write_state_message(self) -> None:
        """Clean partitions from state to avoid bloat."""
        try:
            tap_state = getattr(getattr(self, "_tap", None), "state", {}) or {}
            for stream_state in tap_state.get("bookmarks", {}).values():
                stream_state.pop("partitions", None)
            super()._write_state_message()
        except Exception as exc:
            self.logger.warning("Error writing state message: %s", exc)


# ---------------------------------------------------------------------------
# SuppliersStream  —  GET /Supplier
# ---------------------------------------------------------------------------


class SuppliersStream(ExtendStream):
    """Extend Commerce Suppliers (company-level).

    Schema from SupplierListItem definition.
    FULL_TABLE — no date filter available on this endpoint.
    Pagination: pageNumber (1-based).
    Response wrapper key: SupplierList.
    """

    name = "suppliers"
    primary_keys = ["supplierNumber"]
    replication_method = "FULL_TABLE"

    schema = th.PropertiesList(
        th.Property("supplierNumber", th.StringType),
        th.Property("name", th.StringType),
        th.Property("shortName", th.StringType),
        th.Property("organizationNumber", th.StringType),
        th.Property("address1", th.StringType),
        th.Property("address2", th.StringType),
        th.Property("address3", th.StringType),
        th.Property("postalCode", th.StringType),
        th.Property("city", th.StringType),
        th.Property("state", th.StringType),
        th.Property("countryId", th.StringType),
        th.Property("companyPhone", th.StringType),
        th.Property("rating", th.StringType),
        th.Property("manualRating", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        page = 1
        while True:
            data = self._request(
                f"{self.base_url}/Supplier", params={"pageNumber": page}
            ).json()
            items = data.get("SupplierList", [])
            pagination = data.get("paginationInfo", {})

            for s in items:
                yield {
                    "supplierNumber": s.get("supplierNumber"),
                    "name": s.get("name"),
                    "shortName": s.get("shortName"),
                    "organizationNumber": s.get("organizationNumber"),
                    "address1": s.get("address1"),
                    "address2": s.get("address2"),
                    "address3": s.get("address3"),
                    "postalCode": s.get("postalCode"),
                    "city": s.get("city"),
                    "state": s.get("state"),
                    "countryId": s.get("countryId"),
                    "companyPhone": s.get("companyPhone"),
                    "rating": s.get("rating"),
                    "manualRating": s.get("manualRating"),
                }

            total_pages = pagination.get("totalPages", 0)
            if page >= total_pages:
                break
            page += 1


# ---------------------------------------------------------------------------
# SupplierAgreementsStream  —  GET /SupplierAgreement
# ---------------------------------------------------------------------------


class SupplierAgreementsStream(ExtendStream):
    """Extend Commerce Supplier Agreements (contract/brand level).

    These are the entities used as Optiply Suppliers — they carry lead times,
    currency and payment terms needed for replenishment. Each agreement maps
    to one purchasing contract (e.g. "Gandalf - Corsair", "Fifine").

    NOTE: supplierNumber (FK to parent Supplier) is NOT returned by the list
    endpoint — only by GET /SupplierAgreement/{id}. Omitted here to avoid
    525 extra detail calls. Add if the ETL needs the company-level grouping.

    Filters active=true per Xavier's requirements (2026-03-06 email).

    Schema from SupplierAgreementListItem definition.
    FULL_TABLE — no change-date filter available on this endpoint.
    Pagination: pageNumber (1-based).
    Response wrapper key: SupplierAgreementList.
    """

    name = "supplier_agreements"
    primary_keys = ["supplierAgreementNumber"]
    replication_method = "FULL_TABLE"

    schema = th.PropertiesList(
        th.Property("supplierAgreementNumber", th.IntegerType),
        th.Property("name", th.StringType),
        # supplierNumber (FK to Supplier) only on detail endpoint — not in list response
        th.Property("active", th.BooleanType),
        th.Property("currencyId", th.StringType),
        th.Property("manufacturingLeadTime", th.NumberType),
        th.Property("transportLeadTime", th.NumberType),
        th.Property("customsLeadTime", th.NumberType),
        th.Property("paymentTerms", th.StringType),
        th.Property("deliveryMethod", th.StringType),
        th.Property("incoterms", th.StringType),
        th.Property("validFrom", th.DateTimeType),
        th.Property("validTo", th.DateTimeType),
        th.Property("ordererAddress1", th.StringType),
        th.Property("ordererPostalCode", th.StringType),
        th.Property("ordererCity", th.StringType),
        th.Property("ordererCountryId", th.StringType),
        th.Property("makeAutomaticPurchase", th.BooleanType),
        th.Property("capacity", th.IntegerType),
    ).to_dict()

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        page = 1
        while True:
            data = self._request(
                f"{self.base_url}/SupplierAgreement",
                params={"pageNumber": page, "active": "true"},
            ).json()
            items = data.get("SupplierAgreementList", [])
            pagination = data.get("paginationInfo", {})

            for a in items:
                yield {
                    "supplierAgreementNumber": a.get("supplierAgreementNumber"),
                    "name": a.get("name"),
                    "active": a.get("active"),
                    "currencyId": a.get("currencyId"),
                    "manufacturingLeadTime": a.get("manufacturingLeadTime"),
                    "transportLeadTime": a.get("transportLeadTime"),
                    "customsLeadTime": a.get("customsLeadTime"),
                    "paymentTerms": a.get("paymentTerms"),
                    "deliveryMethod": a.get("deliveryMethod"),
                    "incoterms": a.get("incoterms"),
                    "validFrom": a.get("validFrom"),
                    "validTo": a.get("validTo"),
                    "ordererAddress1": a.get("ordererAddress1"),
                    "ordererPostalCode": a.get("ordererPostalCode"),
                    "ordererCity": a.get("ordererCity"),
                    "ordererCountryId": a.get("ordererCountryId"),
                    "makeAutomaticPurchase": a.get("makeAutomaticPurchase"),
                    "capacity": a.get("capacity"),
                }

            total_pages = pagination.get("totalPages", 0)
            if page >= total_pages:
                break
            page += 1


# ---------------------------------------------------------------------------
# ProductSupplierAgreementsStream  —  GET /ProductSupplierAgreements
# ---------------------------------------------------------------------------


class ProductSupplierAgreementsStream(ExtendStream):
    """Extend Commerce product↔SupplierAgreement links.

    One record per (product, supplier agreement) pair. Used as the primary
    source for Optiply SupplierProducts — replaces the per-product detail
    call that was previously embedded in ProductsStream.

    FULL_TABLE — no date filter available on this endpoint.
    Pagination: pageNumber (1-based).
    Response wrapper key: productSupplierAgreementList.
    """

    name = "product_supplier_agreements"
    primary_keys = ["productNumber", "supplierAgreementNumber"]
    replication_method = "FULL_TABLE"

    schema = th.PropertiesList(
        th.Property("productNumber", th.StringType),
        th.Property("supplierAgreementNumber", th.IntegerType),
        th.Property("supplierAgreementName", th.StringType),
        th.Property("supplierName", th.StringType),
        th.Property("supplierAgreementCurrencyId", th.StringType),
        th.Property("supplierProductNumber", th.StringType),
        th.Property("supplierProductName", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("vatPercent", th.NumberType),
        th.Property("manufacturingLeadTimeHour", th.NumberType),
        th.Property("supplierAgreementProductionLeadtimeHours", th.IntegerType),
        th.Property("supplierAgreementTransportLeadtimeHours", th.IntegerType),
        th.Property("inactive", th.BooleanType),
        th.Property("statisticalNumber", th.StringType),
        th.Property("country", th.StringType),
        th.Property("productUnitId", th.StringType),
        th.Property("useOtherPurchaseUnit", th.BooleanType),
        th.Property("purchaseProductUnit", th.StringType),
        th.Property("quantityPerPurchaseProductUnit", th.NumberType),
    ).to_dict()

    def _map(self, a: dict) -> dict:
        return {
            "productNumber": a.get("productNumber"),
            "supplierAgreementNumber": a.get("supplierAgreementNumber"),
            "supplierAgreementName": a.get("supplierAgreementName"),
            "supplierName": a.get("supplierName"),
            "supplierAgreementCurrencyId": a.get("supplierAgreementCurrencyId"),
            "supplierProductNumber": a.get("supplierProductNumber"),
            "supplierProductName": a.get("supplierProductName"),
            "price": a.get("price"),
            "vatPercent": a.get("vatPercent"),
            "manufacturingLeadTimeHour": a.get("manufacturingLeadTimeHour"),
            "supplierAgreementProductionLeadtimeHours": a.get("supplierAgreementProductionLeadtimeHours"),
            "supplierAgreementTransportLeadtimeHours": a.get("supplierAgreementTransportLeadtimeHours"),
            "inactive": a.get("inactive"),
            "statisticalNumber": a.get("statisticalNumber"),
            "country": a.get("country"),
            "productUnitId": a.get("productUnitId"),
            "useOtherPurchaseUnit": a.get("useOtherPurchaseUnit"),
            "purchaseProductUnit": a.get("purchaseProductUnit"),
            "quantityPerPurchaseProductUnit": a.get("quantityPerPurchaseProductUnit"),
        }

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        url = f"{self.base_url}/ProductSupplierAgreements"

        # Try global paginated list first
        try:
            page = 1
            while True:
                data = self._request(url, params={"pageNumber": page}).json()
                items = data.get("productSupplierAgreementList", [])
                pagination = data.get("paginationInfo", {})
                for a in items:
                    yield self._map(a)
                total_pages = pagination.get("totalPages", 0)
                if page >= total_pages:
                    break
                page += 1
            return
        except requests.exceptions.HTTPError as exc:
            if exc.response is None or exc.response.status_code != 400:
                raise
            logger.warning(
                "ProductSupplierAgreements: global list returned 400 — "
                "falling back to per-supplierAgreement iteration"
            )

        # Fallback: iterate by productNumber (fewer calls than supplierAgreementNumber)
        seen_products: set = set()
        p_offset = 0
        p_count = 100
        while True:
            product_list = self._request(
                f"{self.base_url}/Products",
                params={"pageCount": p_count, "pageOffset": p_offset},
            ).json()
            if not isinstance(product_list, list) or not product_list:
                break
            for p in product_list:
                pn = str(p.get("productNumber") or "")
                if not pn or pn in seen_products:
                    continue
                seen_products.add(pn)
                try:
                    psa_page = 1
                    while True:
                        psa_data = self._request(
                            url, params={"productNumber": pn, "pageNumber": psa_page}
                        ).json()
                        for a in psa_data.get("productSupplierAgreementList", []):
                            yield self._map(a)
                        psa_total = psa_data.get("paginationInfo", {}).get("totalPages", 0)
                        if psa_page >= psa_total:
                            break
                        psa_page += 1
                except requests.exceptions.HTTPError as e:
                    logger.warning("PSA: skipping productNumber=%s (%s)", pn, e)
            if len(product_list) < p_count:
                break
            p_offset += 1

        logger.info("ProductSupplierAgreements: per-product iteration done (%d products)", len(seen_products))


# ---------------------------------------------------------------------------
# ProductsStream  —  GET /Products
# ---------------------------------------------------------------------------


class ProductsStream(ExtendStream):
    """Extend Commerce Products with per-warehouse stock.

    List endpoint (GET /Products) returns one row per product-per-warehouse.
    This stream deduplicates by productNumber, aggregating warehouse stock
    into a JSON array.

    Supplier-product links are handled by ProductSupplierAgreementsStream
    (GET /ProductSupplierAgreements) — no per-product detail calls needed here.

    Incremental via modifiedDateFrom server-side filter.
    Pagination: pageCount + pageOffset (NOT pageNumber).
    Replication key: createDate (only date field present on list response).

    Schema from ProductListItem definition.
    """

    name = "products"
    primary_keys = ["productNumber"]
    replication_key = "createDate"
    replication_method = "INCREMENTAL"

    schema = th.PropertiesList(
        # ProductListItem fields
        th.Property("productNumber", th.StringType),
        th.Property("productName", th.StringType),
        th.Property("createDate", th.DateTimeType),
        th.Property("productUnit", th.StringType),
        th.Property("cost", th.NumberType),
        th.Property("currency", th.StringType),
        th.Property("countryOfOrigin", th.StringType),
        th.Property("supplyMode", th.StringType),
        th.Property("manufacturer", th.StringType),
        th.Property("manufacturerProductNumber", th.StringType),
        th.Property("gtinNumberList", th.StringType),
        th.Property("enabled", th.BooleanType),
        th.Property("statisticalCategory1", th.StringType),
        th.Property("statisticalCategory2", th.StringType),
        th.Property("statisticalCategory3", th.StringType),
        th.Property("companyGroup", th.StringType),
        th.Property("financialCategory", th.StringType),
        # Aggregated per-warehouse stock as JSON: [{warehouse, availableBalance}]
        th.Property("warehouse_stock", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        seen: dict[str, dict] = {}
        stock_map: dict[str, list] = {}

        start_replication = self.get_starting_replication_key_value(context)
        if start_replication:
            modified_date_from = str(start_replication)
        elif self.config.get("start_date"):
            modified_date_from = str(self.config["start_date"])
        else:
            modified_date_from = None

        end_date = self.config.get("end_date")

        page_offset = 0
        page_count = 100
        total_rows = 0

        while True:
            params: dict[str, Any] = {"pageCount": page_count, "pageOffset": page_offset}
            if modified_date_from:
                params["modifiedDateFrom"] = modified_date_from
            if end_date:
                params["modifiedDateTo"] = str(end_date)

            try:
                product_list = self._request(
                    f"{self.base_url}/Products", params=params
                ).json()
            except requests.exceptions.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 400 and page_offset > 0:
                    logger.warning(
                        "Products: 400 at pageOffset=%d after %d rows / %d unique products. "
                        "Yielding partial results. Response: %s",
                        page_offset, total_rows, len(seen),
                        exc.response.text[:500],
                    )
                    break
                raise

            if not isinstance(product_list, list) or not product_list:
                break

            total_rows += len(product_list)

            for p in product_list:
                pn = str(p.get("productNumber") or "")
                if not pn:
                    continue

                stock_map.setdefault(pn, []).append({
                    "warehouse": p.get("warehouse") or "",
                    "availableBalance": p.get("availableBalance") or 0,
                })

                if pn not in seen:
                    groups = p.get("productGroupsAndCategories") or {}
                    seen[pn] = {
                        "productNumber": pn,
                        "productName": p.get("productName"),
                        "createDate": p.get("createDate"),
                        "productUnit": p.get("productUnit"),
                        "cost": p.get("cost"),
                        "currency": p.get("currency"),
                        "countryOfOrigin": p.get("countryOfOrigin"),
                        "supplyMode": p.get("supplyMode"),
                        "manufacturer": p.get("manufacturer"),
                        "manufacturerProductNumber": p.get("manufacturerProductNumber"),
                        "gtinNumberList": p.get("gtinNumberList"),
                        "enabled": p.get("enabled"),
                        "statisticalCategory1": p.get("statisticalCategory1"),
                        "statisticalCategory2": p.get("statisticalCategory2"),
                        "statisticalCategory3": p.get("statisticalCategory3"),
                        "companyGroup": groups.get("companyGroup"),
                        "financialCategory": groups.get("financialCategory"),
                    }

            if len(product_list) < page_count:
                break
            page_offset += 1

            if page_offset % 50 == 0:
                logger.info(
                    "Products: page %d — %d rows fetched, %d unique products so far",
                    page_offset, total_rows, len(seen),
                )

        logger.info("Products: done — %d rows, %d unique products", total_rows, len(seen))

        for pn, record in seen.items():
            record["warehouse_stock"] = json.dumps(stock_map.get(pn, []))
            yield record


# ---------------------------------------------------------------------------
# ProductAvailabilityStream  —  GET /ProductAvailability
# ---------------------------------------------------------------------------


class ProductAvailabilityStream(ExtendStream):
    """Extend Commerce Product Availability (stock per product/warehouse).

    Separate endpoint from Products — provides availability including
    incoming stock and next receiving dates. One row per product/warehouse.

    Per Xavier's requirements (2026-03-06 email): stocks come from this
    endpoint, not from the Products list response.

    Incremental via modifiedDateFrom.
    Pagination: pageNumber (1-based).
    """

    name = "product_availability"
    primary_keys = ["productNumber", "warehouse"]
    replication_key = "changeDate"
    replication_method = "INCREMENTAL"

    schema = th.PropertiesList(
        th.Property("productNumber", th.StringType),
        th.Property("warehouse", th.StringType),
        th.Property("warehouseName", th.StringType),
        th.Property("physicalBalance", th.NumberType),
        th.Property("availableBalanceNow", th.NumberType),
        th.Property("blockedBalance", th.NumberType),
        th.Property("orderedQuantity", th.NumberType),
        th.Property("nextReceivingDate", th.DateTimeType),
        th.Property("quantityOnNextReceiving", th.NumberType),
        th.Property("totalExpectedReceivingFromPurchase", th.NumberType),
        th.Property("totalExpectedReceivingFromWarehouseTransfer", th.NumberType),
        th.Property("totalExpectedReceivingFromReturn", th.NumberType),
        th.Property("changeDate", th.DateTimeType),
    ).to_dict()

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        warehouse_codes = self._tap.warehouse_codes

        start_replication = self.get_starting_replication_key_value(context)
        params_base: dict[str, Any] = {}
        if start_replication:
            params_base["modifiedDateFrom"] = str(start_replication)
        elif self.config.get("start_date"):
            params_base["modifiedDateFrom"] = str(self.config["start_date"])

        page = 1
        while True:
            data = self._request(
                f"{self.base_url}/ProductAvailability",
                params={**params_base, "pageNumber": page},
            ).json()

            items = data.get("productAvailabilityList", [])
            pagination = data.get("paginationInfo", {})

            if not items:
                break

            for item in items:
                wh = item.get("warehouse") or ""
                if warehouse_codes and wh not in warehouse_codes:
                    continue

                yield {
                    "productNumber": str(item.get("productNumber") or ""),
                    "warehouse": wh,
                    "warehouseName": item.get("warehouseName"),
                    "physicalBalance": item.get("physicalBalance"),
                    "availableBalanceNow": item.get("availableBalanceNow"),
                    "blockedBalance": item.get("blockedBalance"),
                    "orderedQuantity": item.get("orderedQuantity"),
                    "nextReceivingDate": item.get("nextReceivingDate"),
                    "quantityOnNextReceiving": item.get("quantityOnNextReceiving"),
                    "totalExpectedReceivingFromPurchase": item.get("totalExpectedReceivingFromPurchase"),
                    "totalExpectedReceivingFromWarehouseTransfer": item.get("totalExpectedReceivingFromWarehouseTransfer"),
                    "totalExpectedReceivingFromReturn": item.get("totalExpectedReceivingFromReturn"),
                    "changeDate": item.get("changeDate"),
                }

            total_pages = pagination.get("totalPages", 0)
            if page >= total_pages:
                break
            page += 1


# ---------------------------------------------------------------------------
# CustomerOrdersStream  —  GET /CustomerOrders  +  GET /CustomerOrders/{id}
# ---------------------------------------------------------------------------


class CustomerOrdersStream(ExtendStream):
    """Extend Commerce CustomerOrders with full detail (header + rows).

    List endpoint returns CustomerOrderListItem summaries.
    Detail endpoint is called per order to get orderRows.
    Rows serialised as JSON string for ETL processing.

    CRITICAL: Always pass modifiedDateFrom — without it the endpoint times out.
    Pagination: pageCount + pageOffset (NOT pageNumber).
    Replication key: changeDate (from CustomerOrderListItem).

    Schema from CustomerOrderListItem + CustomerOrderRow definitions.
    """

    name = "customer_orders"
    primary_keys = ["orderNumber"]
    replication_key = "changeDate"
    replication_method = "INCREMENTAL"

    schema = th.PropertiesList(
        # CustomerOrderListItem fields
        th.Property("orderNumber", th.StringType),
        th.Property("orderNumberExternal", th.StringType),
        th.Property("orderType", th.StringType),
        th.Property("orderStatus", th.StringType),
        th.Property("orderDate", th.DateTimeType),
        th.Property("askedDeliveryDate", th.DateTimeType),
        th.Property("slaDate", th.DateTimeType),
        th.Property("customerNumber", th.StringType),
        th.Property("customerName", th.StringType),
        th.Property("totalPrice", th.NumberType),
        th.Property("changeDate", th.DateTimeType),
        # Detail: CustomerOrderRow list as JSON
        # Key fields per row: position, orderRowStatus,
        #   product.productNumber, salesData.quantity, salesData.unitPrice,
        #   salesData.vatPercent, salesData.currency, warehouse
        th.Property("order_rows", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        start_replication = self.get_starting_replication_key_value(context)
        if start_replication:
            modified_date_from = str(start_replication)
        elif self.config.get("start_date"):
            modified_date_from = str(self.config["start_date"])
        else:
            two_years_ago = datetime.now(timezone.utc) - timedelta(days=730)
            modified_date_from = two_years_ago.strftime("%Y-%m-%dT%H:%M:%S")

        end_date = self.config.get("end_date")

        page_offset = 0
        page_count = 100
        total_orders = 0

        while True:
            params: dict[str, Any] = {
                "pageCount": page_count,
                "pageOffset": page_offset,
                "modifiedDateFrom": modified_date_from,
            }
            if end_date:
                params["changeDateTo"] = str(end_date)

            try:
                order_list = self._request(
                    f"{self.base_url}/CustomerOrders",
                    params=params,
                ).json()
            except requests.exceptions.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 400 and page_offset > 0:
                    logger.warning(
                        "CustomerOrders: 400 at pageOffset=%d after %d orders. "
                        "Yielding partial results. Response: %s",
                        page_offset, total_orders,
                        exc.response.text[:500],
                    )
                    return
                raise

            if not isinstance(order_list, list) or not order_list:
                break

            total_orders += len(order_list)

            for o in order_list:
                order_number = str(o.get("orderNumber") or "")
                if not order_number:
                    continue

                yield {
                    "orderNumber": order_number,
                    "orderNumberExternal": o.get("orderNumberExternal"),
                    "orderType": o.get("orderType"),
                    "orderStatus": o.get("orderStatus"),
                    "orderDate": o.get("orderDate"),
                    "askedDeliveryDate": o.get("askedDeliveryDate"),
                    "slaDate": o.get("slaDate"),
                    "customerNumber": str(o.get("customerNumber") or ""),
                    "customerName": o.get("customerName"),
                    "totalPrice": o.get("totalPrice"),
                    "changeDate": o.get("changeDate"),
                    "order_rows": json.dumps(self._fetch_order_rows(order_number)),
                }

            if len(order_list) < page_count:
                break
            page_offset += 1

            if page_offset % 50 == 0:
                logger.info(
                    "CustomerOrders: page %d — %d orders fetched so far",
                    page_offset, total_orders,
                )

        logger.info("CustomerOrders: done — %d orders", total_orders)

    def _fetch_order_rows(self, order_number: str) -> list:
        """Fetch orderRows from GET /CustomerOrders/{id}.

        Response: {orderHeader: {...}, orderRows: [...]}.
        Key fields per CustomerOrderRow:
          position, orderRowStatus, supplyMode, warehouse,
          product.productNumber, product.productName,
          salesData.quantity, salesData.unitPrice, salesData.vatPercent,
          salesData.currency, expectedDeliveryDate, changeDate.
        """
        try:
            detail = self._request(f"{self.base_url}/CustomerOrders/{order_number}").json()
            rows = detail.get("orderRows", [])
            return rows if isinstance(rows, list) else []
        except Exception:
            logger.warning("Failed to fetch rows for order %s", order_number, exc_info=True)
            return []


# ---------------------------------------------------------------------------
# PurchaseOrdersStream  —  GET /PurchaseOrders  +  GET /PurchaseOrders/{id}
# ---------------------------------------------------------------------------


class PurchaseOrdersStream(ExtendStream):
    """Extend Commerce PurchaseOrders with full detail.

    List endpoint returns PurchaseOrderListItem summaries.
    Detail endpoint is called per PO for header (incl. supplierAgreementNumber),
    rows, and shipments.

    Incremental via createDateFrom.
    Pagination: pageNumber (1-based).
    Replication key: createDate (from PurchaseOrderListItem).

    Schema from PurchaseOrderListItem + PurchaseOrderSupplier +
    PurchaseOrderRow definitions.
    """

    name = "purchase_orders"
    primary_keys = ["purchaseNumber"]
    replication_key = "createDate"
    replication_method = "INCREMENTAL"

    schema = th.PropertiesList(
        # PurchaseOrderListItem fields
        th.Property("purchaseNumber", th.StringType),
        th.Property("status", th.StringType),
        th.Property("createDate", th.DateTimeType),
        th.Property("warehouse", th.StringType),
        th.Property("isOpen", th.BooleanType),
        th.Property("isReceived", th.BooleanType),
        th.Property("externalOrderNumber", th.StringType),
        th.Property("supplierOrderNumber", th.StringType),
        th.Property("shippedDate", th.DateTimeType),
        # PurchaseOrderSupplier fields (from detail header)
        th.Property("supplierNumber", th.StringType),
        th.Property("supplierName", th.StringType),
        th.Property("supplierAgreementNumber", th.IntegerType),  # FK to SupplierAgreement
        th.Property("supplierAgreement", th.StringType),
        th.Property("paymentTerms", th.StringType),
        th.Property("deliveryMethod", th.StringType),
        th.Property("deliveryMethodName", th.StringType),
        th.Property("transportCondition", th.StringType),
        th.Property("forwarder", th.StringType),
        # Header fields
        th.Property("reference", th.StringType),
        th.Property("notes", th.StringType),
        th.Property("requestedDeliveryDate", th.DateTimeType),
        # Delivery address (flattened)
        th.Property("deliveryAddress_name1", th.StringType),
        th.Property("deliveryAddress_name2", th.StringType),
        th.Property("deliveryAddress_address1", th.StringType),
        th.Property("deliveryAddress_postalCode", th.StringType),
        th.Property("deliveryAddress_city", th.StringType),
        th.Property("deliveryAddress_countryCode", th.StringType),
        # Buyer contact (flattened)
        th.Property("buyerContactName", th.StringType),
        th.Property("buyerContactEmail", th.StringType),
        # PurchaseOrderRow list as JSON
        # Key fields per row: position, rowStatus, productNumber, productName,
        #   supplierProductNumber, purchaseDataProductUnit.quantity,
        #   purchaseDataProductUnit.unitPrice, purchaseDataProductUnit.vatPercent,
        #   purchaseDataProductUnit.currency, expectedDeliveryDate
        th.Property("rows", th.StringType),
        # PurchaseOrderShipment list as JSON (populated when isReceived=True)
        th.Property("shipments", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        warehouse_codes = self._tap.warehouse_codes
        start_replication = self.get_starting_replication_key_value(context)

        params_base: dict[str, Any] = {}
        if start_replication:
            params_base["createDateFrom"] = str(start_replication)
        elif self.config.get("start_date"):
            params_base["createDateFrom"] = str(self.config["start_date"])
        if self.config.get("end_date"):
            params_base["createDateTo"] = str(self.config["end_date"])

        page = 1
        while True:
            data = self._request(
                f"{self.base_url}/PurchaseOrders",
                params={**params_base, "pageNumber": page},
            ).json()

            po_list = data.get("purchaseOrderList", [])
            pagination = data.get("paginationInfo", {})

            if not po_list:
                break

            for po in po_list:
                purchase_number = po.get("purchaseNumber")
                if not purchase_number:
                    continue
                if warehouse_codes and po.get("warehouse") not in warehouse_codes:
                    continue

                detail = self._fetch_detail(purchase_number)
                if detail:
                    yield self._map_detail(detail, po)
                else:
                    yield self._map_summary(po)

            total_pages = pagination.get("totalPages", 0)
            if page >= total_pages:
                break
            page += 1

    def _fetch_detail(self, purchase_number: str) -> Optional[dict]:
        try:
            return self._request(f"{self.base_url}/PurchaseOrders/{purchase_number}").json()
        except Exception:
            logger.warning("Failed to fetch detail for PO %s", purchase_number, exc_info=True)
            return None

    def _map_detail(self, detail: dict, summary: dict) -> dict:
        header = detail.get("header", {}) or {}
        rows = detail.get("rows", [])
        shipments = detail.get("shipments", [])
        supplier = header.get("supplier", {}) or {}
        delivery_addr = header.get("deliveryAddress", {}) or {}
        buyer = header.get("buyerContact", {}) or {}

        return {
            "purchaseNumber": header.get("purchaseNumber") or summary.get("purchaseNumber"),
            "status": header.get("status") or summary.get("status"),
            "createDate": header.get("createDate") or summary.get("createDate"),
            "warehouse": header.get("warehouse") or summary.get("warehouse"),
            "isOpen": summary.get("isOpen", True),
            "isReceived": summary.get("isReceived", False),
            "externalOrderNumber": header.get("externalOrderNumber") or summary.get("externalOrderNumber", ""),
            "supplierOrderNumber": header.get("supplierOrderNumber") or summary.get("supplierOrderNumber", ""),
            "shippedDate": header.get("shippedDate") or summary.get("shippedDate"),
            # PurchaseOrderSupplier
            "supplierNumber": supplier.get("supplierNumber") or summary.get("supplierNumber"),
            "supplierName": supplier.get("supplierName") or summary.get("supplierName"),
            "supplierAgreementNumber": supplier.get("supplierAgreementNumber"),
            "supplierAgreement": supplier.get("supplierAgreement"),
            "paymentTerms": supplier.get("paymentTerms"),
            "deliveryMethod": supplier.get("deliveryMethod"),
            "deliveryMethodName": supplier.get("deliveryMethodName"),
            "transportCondition": supplier.get("transportCondition"),
            "forwarder": supplier.get("forwarder"),
            # Header
            "reference": header.get("reference", ""),
            "notes": header.get("notes", ""),
            "requestedDeliveryDate": header.get("requestedDeliveryDate"),
            # Delivery address
            "deliveryAddress_name1": delivery_addr.get("name1"),
            "deliveryAddress_name2": delivery_addr.get("name2"),
            "deliveryAddress_address1": delivery_addr.get("address1"),
            "deliveryAddress_postalCode": delivery_addr.get("postalCode"),
            "deliveryAddress_city": delivery_addr.get("city"),
            "deliveryAddress_countryCode": delivery_addr.get("countryCode"),
            # Buyer contact
            "buyerContactName": buyer.get("name"),
            "buyerContactEmail": buyer.get("email"),
            # Nested
            "rows": json.dumps(rows),
            "shipments": json.dumps(shipments),
        }

    def _map_summary(self, summary: dict) -> dict:
        """Fallback when detail fetch fails — summary fields only."""
        return {
            "purchaseNumber": summary.get("purchaseNumber"),
            "status": summary.get("status"),
            "createDate": summary.get("createDate"),
            "warehouse": summary.get("warehouse"),
            "isOpen": summary.get("isOpen", True),
            "isReceived": summary.get("isReceived", False),
            "externalOrderNumber": summary.get("externalOrderNumber", ""),
            "supplierOrderNumber": summary.get("supplierOrderNumber", ""),
            "shippedDate": summary.get("shippedDate"),
            "supplierNumber": summary.get("supplierNumber"),
            "supplierName": summary.get("supplierName"),
            "supplierAgreementNumber": None,
            "supplierAgreement": None,
            "paymentTerms": None,
            "deliveryMethod": None,
            "deliveryMethodName": None,
            "transportCondition": None,
            "forwarder": None,
            "reference": None,
            "notes": None,
            "requestedDeliveryDate": None,
            "deliveryAddress_name1": None,
            "deliveryAddress_name2": None,
            "deliveryAddress_address1": None,
            "deliveryAddress_postalCode": None,
            "deliveryAddress_city": None,
            "deliveryAddress_countryCode": None,
            "buyerContactName": None,
            "buyerContactEmail": None,
            "rows": "[]",
            "shipments": "[]",
        }


# ---------------------------------------------------------------------------
# Shared helper: day-by-day paginator for Reports endpoints
# ---------------------------------------------------------------------------


def _iter_report_days(stream: "ExtendStream", url: str, list_key: str, start_date: str, end_date: Optional[str] = None) -> Iterable[dict]:
    """Iterate a Reports endpoint one day at a time, paginating each day.

    Reports endpoints require changeDate == toChangeDate (one-day window).
    Pagination uses pageNumber (1-based); stop when paginationInfo.currentPage
    equals paginationInfo.totalPages.

    Args:
        stream:     ExtendStream instance (for _request and logging).
        url:        Full endpoint URL.
        list_key:   Key in the JSON response that holds the list of records.
        start_date: ISO date string "YYYY-MM-DD" to start from.
        end_date:   ISO date string "YYYY-MM-DD" upper bound (inclusive).
                    Defaults to today (UTC).
    """
    current = datetime.strptime(start_date[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if end_date:
        stop = datetime.strptime(end_date[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        stop = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    total_days = (stop - current).days + 1
    day_num = 0

    while current <= stop:
        date_str = current.strftime("%Y-%m-%d")
        day_num += 1
        page = 1
        day_records = 0

        while True:
            params = {
                "changeDate": date_str,
                "toChangeDate": date_str,
                "pageNumber": page,
            }
            try:
                data = stream._request(url, params=params).json()
            except requests.exceptions.HTTPError as exc:
                response = exc.response
                if response is None or response.status_code != 400 or page == 1:
                    raise

                probe = stream._request(url, params={
                    "changeDate": date_str,
                    "toChangeDate": date_str,
                    "pageNumber": 1,
                }).json()
                total_pages = int(probe.get("paginationInfo", {}).get("totalPages") or 1)
                if page > total_pages:
                    logger.warning(
                        "Reports endpoint rejected page %s for %s; current totalPages is %s. "
                        "Treating this as end-of-day pagination drift.",
                        page,
                        date_str,
                        total_pages,
                    )
                    break
                raise

            items = data.get(list_key, [])
            pagination = data.get("paginationInfo", {})
            day_records += len(items)

            for item in items:
                if not item.get("changeDate"):
                    item["changeDate"] = date_str + "T00:00:00+00:00"
                yield item

            current_page = int(pagination.get("currentPage") or page)
            total_pages = int(pagination.get("totalPages") or 1)
            if not total_pages or current_page >= total_pages:
                break
            page += 1

        logger.info(
            "Reports %s day %d/%d (%s): %d records across %d pages",
            list_key, day_num, total_days, date_str, day_records, page,
        )
        current += timedelta(days=1)


# ---------------------------------------------------------------------------
# ReportsOrderHeadersStream  —  GET /reports/{client}/OrderHeaders
# ---------------------------------------------------------------------------


class ReportsOrderHeadersStream(ExtendStream):
    """Extend Commerce order headers from the Reports API.

    Iterates day by day (changeDate == toChangeDate) from start_date to today,
    paginating all pages within each day before advancing.

    Pagination: pageNumber (1-based), stop when currentPage == totalPages.
    Replication key: changeDate.
    """

    name = "reports_order_headers"
    primary_keys = ["orderNumber"]
    replication_key = "changeDate"
    replication_method = "INCREMENTAL"

    schema = th.PropertiesList(
        th.Property("orderNumber", th.StringType),
        th.Property("orderNumberExternal", th.StringType),
        th.Property("orderType", th.StringType),
        th.Property("orderStatus", th.StringType),
        th.Property("orderDate", th.DateTimeType),
        th.Property("customerNumber", th.StringType),
        th.Property("customerName", th.StringType),
        th.Property("totalPrice", th.NumberType),
        th.Property("currency", th.StringType),
        th.Property("warehouse", th.StringType),
        th.Property("changeDate", th.DateTimeType),
    ).to_dict()

    @property
    def _reports_url(self) -> str:
        api_url = self.config.get("api_url", "https://s05.extend.se/RESTAPI").rstrip("/")
        return f"{api_url}/reports/{self.config['client']}/OrderHeaders"

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        start_replication = self.get_starting_replication_key_value(context)
        if start_replication:
            start_date = str(start_replication)[:10]
        elif self.config.get("start_date"):
            start_date = str(self.config["start_date"])[:10]
        else:
            start_date = (datetime.now(timezone.utc) - timedelta(days=365)).strftime("%Y-%m-%d")

        end_date = str(self.config["end_date"])[:10] if self.config.get("end_date") else None

        for item in _iter_report_days(self, self._reports_url, "orderHeaderList", start_date, end_date):
            yield {
                "orderNumber": item.get("orderNumber"),
                "orderNumberExternal": item.get("orderNumberExternal"),
                "orderType": item.get("orderType"),
                "orderStatus": item.get("orderStatus"),
                "orderDate": item.get("orderDate"),
                "customerNumber": str(item.get("customerNumber") or ""),
                "customerName": item.get("customerName"),
                "totalPrice": item.get("totalPrice"),
                "currency": item.get("currency"),
                "warehouse": item.get("warehouse"),
                "changeDate": item.get("changeDate"),
            }


# ---------------------------------------------------------------------------
# ReportsOrderRowsStream  —  GET /reports/{client}/OrderRows
# ---------------------------------------------------------------------------


class ReportsOrderRowsStream(ExtendStream):
    """Extend Commerce order rows from the Reports API.

    Iterates day by day (changeDate == toChangeDate) from start_date to today,
    paginating all pages within each day before advancing.

    Pagination: pageNumber (1-based), stop when currentPage == totalPages.
    Replication key: changeDate.
    """

    name = "reports_order_rows"
    primary_keys = ["orderNumber", "position"]
    replication_key = "changeDate"
    replication_method = "INCREMENTAL"

    schema = th.PropertiesList(
        th.Property("orderNumber", th.StringType),
        th.Property("position", th.IntegerType),
        th.Property("orderRowStatus", th.StringType),
        th.Property("productNumber", th.StringType),
        th.Property("productName", th.StringType),
        th.Property("supplierProductNumber", th.StringType),
        th.Property("quantity", th.NumberType),
        th.Property("unitPrice", th.NumberType),
        th.Property("vatPercent", th.NumberType),
        th.Property("currency", th.StringType),
        th.Property("warehouse", th.StringType),
        th.Property("expectedDeliveryDate", th.DateTimeType),
        th.Property("changeDate", th.DateTimeType),
    ).to_dict()

    @property
    def _reports_url(self) -> str:
        api_url = self.config.get("api_url", "https://s05.extend.se/RESTAPI").rstrip("/")
        return f"{api_url}/reports/{self.config['client']}/OrderRows"

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        start_replication = self.get_starting_replication_key_value(context)
        if start_replication:
            start_date = str(start_replication)[:10]
        elif self.config.get("start_date"):
            start_date = str(self.config["start_date"])[:10]
        else:
            start_date = (datetime.now(timezone.utc) - timedelta(days=365)).strftime("%Y-%m-%d")

        end_date = str(self.config["end_date"])[:10] if self.config.get("end_date") else None

        for item in _iter_report_days(self, self._reports_url, "orderRowList", start_date, end_date):
            yield {
                "orderNumber": item.get("orderNumber"),
                "position": item.get("position"),
                "orderRowStatus": item.get("orderRowStatus"),
                "productNumber": item.get("productNumber"),
                "productName": item.get("productName"),
                "supplierProductNumber": item.get("supplierProductNumber"),
                "quantity": item.get("quantity"),
                "unitPrice": item.get("unitPrice"),
                "vatPercent": item.get("vatPercent"),
                "currency": item.get("currency"),
                "warehouse": item.get("warehouse"),
                "expectedDeliveryDate": item.get("expectedDeliveryDate"),
                "changeDate": item.get("changeDate"),
            }


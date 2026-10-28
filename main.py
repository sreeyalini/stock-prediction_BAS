
import csv
import json
import logging
import math
import os
import re

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from urllib.parse import quote

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


# ---------------------------------------------------------
# 1. ENVIRONMENT CONFIGURATION
# ---------------------------------------------------------

def load_manifest_env():
    """
    In BAS, apply the env section of manifest.yml when
    running locally. Existing environment variables win.
    """
    path = Path(__file__).with_name("manifest.yml")

    if "VCAP_SERVICES" in os.environ or not path.is_file():
        return

    import yaml

    manifest = yaml.safe_load(
        path.read_text(encoding="utf-8")
    ) or {}

    applications = manifest.get("applications") or [{}]

    for key, value in (applications[0].get("env") or {}).items():
        if isinstance(value, bool):
            value = str(value).lower()
        os.environ.setdefault(key, str(value))


load_manifest_env()
load_dotenv()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("sap_stock")

app = FastAPI(title="SAP TabPFN 30-Day Stock Planning")

CLIENT = os.getenv("S4_CLIENT", "100")

VERIFY = os.getenv("VERIFY_SSL", "true").lower() == "true"

CSV_PATH = os.getenv(
    "TRAINING_CSV_PATH",
    str(
        Path(__file__).with_name(
            "stock_training_DIRECT_STOCK_NEEDED_30D_DEMO.csv"
        )
    ),
)

# Feature order must match the training CSV.
FEATURES = [
    "CurrentStock",
    "Consumption30D",
    "OpenPO30D",
    "Demand30D",
    "SafetyStock",
]

TARGET_COLUMN = "StockNeededAfter30Days"

MOVEMENTS = {
    "201": 1,
    "221": 1,
    "261": 1,
    "281": 1,
    "601": 1,
    "202": -1,
    "222": -1,
    "262": -1,
    "282": -1,
    "602": -1,
}


# ---------------------------------------------------------
# 2. REQUEST MODEL
# ---------------------------------------------------------

class StockRequest(BaseModel):
    material: str = Field(min_length=1)
    plant: str = Field(min_length=1)
    as_of_date: date | None = None


# ---------------------------------------------------------
# 3. GENERAL UTILITIES
# ---------------------------------------------------------

def qty(value):
    """Convert a quantity to a finite float."""
    if value is None or value == "":
        return 0.0

    number = float(value)

    if not math.isfinite(number):
        raise ValueError("Non-finite SAP quantity")

    return number


def sap_date(value):
    """Convert common SAP OData date representations to date."""
    if value is None or value == "":
        return None

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    text = str(value)

    match = re.fullmatch(
        r"/Date\((-?\d+)(?:[+-]\d{4})?\)/",
        text,
    )

    if match:
        return datetime.fromtimestamp(
            int(match.group(1)) / 1000,
            tz=timezone.utc,
        ).date()

    try:
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise ValueError(
            f"Unsupported SAP date: {text}"
        ) from exc


def esc(value):
    """Escape single quotes in OData string literals."""
    return str(value).replace("'", "''")


# ---------------------------------------------------------
# 4. SAP BTP SERVICE BINDINGS AND OAUTH
# ---------------------------------------------------------

def bound_service_credentials(service_name):
    """Find a bound BTP service by label, name, or tag."""
    try:
        bindings = json.loads(
            os.getenv("VCAP_SERVICES", "{}")
        )
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "VCAP_SERVICES contains invalid JSON"
        ) from exc

    for label, entries in bindings.items():
        for entry in entries:
            names = {
                str(label).lower(),
                str(entry.get("label", "")).lower(),
                str(entry.get("name", "")).lower(),
                *[
                    str(tag).lower()
                    for tag in entry.get("tags", [])
                ],
            }

            if service_name.lower() in names:
                return entry.get("credentials") or {}

    raise RuntimeError(
        f"No bound {service_name} service found in VCAP_SERVICES"
    )


def oauth_token(credentials):
    """Get an OAuth token using BTP service credentials."""
    uaa = credentials.get("uaa") or {}

    base = credentials.get("url") or uaa.get("url")
    client_id = (
        credentials.get("clientid")
        or uaa.get("clientid")
    )
    secret = (
        credentials.get("clientsecret")
        or uaa.get("clientsecret")
    )

    if not all((base, client_id, secret)):
        raise RuntimeError(
            "Service binding lacks OAuth URL/clientid/clientsecret"
        )

    endpoint = base.rstrip("/") + "/oauth/token"

    response = requests.post(
        endpoint,
        data={"grant_type": "client_credentials"},
        auth=(client_id, secret),
        verify=VERIFY,
        timeout=30,
    )

    response.raise_for_status()

    return response.json()["access_token"]


def destination_config():
    """Resolve the named destination from the BTP Destination service."""
    name = os.getenv("S4_DESTINATION_NAME", "").strip()

    if not name:
        raise RuntimeError("S4_DESTINATION_NAME is missing")

    credentials = bound_service_credentials("destination")
    uri = credentials.get("uri")

    if not uri:
        raise RuntimeError(
            "Destination service binding has no uri"
        )

    url = (
        uri.rstrip("/")
        + "/destination-configuration/v1/destinations/"
        + quote(name, safe="")
    )

    response = requests.get(
        url,
        headers={
            "Authorization": (
                "Bearer " + oauth_token(credentials)
            )
        },
        verify=VERIFY,
        timeout=30,
    )

    response.raise_for_status()

    payload = response.json()
    config = payload.get("destinationConfiguration") or {}

    if not config.get("URL"):
        raise RuntimeError(
            f"Destination {name} has no URL"
        )

    return config, payload.get("authTokens") or []


def connectivity_proxy_config():
    """Resolve the SAP Connectivity proxy."""
    credentials = bound_service_credentials("connectivity")

    host = credentials.get("onpremise_proxy_host")
    port = credentials.get("onpremise_proxy_port")

    if not host or not port:
        raise RuntimeError(
            "Connectivity binding lacks proxy host/port"
        )

    return (
        f"http://{host}:{port}",
        oauth_token(credentials),
    )


def running_on_cloud_foundry():
    return "VCAP_SERVICES" in os.environ


# ---------------------------------------------------------
# 5. SAP DESTINATION ACCESS IN BAS
# ---------------------------------------------------------

def bas_destination_get(service, entity, query, headers):
    """
    Access a BAS destination through the local destination proxy.
    """
    from requests.auth import HTTPBasicAuth

    name = os.getenv("S4_DESTINATION_NAME", "").strip()

    if not name:
        raise RuntimeError("S4_DESTINATION_NAME is missing")

    proxy_url = (
        os.getenv("HTTP_PROXY")
        or os.getenv("http_proxy")
        or "http://localhost:8887"
    )

    url = (
        f"http://{name}.dest/"
        f"sap/opu/odata/sap/{service}/{entity}"
    )

    options = {
        "proxies": {
            "http": proxy_url,
            "https": proxy_url,
        }
    }

    username = os.getenv("S4_USERNAME")
    password = os.getenv("S4_PASSWORD")

    # Optional; use only if the destination proxy requires it.
    if username and password:
        options["auth"] = HTTPBasicAuth(
            username,
            password,
        )

    with requests.Session() as session:
        session.trust_env = False

        return session.get(
            url,
            params=query,
            headers=headers,
            timeout=60,
            **options,
        )


def odata_payload(response):
    """Validate and unwrap an SAP OData V2 response."""
    response.raise_for_status()

    data = response.json()

    if "d" not in data:
        raise RuntimeError(
            "Unexpected SAP OData response: missing d property"
        )

    return data["d"]


# ---------------------------------------------------------
# 6. SAP ODATA REQUEST HANDLER
# ---------------------------------------------------------

def sap_get(service, entity, params=None):
    """Fetch SAP OData through BAS or BTP destination connectivity."""

    if not running_on_cloud_foundry():
        query = {
            "sap-client": CLIENT,
            "$format": "json",
        }
        query.update(params or {})

        response = bas_destination_get(
            service,
            entity,
            query,
            {"Accept": "application/json"},
        )

        return odata_payload(response)

    destination, auth_tokens = destination_config()

    base = destination["URL"].rstrip("/")
    proxy_type = str(
        destination.get("ProxyType", "")
    ).lower()

    auth_type = str(
        destination.get(
            "Authentication",
            "NoAuthentication",
        )
    ).lower()

    path = f"/sap/opu/odata/sap/{service}/{entity}"

    # Support destinations whose URL already includes the service root.
    marker = "/sap/opu/odata/sap/"

    if marker in base:
        prefix, suffix = base.split(marker, 1)
        service_root = suffix.strip("/")

        if service_root and service_root != service:
            raise RuntimeError(
                "Destination URL is scoped to a different SAP OData service"
            )

        base = prefix

    url = base + path

    query = {
        "sap-client": CLIENT,
        "$format": "json",
    }
    query.update(params or {})

    headers = {"Accept": "application/json"}
    options = {}

    if proxy_type == "onpremise":
        proxy_url, access_token = connectivity_proxy_config()

        options["proxies"] = {
            "http": proxy_url,
            "https": proxy_url,
        }

        headers["Proxy-Authorization"] = (
            "Bearer " + access_token
        )

        location_id = (
            os.getenv("SCC_LOCATION_ID")
            or destination.get("CloudConnectorLocationId")
            or ""
        ).strip()

        if location_id:
            headers[
                "SAP-Connectivity-SCC-Location_ID"
            ] = location_id

    elif proxy_type != "internet":
        raise RuntimeError(
            "Destination ProxyType must be OnPremise or Internet"
        )

    if auth_type == "basicauthentication":
        from requests.auth import HTTPBasicAuth

        username = (
            destination.get("User")
            or os.getenv("S4_USERNAME")
        )
        password = (
            destination.get("Password")
            or os.getenv("S4_PASSWORD")
        )

        if not username or not password:
            raise RuntimeError(
                "Destination BasicAuthentication lacks User/Password"
            )

        options["auth"] = HTTPBasicAuth(
            username,
            password,
        )

    elif auth_type == "noauthentication":
        pass

    elif auth_tokens:
        token_entry = next(
            (
                item
                for item in auth_tokens
                if item.get("http_header")
            ),
            None,
        )

        if not token_entry:
            raise RuntimeError(
                "Destination authentication tokens lack an HTTP header"
            )

        header = token_entry["http_header"]
        headers[header["key"]] = header["value"]

    else:
        raise RuntimeError(
            "Destination authentication is not supported: "
            + auth_type
        )

    with requests.Session() as session:
        session.trust_env = False

        response = session.get(
            url,
            params=query,
            headers=headers,
            verify=VERIFY,
            timeout=60,
            **options,
        )

    return odata_payload(response)


# ---------------------------------------------------------
# 7. SAP PAGINATION
# ---------------------------------------------------------

def sap_rows(
    service,
    entity,
    filter_expr=None,
    page_size=200,
    max_rows=50000,
):
    """Retrieve OData V2 results using $top and $skip."""

    output = []
    skip = 0

    while True:
        params = {
            "$top": str(page_size),
            "$skip": str(skip),
        }

        if filter_expr:
            params["$filter"] = filter_expr

        data = sap_get(service, entity, params)
        batch = data.get("results", [])

        output.extend(batch)

        if len(output) > max_rows:
            raise RuntimeError(
                f"SAP result exceeded {max_rows} rows: {entity}"
            )

        if not batch:
            break

        if not data.get("__next") and len(batch) < page_size:
            break

        skip += len(batch)

    return output


def nav_lines(service, item, navigation="to_ScheduleLine"):
    """Read the schedule-line navigation for a document item."""

    data = item.get(navigation) or {}

    if "results" in data:
        return data["results"]

    uri = item.get("__metadata", {}).get("uri", "")

    if not uri:
        raise RuntimeError(
            "Missing SAP item navigation URI"
        )

    marker = f"/sap/opu/odata/sap/{service}/"
    entity = uri.split(marker, 1)[-1]

    return sap_rows(
        service,
        f"{entity}/{navigation}",
    )


# ---------------------------------------------------------
# 8. DATE WINDOW HELPERS
# ---------------------------------------------------------

def window(records, as_of, kind):
    """
    records: list of (date, quantity).

    Consumption: trailing 30 days, inclusive.
    PO and sales: current date through the next 29 days.

    If the primary window has no records, use the latest
    available historical 30-day window.
    """

    valid = [
        (record_date, quantity)
        for record_date, quantity in records
        if record_date is not None and quantity > 0
    ]

    if kind == "consumption":
        start = as_of - timedelta(days=29)
        end = as_of
    else:
        start = as_of
        end = as_of + timedelta(days=29)

    current = [
        (record_date, quantity)
        for record_date, quantity in valid
        if start <= record_date <= end
    ]

    if current:
        return {
            "quantity": round(
                sum(quantity for _, quantity in current),
                3,
            ),
            "source": "CURRENT",
            "start": str(start),
            "end": str(end),
            "reference_date": str(as_of),
        }

    # Never use future records as historical consumption.
    historical = [
        (record_date, quantity)
        for record_date, quantity in valid
        if record_date < as_of
    ]

    if historical:
        latest = max(
            record_date
            for record_date, _ in historical
        )
        historical_start = latest - timedelta(days=29)

        return {
            "quantity": round(
                sum(
                    quantity
                    for record_date, quantity in historical
                    if historical_start <= record_date <= latest
                ),
                3,
            ),
            "source": "HISTORICAL_FALLBACK",
            "start": str(historical_start),
            "end": str(latest),
            "reference_date": str(latest),
        }

    return {
        "quantity": 0.0,
        "source": "NO_DATED_RECORDS_IN_WINDOW",
        "start": str(start),
        "end": str(end),
        "reference_date": None,
    }


# ---------------------------------------------------------
# 9. SAP SAFETY STOCK
# ---------------------------------------------------------

def safety_stock_from_sap(material, plant):
    """Read plant-level configured MRP safety stock from SAP."""

    key = (
        f"A_ProductSupplyPlanning("
        f"Product='{esc(material)}',"
        f"Plant='{esc(plant)}')"
    )

    row = sap_get("API_PRODUCT_SRV", key)

    if (
        "SafetyStockQuantity" not in row
        or row["SafetyStockQuantity"] is None
    ):
        raise RuntimeError(
            "SAP product planning has no SafetyStockQuantity "
            "for this product/plant"
        )

    configured = qty(row["SafetyStockQuantity"])
    minimum = qty(
        row.get("MinimumSafetyStockQuantity")
    )

    if configured < 0 or minimum < 0:
        raise RuntimeError(
            "Negative safety stock in SAP product planning"
        )

    return {
        "quantity": max(configured, minimum),
        "configured": configured,
        "minimum": minimum,
        "source": "SAP_PRODUCT_SUPPLY_PLANNING",
        "product": row.get("Product", material),
        "plant": row.get("Plant", plant),
    }


# ---------------------------------------------------------
# 10. CURRENT SAP STOCK
# ---------------------------------------------------------

def current_stock(material, plant):
    filter_expr = (
        f"Material eq '{esc(material)}' "
        f"and Plant eq '{esc(plant)}'"
    )

    rows = sap_rows(
        "API_MATERIAL_STOCK_SRV",
        "A_MatlStkInAcctMod",
        filter_expr,
    )

    total = 0.0
    seen = set()

    for row in rows:
        if (
            str(row.get("InventoryStockType") or "")
            not in ("01", "")
            or row.get("InventorySpecialStockType")
        ):
            continue

        key = tuple(
            row.get(column)
            for column in (
                "Material",
                "Plant",
                "StorageLocation",
                "Batch",
                "InventoryStockType",
                "InventorySpecialStockType",
                "Supplier",
                "Customer",
            )
        )

        if key in seen:
            continue

        seen.add(key)

        total += qty(
            row.get("MatlWrhsStkQtyInMatlBaseUnit")
        )

    return max(0.0, total)


# ---------------------------------------------------------
# 11. MATERIAL DOCUMENT POSTING DATES
# ---------------------------------------------------------

@lru_cache(maxsize=20000)
def posting_date(year, document):
    key = (
        f"A_MaterialDocumentHeader("
        f"MaterialDocumentYear='{esc(year)}',"
        f"MaterialDocument='{esc(document)}')"
    )

    row = sap_get(
        "API_MATERIAL_DOCUMENT_SRV",
        key,
    )

    return sap_date(row.get("PostingDate"))


# ---------------------------------------------------------
# 12. HISTORICAL CONSUMPTION
# ---------------------------------------------------------

def consumption(material, plant, as_of):
    filter_expr = (
        f"Material eq '{esc(material)}' "
        f"and Plant eq '{esc(plant)}'"
    )

    items = sap_rows(
        "API_MATERIAL_DOCUMENT_SRV",
        "A_MaterialDocumentItem",
        filter_expr,
    )

    movements = defaultdict(float)

    for item in items:
        movement = str(
            item.get("GoodsMovementType") or ""
        )

        if movement not in MOVEMENTS:
            continue

        if item.get("GoodsMovementIsCancelled") is True:
            continue

        year = item.get("MaterialDocumentYear")
        document = item.get("MaterialDocument")

        if not year or not document:
            continue

        document_date = posting_date(
            str(year),
            str(document),
        )

        if document_date is None or document_date > as_of:
            continue

        amount = (
            qty(item.get("QuantityInBaseUnit"))
            * MOVEMENTS[movement]
        )

        movements[document_date] += amount

    # Preserve signed reversals until the date window is summed.
    signed_records = list(movements.items())

    start = as_of - timedelta(days=29)
    end = as_of

    current = [
        (record_date, quantity)
        for record_date, quantity in signed_records
        if start <= record_date <= end
    ]

    if current:
        return {
            "quantity": round(
                max(0.0, sum(q for _, q in current)),
                3,
            ),
            "source": "CURRENT",
            "start": str(start),
            "end": str(end),
            "reference_date": str(as_of),
        }

    older = [
        (record_date, quantity)
        for record_date, quantity in signed_records
        if record_date < as_of
    ]

    if older:
        latest = max(
            record_date
            for record_date, _ in older
        )

        begin = latest - timedelta(days=29)

        return {
            "quantity": round(
                max(
                    0.0,
                    sum(
                        quantity
                        for record_date, quantity in older
                        if begin <= record_date <= latest
                    ),
                ),
                3,
            ),
            "source": "HISTORICAL_FALLBACK",
            "start": str(begin),
            "end": str(latest),
            "reference_date": str(latest),
        }

    return {
        "quantity": 0.0,
        "source": "NO_HISTORY",
        "start": str(start),
        "end": str(end),
        "reference_date": None,
    }


# ---------------------------------------------------------
# 13. PURCHASE ORDER SCHEDULES
# ---------------------------------------------------------

def purchase_orders(material, plant, as_of):
    filter_expr = (
        f"Material eq '{esc(material)}' "
        f"and Plant eq '{esc(plant)}'"
    )

    items = sap_rows(
        "API_PURCHASEORDER_PROCESS_SRV",
        "A_PurchaseOrderItem",
        filter_expr,
    )

    records = []
    excluded_account_assigned = 0

    for item in items:
        if (
            item.get("PurchasingDocumentDeletionCode")
            or item.get("IsCompletelyDelivered")
            or item.get("GoodsReceiptIsExpected") is False
            or item.get("IsReturnsItem")
        ):
            continue

        if item.get("AccountAssignmentCategory"):
            excluded_account_assigned += 1

        for line in nav_lines(
            "API_PURCHASEORDER_PROCESS_SRV",
            item,
        ):
            delivery_date = sap_date(
                line.get("ScheduleLineDeliveryDate")
            )

            quantity = qty(
                line.get("ScheduleLineOrderQuantity")
            )

            if delivery_date and quantity > 0:
                records.append(
                    (delivery_date, quantity)
                )

    result = window(
        records,
        as_of,
        "po",
    )

    # Scheduled quantity is not necessarily GR-adjusted open quantity.
    result["quantity_basis"] = (
        "SCHEDULED_NOT_GR_ADJUSTED"
    )
    result[
        "account_assigned_items_not_inventory_receipts"
    ] = excluded_account_assigned
    result["usable_as_confirmed_incoming"] = False

    return result


# ---------------------------------------------------------
# 14. SALES ORDER DEMAND
# ---------------------------------------------------------

def sales_demand(material, plant, as_of):
    filter_expr = f"Material eq '{esc(material)}'"

    items = sap_rows(
        "API_SALES_ORDER_SRV",
        "A_SalesOrderItem",
        filter_expr,
    )

    records = []

    for item in items:
        if (
            str(item.get("ProductionPlant") or "")
            != str(plant)
        ):
            continue

        if (
            item.get("SalesDocumentRjcnReason")
            or item.get("DeliveryStatus") == "C"
            or item.get("SDProcessStatus") == "C"
        ):
            continue

        lines = nav_lines(
            "API_SALES_ORDER_SRV",
            item,
        )

        requested = [
            (
                sap_date(line.get("RequestedDeliveryDate")),
                qty(line.get("ScheduleLineOrderQuantity")),
            )
            for line in lines
            if (
                line.get("RequestedDeliveryDate")
                and qty(
                    line.get("ScheduleLineOrderQuantity")
                ) > 0
            )
        ]

        confirmed = [
            (
                sap_date(line.get("ConfirmedDeliveryDate")),
                qty(
                    line.get("OpenConfdDelivQtyInOrdQtyUnit")
                ),
                qty(
                    line.get("DeliveredQtyInOrderQtyUnit")
                ),
            )
            for line in lines
            if line.get("ConfirmedDeliveryDate")
        ]

        # Confirmed and requested schedule lines describe the same order.
        confirmed_open = sum(
            max(0.0, quantity)
            for _, quantity, _ in confirmed
        )

        delivered = sum(
            max(0.0, delivered_quantity)
            for _, _, delivered_quantity in confirmed
        )

        ordered = sum(
            quantity
            for _, quantity in requested
        )

        for delivery_date, quantity, _ in confirmed:
            if delivery_date and quantity > 0:
                records.append(
                    (delivery_date, quantity)
                )

        # Allocate remaining unconfirmed quantity across requested dates.
        unconfirmed = max(
            0.0,
            ordered - delivered - confirmed_open,
        )

        if unconfirmed and requested:
            base = sum(
                quantity
                for _, quantity in requested
            )

            if base > 0:
                for delivery_date, quantity in requested:
                    if delivery_date:
                        records.append(
                            (
                                delivery_date,
                                unconfirmed * quantity / base,
                            )
                        )

    result = window(
        records,
        as_of,
        "sales",
    )

    result["quantity_basis"] = (
        "OPEN_CONFIRMED_PLUS_UNCONFIRMED_NOT_DOUBLE_COUNTED"
    )

    return result


# ---------------------------------------------------------
# 15. TRAINING DATA FOR DIRECT 30-DAY PREDICTION
# ---------------------------------------------------------

def training_rows():
    """
    Load features and the direct StockNeededAfter30Days target.

    The CSV must contain:
      CurrentStock
      Consumption30D
      OpenPO30D
      Demand30D
      SafetyStock
      StockNeededAfter30Days
    """

    if not os.path.isfile(CSV_PATH):
        raise RuntimeError(
            f"Training CSV missing: {CSV_PATH}"
        )

    with open(
        CSV_PATH,
        newline="",
        encoding="utf-8",
    ) as file:
        data = list(csv.DictReader(file))

    required = FEATURES + [TARGET_COLUMN]

    if not data or any(
        column not in data[0]
        for column in required
    ):
        raise RuntimeError(
            f"CSV must contain these columns: {required}"
        )

    x_train = []
    y_train = []

    for row in data:
        try:
            feature_values = [
                qty(row[column])
                for column in FEATURES
            ]

            target_value = qty(
                row[TARGET_COLUMN]
            )

        except (ValueError, TypeError):
            continue

        if (
            any(value < 0 for value in feature_values)
            or target_value < 0
        ):
            continue

        x_train.append(feature_values)
        y_train.append(target_value)

    if len(x_train) < 20:
        raise RuntimeError(
            "Need at least 20 valid labelled training rows; "
            f"found {len(x_train)}"
        )

    log.info(
        "Training rows loaded: %s",
        len(x_train),
    )
    log.info(
        "Training file: %s",
        CSV_PATH,
    )
    log.info(
        "Training target: %s",
        TARGET_COLUMN,
    )

    return x_train, y_train


# ---------------------------------------------------------
# 16. SAP AI CORE ACCESS TOKEN
# ---------------------------------------------------------

def token():
    """Get an access token for SAP AI Core inference."""

    auth_url = os.getenv(
        "AICORE_AUTH_URL",
        "",
    ).strip()

    client_id = os.getenv(
        "AICORE_CLIENT_ID",
        "",
    ).strip()

    client_secret = os.getenv(
        "AICORE_CLIENT_SECRET",
        "",
    )

    if not all(
        (auth_url, client_id, client_secret)
    ):
        raise RuntimeError(
            "AICORE_AUTH_URL, AICORE_CLIENT_ID, "
            "and AICORE_CLIENT_SECRET must be configured"
        )

    response = requests.post(
        auth_url,
        data={
            "grant_type": "client_credentials",
        },
        auth=(
            client_id,
            client_secret,
        ),
        verify=VERIFY,
        timeout=60,
    )

    response.raise_for_status()

    access_token = response.json().get(
        "access_token"
    )

    if not access_token:
        raise RuntimeError(
            "OAuth response did not contain access_token"
        )

    return access_token


# ---------------------------------------------------------
# 17. TABPFN INFERENCE: DIRECT STOCK REQUIREMENT
# ---------------------------------------------------------

def predict_tabpfn(features):
    """
    Predict StockNeededAfter30Days directly.

    This does not predict DemandNext7Days and does not
    scale a seven-day prediction into a 30-day value.
    """

    x_train, y_train = training_rows()

    endpoint = os.getenv(
        "TABPFN_INFERENCE_URL",
        "",
    ).strip()

    if not endpoint:
        api_url = os.getenv(
            "AICORE_API_URL",
            "",
        ).rstrip("/")

        deployment_id = os.getenv(
            "TABPFN_DEPLOYMENT_ID",
            "",
        ).strip()

        predict_path = os.getenv(
            "TABPFN_PREDICT_PATH",
            "predict",
        ).strip().lstrip("/")

        if not api_url or not deployment_id:
            raise RuntimeError(
                "Set TABPFN_INFERENCE_URL or configure "
                "AICORE_API_URL and TABPFN_DEPLOYMENT_ID"
            )

        endpoint = (
            f"{api_url}/inference/deployments/"
            f"{deployment_id}/{predict_path}"
        )

    if len(features) != len(FEATURES):
        raise ValueError(
            f"Expected {len(FEATURES)} features, "
            f"received {len(features)}"
        )

    # Confirm these field names against the deployed
    # inference service's request schema.
    payload = {
        "task_config": {
            "task": "regression",
            "tabpfn_config": {
                "n_estimators": 8,
                "random_state": 0,
            },
            "predict_params": {
                "output_type": "median",
            },
        },
        "x_train": x_train,
        "y_train": y_train,
        "x_test": [features],
    }

    headers = {
        "Authorization": f"Bearer {token()}",
        "AI-Resource-Group": os.getenv(
            "AICORE_RESOURCE_GROUP",
            "default",
        ),
        "Content-Type": "application/json",
    }

    response = requests.post(
        endpoint,
        json=payload,
        headers=headers,
        verify=VERIFY,
        timeout=180,
    )

    if not response.ok:
        raise RuntimeError(
            f"TabPFN HTTP {response.status_code}: "
            f"{response.text[:1000]}"
        )

    result = response.json()

    # Support common response wrapper names.
    if isinstance(result, dict):
        for key in (
            "predictions",
            "prediction",
            "y_pred",
        ):
            if key in result:
                result = result[key]
                break

    if isinstance(result, dict):
        for key in (
            "result",
            "values",
            "median",
            "value",
        ):
            if key in result:
                result = result[key]
                break

    while isinstance(result, list):
        if not result:
            raise RuntimeError(
                "TabPFN returned an empty prediction"
            )

        result = result[0]

    if isinstance(result, dict):
        for key in (
            "prediction",
            "value",
            "median",
        ):
            if key in result:
                result = result[key]
                break

    try:
        predicted = float(result)
    except (ValueError, TypeError) as exc:
        raise RuntimeError(
            "Unexpected TabPFN response format: "
            f"{response.text[:1000]}"
        ) from exc

    if not math.isfinite(predicted):
        raise RuntimeError(
            "TabPFN returned a non-finite prediction"
        )

    return max(0.0, predicted)


# ---------------------------------------------------------
# 18. HEALTH ENDPOINT
# ---------------------------------------------------------

@app.get("/health")
def health():
    return {
        "status": "ok",
        "prediction_target": TARGET_COLUMN,
        "prediction_horizon_days": 30,
    }


# ---------------------------------------------------------
# 19. DIRECT 30-DAY STOCK REQUIREMENT ENDPOINT
# ---------------------------------------------------------

@app.post("/predict-stock")
def predict_stock(request: StockRequest):
    try:
        as_of = request.as_of_date or date.today()

        material = request.material.strip()
        plant = request.plant.strip()

        if not material or not plant:
            raise ValueError(
                "Material and plant must not be blank"
            )

        # Step 1: Current SAP inventory.
        stock = current_stock(
            material,
            plant,
        )

        # Step 2: SAP-maintained safety stock.
        safety = safety_stock_from_sap(
            material,
            plant,
        )

        safety_quantity = safety["quantity"]

        # Step 3: Trailing 30-day consumption.
        consumption_data = consumption(
            material,
            plant,
            as_of,
        )

        # Step 4: Scheduled purchase orders over 30 days.
        po_data = purchase_orders(
            material,
            plant,
            as_of,
        )

        # Step 5: Open sales demand over 30 days.
        sales_data = sales_demand(
            material,
            plant,
            as_of,
        )

        # Step 6: Feature order must match the CSV.
        features = [
            stock,
            consumption_data["quantity"],
            po_data["quantity"],
            sales_data["quantity"],
            safety_quantity,
        ]

        # Step 7: Direct model prediction of the stock target.
        predicted_stock_needed = predict_tabpfn(
            features
        )

        # The training target StockNeededAfter30Days is
        # assumed to already include safety stock.
        # Do not add safety stock a second time.
        stock_needed_after_30_days = math.ceil(
            predicted_stock_needed
        )

        # PO schedule lines are not verified incoming receipts.
        # Do not subtract scheduled PO quantities from replenishment.
        verified_incoming_30d = 0.0

        additional_stock_required = max(
            0,
            math.ceil(
                stock_needed_after_30_days
                - stock
                - verified_incoming_30d
            ),
        )

        projected_stock_without_replenishment = (
            stock - predicted_stock_needed
        )

        return {
            "Material": material,
            "Plant": plant,
            "AsOfDate": str(as_of),
            "PredictionHorizonDays": 30,

            "CurrentStock": round(
                stock,
                2,
            ),

            "Consumption30D": consumption_data[
                "quantity"
            ],

            "OpenPO30D": po_data[
                "quantity"
            ],

            "Demand30D": sales_data[
                "quantity"
            ],

            "Metrics": {
                "Consumption30D": consumption_data,
                "OpenPO30D": po_data,
                "Demand30D": sales_data,
            },

            "PredictedStockNeededAfter30Days": round(
                predicted_stock_needed,
                2,
            ),

            "StockNeededAfter30Days": (
                stock_needed_after_30_days
            ),

            "SafetyStock": safety_quantity,
            "SafetyStockDetails": safety,

            "VerifiedIncoming30D": (
                verified_incoming_30d
            ),

            "ProjectedStockAfter30DaysWithoutReplenishment": (
                round(
                    projected_stock_without_replenishment,
                    2,
                )
            ),

            "AdditionalStockRequired": (
                additional_stock_required
            ),

            "Unit": "PC",

            "TrainingDataSource": os.getenv(
                "TRAINING_DATA_SOURCE",
                "SYNTHETIC_TEST",
            ),

            "TrainingTarget": TARGET_COLUMN,

            "Warning": (
                "Training labels are synthetic demonstration "
                "proxies, not verified historical 30-day stock "
                "requirements. SAP purchase-order schedule "
                "quantities are not verified receipts. The "
                "prediction is not a validated inventory forecast."
            ),
        }

    except HTTPException:
        raise

    except (
        requests.RequestException,
        ValueError,
        KeyError,
        RuntimeError,
    ) as exc:
        log.exception(
            "30-day stock prediction failed"
        )

        raise HTTPException(
            status_code=502,
            detail=str(exc),
        ) from exc

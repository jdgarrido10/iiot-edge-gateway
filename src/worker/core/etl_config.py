"""
ETL configuration: regex patterns, default values, column aliases, and output schemas.
"""

import re


# ==============================================================================
# 1. Weight extraction
# ==============================================================================

WEIGHT_PATTERN = re.compile(r"([\d\s]+[.,]?\d*)\s*(k?g)", re.IGNORECASE)


# ==============================================================================
# 2. Default worker counts per production line
# ==============================================================================

DEFAULT_WORKERS_PER_LINE: dict[str, int] = {
    "Linea_0202": 3,
    "Linea_0203": 4,
    "Linea_0204": 2,
    "Linea_0205": 3,
    "default": 1,
}


# ==============================================================================
# 3. Column alias mapping  (canonical_name -> [accepted_source_names])
# ==============================================================================

COLUMN_ALIASES: dict[str, list[str]] = {
    "net_weight": [
        "ActualNetWeightValue",
        "Peso",
        "Weight",
        "PesoNeto",
        "net_weight",
        "val_weight",
    ],
    "printed_weight": ["PrintedNetWeightValue", "PesoImpreso", "PrintWeight"],
    "article_name": [
        "ArticleName",
        "Producto",
        "Product_ID",
        "Article",
        "article_name",
    ],
    "article_number": [
        "ArticleNumber",
        "CodigoArticulo",
        "ItemCode",
        "ArtNum",
        "article_number",
    ],
    "batch_code": ["BatchNumber", "Lote", "Batch", "Lot_ID", "batch_code", "batch"],
    "timestamp": ["Timestamp", "Fecha", "Date", "Time", "EventTime", "timestamp"],
    "serverTime": ["CreationDate"],
    "reject_flag": [
        "totalEnumerator",
        "TotalNumerator",
        "TotalNumerator1",
        "Reject",
        "Rechazo",
        "Error",
        "Status_Flag",
    ],
    "operator_id": ["User", "Usuario", "Operator", "user_id"],
    "record_uuid": ["Id", "UUID", "RecordID", "uid"],
    "linea": ["DeviceName", "Line", "Source", "device_name"],
    "identity": ["Identity"],
    "active_workers": ["active_workers", "workers", "num_trabajadores", "operarios"],
}


# ==============================================================================
# 4. InfluxDB output column selection (ordered)
# ==============================================================================

INFLUX_COLUMNS: list[str] = [
    # Temporal identifiers
    "timestamp",
    # Production context
    "mqtt_topic",
    "linea",
    "batch_code",
    # Product identification
    "article_number",
    "article_name",
    "number_refactor",
    # Batch tracking
    "batch_instance_id",
    "batch_start_time",
    "batch_sequence",
    # Weight metrics
    "net_weight",
    "clean_net_weight_g",
    "target_weight_g",
    "giveaway_g",
    "tare_g",
    # Quality control
    "quality_status",
    "quality_mismatch",
    "reject_flag",
    # Operational context
    "active_workers",
    "shift",
    # Technical metadata
    "record_uuid",
    "identity",
]

"""
Test suite for ProductionMonitor: article-change detection and return alerts.
"""

import pytest
import pandas as pd
from datetime import datetime, timezone
from unittest.mock import MagicMock

from alerts.production_monitor import ProductionMonitor

# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture
def monitor(monkeypatch: pytest.MonkeyPatch) -> ProductionMonitor:
    """Instantiate an isolated ProductionMonitor with mocked external dependencies."""
    mock_mapper = MagicMock()
    # Simulate mapper injecting 'number_refactor'
    mock_mapper.enrich_dataframe.side_effect = lambda df, target_col="number_refactor": (
        df.assign(**{target_col: df["article_name"]})
    )

    mock_minio = MagicMock()

    # Disable watchdog thread to avoid background interference during unit tests
    monkeypatch.setattr(
        ProductionMonitor, "_start_watchdog", lambda self: None, raising=False
    )

    mon = ProductionMonitor(minio_adapter=mock_minio, mapper=mock_mapper)
    mon.state_memory = {}
    return mon


def build_chunk(
    minutes_offset: int, device: str, article: str, identity: int
) -> pd.DataFrame:
    """Helper to generate sequential micro-batches."""
    ts = datetime(2024, 1, 1, 10, minutes_offset, tzinfo=timezone.utc)
    return pd.DataFrame(
        {
            "timestamp": [ts],
            "device_name": [device],
            "article_name": [article],
            "identity": [identity],
            "CreationDateUtc": [ts.isoformat()],  # Prevents false lag detection
        }
    )


# =====================================================================
# Business Logic Tests
# =====================================================================


def test_monitor_continuous_production_no_alerts(monitor: ProductionMonitor):
    """Scenario 1: Standard continuous production should not trigger alerts."""
    topic = "factory/zone_1/production"

    df1 = build_chunk(0, "Line_A", "Brand_X", 100)
    _, alerts1 = monitor.process_chunk(df1, topic)

    df2 = build_chunk(1, "Line_A", "Brand_X", 101)
    _, alerts2 = monitor.process_chunk(df2, topic)

    assert len(alerts1) == 0, "False positive alert on production start."
    assert len(alerts2) == 0, "False positive alert on continuous matching batch."


def test_monitor_article_change_detection(monitor: ProductionMonitor):
    """Scenario 2: Article change triggers a new batch instance."""
    topic = "factory/zone_1/production"

    df_brand_x = build_chunk(0, "Line_A", "Brand_X", 100)
    monitor.process_chunk(df_brand_x, topic)

    df_brand_y = build_chunk(5, "Line_A", "Brand_Y", 101)
    out_df, alerts = monitor.process_chunk(df_brand_y, topic)

    state_key = f"{topic}/Line_A"
    current_state = monitor.state_memory[state_key]["current"]

    assert current_state["number_refactor"] == "Brand_Y", (
        "Monitor failed to update the active article state."
    )


def test_monitor_global_return_alert(monitor: ProductionMonitor):
    """Scenario 3: Global Return Detection (Human Error Prevention).

    Ensures that if an article was recently produced and closed, attempting
    to produce it again (even on a different line) triggers a critical alert.
    """
    topic = "factory/zone_1/production"

    # 1. Line_A produces Brand_X
    df1 = build_chunk(0, "Line_A", "Brand_X", 1)
    monitor.process_chunk(df1, topic)

    # 2. Line_A switches to Brand_Y (closing Brand_X batch)
    df2 = build_chunk(10, "Line_A", "Brand_Y", 2)
    monitor.process_chunk(df2, topic)

    # 3. Line_B mistakenly starts producing Brand_X
    df3 = build_chunk(20, "Line_B", "Brand_X", 3)
    _, alerts = monitor.process_chunk(df3, topic)

    return_alerts = [a for a in alerts if "RETURN" in a.get("msg", "").upper()]

    assert len(return_alerts) > 0, (
        "Critical Failure: Monitor missed a cross-line global return."
    )


def test_monitor_concurrent_segregation_ok(monitor: ProductionMonitor):
    """Scenario 4: Multi-line segregation.

    Concurrent production of different articles on different lines is valid
    and should not cross-contaminate state or trigger return alerts.
    """
    topic = "factory/zone_1/production"

    df1 = build_chunk(0, "Line_A", "Brand_X", 10)
    df2 = build_chunk(1, "Line_B", "Brand_Y", 11)

    _, alerts1 = monitor.process_chunk(df1, topic)
    _, alerts2 = monitor.process_chunk(df2, topic)

    assert len(alerts1) == 0
    return_alerts = [a for a in alerts2 if "RETURN" in a.get("msg", "").upper()]
    assert len(return_alerts) == 0, (
        "False positive: Concurrent valid production flagged as a return."
    )

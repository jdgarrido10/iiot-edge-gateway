"""
Unified production monitor: article-change detection, batch tracking,
inactivity watchdog, and global return detection.
"""

import json
import threading
import time
from datetime import datetime, timezone, time as dt_time
from pathlib import Path
from threading import Lock

import pandas as pd

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from .article_mapper import ArticleMapper
from core.logger import setup_logger

logger = setup_logger(__name__)

_MADRID_TZ = ZoneInfo("Europe/Madrid")


class ProductionMonitor:
    """Monitor production lines for article changes, batch boundaries, and stops.

    Responsibilities:
    1. Detect article changes per line and generate unique ``batch_instance_id`` values.
    2. Handle batch-boundary edge cases: >1-hour gaps, day changes, post-WiFi
       reconnection bursts.
    3. Run a background watchdog thread that fires stop alerts when a line
       has been silent longer than ``STOP_THRESHOLD_SECONDS`` during working hours.
    4. Detect global product returns (same article seen earlier on another line).
    5. Persist state to disk so recovery survives process restarts.

    Thread-safety model:
    - A ``_global_lock`` protects ``state_memory`` structure mutations.
    - Each MQTT topic gets its own ``_locks[topic]`` for processing isolation.
    - The watchdog marks ``stop_alert_sent = True`` inside ``_global_lock``
      *before* releasing it, then fires I/O (email, disk) outside the lock
      to prevent alert duplication and avoid blocking ``process_chunk``.

    Args:
        minio_adapter: Adapter used to persist daily reports.
        mapper: :class:`ArticleMapper` for code-to-name resolution.
        alert_notifier: Optional :class:`AlertNotifier` for email dispatching.
    """

    # Watchdog and shift configuration
    STOP_THRESHOLD_SECONDS = 600  # 10 minutes of silence triggers a stop alert
    LAG_THRESHOLD_SECONDS = (
        900  # 15-minute server lag → post-WiFi burst, suppress alert
    )
    SHIFT_START = dt_time(6, 0)  # Watchdog active from 06:00 Madrid
    SHIFT_END = dt_time(19, 30)  # Watchdog inactive after 19:30 Madrid
    AFTERNOON_START = dt_time(15, 0)  # Barrier: prevents post-lunch false positives
    LUNCH_START = dt_time(13, 30)
    LUNCH_END = dt_time(15, 0)

    # Batch detection
    MAX_GAP_SECONDS = 3600  # Gaps > 1 hour force a new batch sequence
    WIFI_GAP_THRESHOLD = 8 * 3600  # Post-WiFi burst: gaps > 8h force a new batch

    # State history limits
    MAX_PREVIOUS_BUFFER = 50
    MAX_DAILY_HISTORY = 500

    LOCAL_STATE_DIR = Path("/app/data/monitor_state")

    def __init__(self, minio_adapter, mapper: ArticleMapper, alert_notifier=None):
        self.minio = minio_adapter
        self.mapper = mapper
        self.alert_notifier = alert_notifier

        self.state_memory: dict = {}
        self._locks: dict = {}
        self._global_lock = Lock()

        self._restore_state_from_disk()

        self.running = True
        self.watchdog_thread = threading.Thread(
            target=self._watchdog_loop,
            daemon=True,
            name="StopWatchdog",
        )
        self.watchdog_thread.start()
        logger.info(
            f"Watchdog started "
            f"(threshold={self.STOP_THRESHOLD_SECONDS}s, "
            f"active {self.SHIFT_START}–{self.SHIFT_END} Madrid)"
        )

    def stop(self) -> None:
        """Signal the watchdog thread to stop and wait for it to exit."""
        self.running = False
        if self.watchdog_thread.is_alive():
            self.watchdog_thread.join(timeout=2)

    # ------------------------------------------------------------------
    # Watchdog
    # ------------------------------------------------------------------

    def _watchdog_loop(self) -> None:
        """Background loop: check for line inactivity every 60 seconds."""
        while self.running:
            try:
                time.sleep(60)
                self._check_inactivity()
            except Exception as e:
                logger.error(f"Watchdog loop error: {e}")

    def _check_inactivity(self) -> None:
        """Evaluate all tracked lines for stop conditions.

        Stop alert decision and flag mutation happen inside ``_global_lock``
        to prevent duplicates. Email and disk I/O happen outside the lock to
        avoid blocking ``process_chunk`` callers.
        """
        now_utc = datetime.now(timezone.utc)

        try:
            now_madrid = now_utc.astimezone(_MADRID_TZ)
            now_time = now_madrid.time()
            today_date = now_madrid.date()
        except Exception:
            now_time = dt_time(0, 0)
            today_date = datetime.now(timezone.utc).date()

        # Shift filter — watchdog only active during working hours
        if not (self.SHIFT_START <= now_time <= self.SHIFT_END):
            return

        # Lunch filter — expected pause, suppress alerts
        if self.LUNCH_START <= now_time <= self.LUNCH_END:
            return

        pending_alerts: list[tuple] = []

        with self._global_lock:
            keys = list(self.state_memory.keys())

        for key in keys:
            if "_COUNTERS" in key or "/" not in key:
                continue

            with self._global_lock:
                state = self.state_memory.get(key)
                if not state or state.get("type") != "LOCAL_STATE":
                    continue
                if state.get("stop_alert_sent", False):
                    continue

                current_batch = state.get("current")
                if not current_batch:
                    continue

                last_seen = current_batch.get("end_time")
                if not last_seen:
                    continue

                if last_seen.tzinfo is None:
                    last_seen = last_seen.replace(tzinfo=timezone.utc)

                last_seen_madrid = last_seen.astimezone(_MADRID_TZ)
                last_seen_date = last_seen_madrid.date()
                last_seen_time = last_seen_madrid.time()

                # Barrier 1: day change — avoids 14-hour false positives overnight
                if today_date > last_seen_date:
                    continue

                # Barrier 2: afternoon shift — avoids post-lunch false positives
                if (
                    now_time >= self.AFTERNOON_START
                    and last_seen_time < self.AFTERNOON_START
                ):
                    continue

                # Barrier 3: post-WiFi reconnection burst — high server lag means
                # data was buffered during a connectivity outage, not a real stop
                last_lag = state.get("last_connection_lag_seconds", 0.0)
                if last_lag > self.LAG_THRESHOLD_SECONDS:
                    logger.info(
                        f"Stop suppressed for '{key}': server lag {last_lag / 60:.1f}min "
                        "suggests post-WiFi reconnection burst"
                    )
                    continue

                silence_duration = (now_utc - last_seen).total_seconds()
                if silence_duration > self.STOP_THRESHOLD_SECONDS:
                    # Mark the flag inside the lock so the next watchdog cycle
                    # sees it immediately, even if the email/disk I/O below fails
                    state["stop_alert_sent"] = True
                    pending_alerts.append((key, current_batch.copy(), silence_duration))

        # Fire I/O outside the lock
        for key, batch_snapshot, duration in pending_alerts:
            try:
                self._trigger_stop_alert(key, batch_snapshot, duration)
            except Exception as e:
                logger.error(
                    f"Stop alert dispatch failed for '{key}': {e} — will not retry"
                )
                continue

            try:
                with self._global_lock:
                    state = self.state_memory.get(key)
                    state_copy = state.copy() if state else None
                if state_copy:
                    self._save_state_snapshot_dual(key, state_copy)
            except Exception as e:
                logger.error(f"Error saving snapshot after stop alert for '{key}': {e}")

    def _trigger_stop_alert(
        self, topic: str, batch_data: dict, duration: float
    ) -> None:
        """Build and dispatch a stop alert for a silent production line.

        Args:
            topic: Virtual topic identifying the line (``{mqtt_topic}/{device}``).
            batch_data: Snapshot of the line's last known active batch.
            duration: Silence duration in seconds.
        """
        minutes = int(duration // 60)
        product = batch_data.get("number_refactor", "Unknown")
        line = topic.split("/")[-1]

        msg = (
            f"STOP DETECTED: {line} has been inactive for {minutes} minutes "
            f"(last product: {product})"
        )
        logger.warning(msg)

        if self.alert_notifier:
            self.alert_notifier.send_alert(
                topic=topic,
                message=msg,
                alert_type="LINE STOP (>10min)",
                data={
                    "product": product,
                    "origin_line": line,
                    "current_line": "UNEXPECTED STOP",
                    "prev_start": batch_data.get("start_time"),
                    "prev_end": batch_data.get("end_time"),
                    "curr_start": datetime.now(timezone.utc),
                    "gap_seconds": duration,
                },
            )

    # ------------------------------------------------------------------
    # Main processing entry point
    # ------------------------------------------------------------------

    def process_chunk(self, df: pd.DataFrame, mqtt_topic: str) -> tuple:
        """Process a batch of records for a given MQTT topic.

        Groups records by device (line) and calls :meth:`_unsafe_process_logic`
        for each group under a per-topic lock.

        Args:
            df: Enriched DataFrame from the ETL pipeline.
            mqtt_topic: Source MQTT topic for state scoping.

        Returns:
            Tuple of ``(enriched_df, alerts)``.
        """
        with self._global_lock:
            if mqtt_topic not in self._locks:
                self._locks[mqtt_topic] = Lock()
            topic_lock = self._locks[mqtt_topic]

        with topic_lock:
            if df.empty:
                return df, []

            # Normalise timestamp to UTC
            if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
                df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            elif df["timestamp"].dt.tz is None:
                df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
            else:
                df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")

            max_ts = df["timestamp"].max()
            current_date_str = max_ts.strftime("%Y-%m-%d")
            counter_key = f"{mqtt_topic}_COUNTERS"

            # Initialise shared state structures if first time seen
            if counter_key not in self.state_memory:
                self.state_memory[counter_key] = {
                    "type": "GLOBAL_COUNTERS",
                    "counters": {},
                    "created_utc": max_ts.isoformat(),
                }

            if mqtt_topic not in self.state_memory:
                self.state_memory[mqtt_topic] = {
                    "type": "SHARED_HISTORY",
                    "logic_date": current_date_str,
                    "previous": [],
                    "daily_history": [],
                }

            shared_state = self.state_memory[mqtt_topic]

            # Daily reset
            if shared_state.get("logic_date") != current_date_str:
                logger.info(
                    "Daily reset triggered",
                    extra={"event": "daily_reset", "topic": mqtt_topic},
                )
                shared_state.update(
                    {
                        "previous": [],
                        "daily_history": [],
                        "logic_date": current_date_str,
                    }
                )

            group_col = self._detect_device_column(df)

            if group_col:
                parts = []
                alerts_total = []
                for device_name, device_df in df.groupby(group_col, sort=False):
                    virtual_topic = f"{mqtt_topic}/{device_name}"
                    enriched_df, new_alerts = self._unsafe_process_logic(
                        device_df.copy(),
                        state_key=virtual_topic,
                        history_key=mqtt_topic,
                        counter_key=counter_key,
                        max_ts_data=max_ts,
                    )
                    parts.append(enriched_df)
                    alerts_total.extend(new_alerts)

                return pd.concat(parts, ignore_index=False), alerts_total

            return self._unsafe_process_logic(
                df,
                state_key=mqtt_topic,
                history_key=mqtt_topic,
                counter_key=counter_key,
                max_ts_data=max_ts,
            )

    # ------------------------------------------------------------------
    # Core processing logic (must be called under topic_lock)
    # ------------------------------------------------------------------

    def _unsafe_process_logic(
        self,
        df: pd.DataFrame,
        state_key: str,
        history_key: str,
        counter_key: str,
        max_ts_data,
    ) -> tuple:
        """Detect article changes, assign batch IDs, and check for returns.

        Called *unsafe* because it must be invoked with the topic lock already
        held by the caller (:meth:`process_chunk`).

        Args:
            df: Per-device DataFrame slice, sorted by timestamp on return.
            state_key: State key for this device (``{topic}/{device}``).
            history_key: Shared history key (the bare MQTT topic).
            counter_key: Global counter key (``{topic}_COUNTERS``).
            max_ts_data: Maximum timestamp of the full batch (for persistence).

        Returns:
            Tuple of ``(enriched_df, alerts)``.
        """
        alerts: list = []
        state_changed = False

        df = self.mapper.enrich_dataframe(df, target_col="number_refactor")
        df.sort_values(by="timestamp", inplace=True)
        current_date_str = df["timestamp"].max().strftime("%Y-%m-%d")

        # Calculate WiFi connection lag
        # CreationDateUtc = server arrival time (true UTC).
        # timestamp = PLC event time (Madrid local clock, mislabelled as UTC).
        # High lag → data was buffered during connectivity loss → not a real stop.
        connection_lag_seconds = 0.0
        if "CreationDateUtc" in df.columns:
            try:
                creation_utc = pd.to_datetime(df["CreationDateUtc"], utc=True)
                creation_madrid = creation_utc.dt.tz_convert(_MADRID_TZ)
                # Strip the incorrect UTC label and reinterpret as Madrid local
                ts_madrid = (
                    df["timestamp"].dt.tz_localize(None).dt.tz_localize(_MADRID_TZ)
                )
                lag_series = (creation_madrid - ts_madrid).dt.total_seconds()
                connection_lag_seconds = float(lag_series.clip(lower=0).max())
                if connection_lag_seconds > self.LAG_THRESHOLD_SECONDS:
                    logger.info(
                        f"WiFi lag detected on '{state_key}': "
                        f"{connection_lag_seconds / 60:.1f}min — possible post-reconnection burst"
                    )
            except Exception as e:
                logger.warning(
                    f"Could not calculate connection lag for '{state_key}': {e}"
                )

        # Initialise local state if first time seen
        if state_key not in self.state_memory:
            self.state_memory[state_key] = {
                "type": "LOCAL_STATE",
                "current": None,
                "logic_date": current_date_str,
                "stop_alert_sent": False,
                "last_connection_lag_seconds": 0.0,
            }

        local_state = self.state_memory[state_key]
        shared_state = self.state_memory[history_key]
        global_counters = self.state_memory[counter_key]

        # Update lag for watchdog consumption
        local_state["last_connection_lag_seconds"] = connection_lag_seconds
        state_changed = True

        # Reset stop alert when new activity is detected
        if local_state.get("stop_alert_sent", False):
            logger.info(f"Activity resumed on '{state_key}' — stop alert cleared")
            local_state["stop_alert_sent"] = False

        # Daily reset for this device
        if local_state.get("logic_date") != current_date_str:
            local_state.update(
                {
                    "current": None,
                    "logic_date": current_date_str,
                    "stop_alert_sent": False,
                    "last_connection_lag_seconds": 0.0,
                }
            )

        # Detect article change boundaries within the chunk
        change_flag = df["number_refactor"] != df["number_refactor"].shift()
        group_id = change_flag.cumsum()

        batches_df = (
            df.groupby([group_id.rename("gid"), "number_refactor"], sort=False)
            .agg(batch_start=("timestamp", "min"), batch_end=("timestamp", "max"))
            .reset_index()
        )

        batch_mapping: dict = {}

        for row in batches_df.itertuples(index=False):
            current_batch = local_state["current"]
            is_new_article = (
                current_batch is None
                or current_batch["number_refactor"] != row.number_refactor
            )

            is_timeout = False
            if not is_new_article and current_batch is not None:
                gap = (row.batch_start - current_batch["end_time"]).total_seconds()

                if gap > self.MAX_GAP_SECONDS:
                    is_timeout = True
                    logger.info(
                        f"Gap >1h on '{state_key}' — forcing new batch sequence"
                    )

                else:
                    # Day change: same article but different calendar day
                    try:
                        prev_date = (
                            current_batch["end_time"].astimezone(_MADRID_TZ).date()
                        )
                        curr_date = row.batch_start.astimezone(_MADRID_TZ).date()
                        if curr_date > prev_date:
                            is_timeout = True
                            logger.info(
                                f"Day change on '{state_key}' "
                                f"({prev_date} → {curr_date}) — forcing new batch sequence"
                            )
                    except Exception:
                        pass

                    # Post-WiFi burst: high lag + large inter-packet gap
                    if (
                        not is_timeout
                        and connection_lag_seconds > self.LAG_THRESHOLD_SECONDS
                    ):
                        if gap > self.WIFI_GAP_THRESHOLD:
                            is_timeout = True
                            logger.info(
                                f"Post-WiFi gap ({gap / 3600:.1f}h) on '{state_key}' "
                                "— forcing new batch sequence"
                            )

            if is_new_article or is_timeout:
                if current_batch is not None:
                    shared_state["previous"].append(current_batch)
                    shared_state["daily_history"].append(current_batch.copy())
                    if len(shared_state["previous"]) > self.MAX_PREVIOUS_BUFFER:
                        shared_state["previous"].pop(0)

                art_code = row.number_refactor
                global_counters["counters"].setdefault(art_code, 0)
                global_counters["counters"][art_code] += 1
                sequence = global_counters["counters"][art_code]

                batch_id = f"{art_code}_{sequence:04d}"
                safe_batch_id = "".join(
                    c for c in batch_id if c.isalnum() or c in "._-"
                )
                batch_reason = "timeout" if is_timeout else "new_article"

                local_state["current"] = {
                    "number_refactor": art_code,
                    "start_time": row.batch_start,
                    "end_time": row.batch_end,
                    "device": state_key.split("/")[-1],
                    "batch_id": safe_batch_id,
                    "batch_sequence": sequence,
                    "reason": batch_reason,
                }
                local_state["stop_alert_sent"] = False
                state_changed = True

                alert = self._check_global_return(
                    art_code,
                    row.batch_start,
                    shared_state,
                    state_key,
                    current_reason=batch_reason,
                )
                if alert:
                    alerts.append(alert)
                    if self.alert_notifier:
                        logger.info("Global return detected — dispatching alert email")
                        self.alert_notifier.send_alert(
                            topic=alert["topic"],
                            message=alert["msg"],
                            alert_type=alert["type"],
                            data=alert.get("data"),
                        )

            else:
                if row.batch_end > current_batch["end_time"]:
                    current_batch["end_time"] = row.batch_end
                    state_changed = True

            batch_mapping[row.gid] = {
                "id": local_state["current"]["batch_id"],
                "seq": local_state["current"]["batch_sequence"],
                "start": local_state["current"]["start_time"],
            }

        # Assign batch metadata columns
        gid_series = group_id.reindex(df.index)
        batch_data = gid_series.map(lambda g: batch_mapping.get(g, {}))
        df["batch_instance_id"] = batch_data.map(lambda d: d.get("id"))
        df["batch_sequence"] = batch_data.map(lambda d: d.get("seq"))
        df["batch_start_time"] = batch_data.map(lambda d: d.get("start"))

        if state_changed:
            self._save_daily_report_dual(history_key, shared_state, max_ts_data)
            self._save_state_snapshot_dual(state_key, local_state)
            self._save_global_counters(counter_key, global_counters, max_ts_data)

        return df, alerts

    # ------------------------------------------------------------------
    # Global return detection
    # ------------------------------------------------------------------

    def _check_global_return(
        self,
        new_refactor: str,
        new_start,
        shared_state: dict,
        state_key: str,
        current_reason: str,
    ) -> dict | None:
        """Check whether *new_refactor* was recently processed on another line.

        Timeout-triggered batches are excluded to avoid false positives from
        reconnection bursts.

        Args:
            new_refactor: Article code of the new batch.
            new_start: Start timestamp of the new batch.
            shared_state: Shared history dict for the MQTT topic.
            state_key: Virtual topic of the current device.
            current_reason: ``"new_article"`` or ``"timeout"``.

        Returns:
            Alert dict if a return is detected, otherwise ``None``.
        """
        if current_reason == "timeout":
            return None

        current_device = state_key.split("/")[-1]

        for prev in shared_state["previous"]:
            if prev["number_refactor"] != new_refactor:
                continue
            if new_start <= prev["end_time"]:
                continue

            device_origin = prev.get("device", "unknown line")
            gap_seconds = (new_start - prev["end_time"]).total_seconds()

            alert_data = {
                "product": new_refactor,
                "origin_line": device_origin,
                "current_line": current_device,
                "prev_start": prev.get("start_time"),
                "prev_end": prev["end_time"],
                "curr_start": new_start,
                "gap_seconds": gap_seconds,
            }

            try:
                end_time_str = (
                    prev["end_time"].astimezone(_MADRID_TZ).strftime("%H:%M:%S")
                )
            except Exception:
                end_time_str = str(prev["end_time"])

            msg = (
                f"GLOBAL RETURN: {new_refactor} — previously on {device_origin} "
                f"(ended {end_time_str}, gap {gap_seconds / 3600:.1f}h)"
            )
            logger.warning("Global return detected", extra={"event": "global_return"})

            return {
                "topic": state_key,
                "msg": msg,
                "timestamp": new_start,
                "type": "GLOBAL_RETURN",
                "data": alert_data,
            }

        return None

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _save_global_counters(
        self, counter_key: str, counter_data: dict, max_ts
    ) -> None:
        """Persist article sequence counters to disk.

        Args:
            counter_key: State memory key for the counters dict.
            counter_data: Counter dict to persist.
            max_ts: Timestamp used for the ``last_update`` metadata field.
        """
        try:
            safe_key = counter_key.replace("/", "_")
            self.LOCAL_STATE_DIR.mkdir(parents=True, exist_ok=True)

            save_data = counter_data.copy()
            save_data["stored_key"] = counter_key
            try:
                save_data["last_update_madrid"] = max_ts.astimezone(
                    _MADRID_TZ
                ).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                save_data["last_update_madrid"] = max_ts.isoformat()

            with open(
                self.LOCAL_STATE_DIR / f"{safe_key}.json", "w", encoding="utf-8"
            ) as f:
                json.dump(save_data, f, default=str, indent=2, ensure_ascii=False)

        except Exception as e:
            logger.error(f"Error saving global counters: {e}")

    def _save_daily_report_dual(self, topic: str, state_data: dict, max_ts) -> None:
        """Persist the daily production report to disk and MinIO.

        Args:
            topic: MQTT topic used to name the output file.
            state_data: Shared history dict for the topic.
            max_ts: Timestamp of the last record in the batch.
        """
        try:
            safe_topic = topic.replace("/", "_")
            spanish_state = self._to_spanish_time(state_data)
            date_str = spanish_state.get("logic_date")

            try:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            except Exception:
                date_obj = None

            try:
                last_update_str = max_ts.astimezone(_MADRID_TZ).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            except Exception:
                last_update_str = max_ts.isoformat()

            report = {
                "date": date_str,
                "topic": topic,
                "last_update": last_update_str,
                "productions_completed": spanish_state.get("daily_history", []),
                "total_changes": len(spanish_state.get("daily_history", [])),
                "metadata": "Timestamps based on PLC Event Time (Europe/Madrid)",
            }

            self.LOCAL_STATE_DIR.mkdir(parents=True, exist_ok=True)
            with open(
                self.LOCAL_STATE_DIR / f"{safe_topic}_GLOBAL.json",
                "w",
                encoding="utf-8",
            ) as f:
                json.dump(report, f, default=str, indent=2, ensure_ascii=False)

            if self.minio and date_obj:
                path = (
                    f"packaging/daily_production/{safe_topic}/"
                    f"{date_obj.year}/{date_obj.month:02d}/report_{date_str}.json"
                )
                self.minio.save_json(path, report)

        except Exception as e:
            logger.error(f"Error saving daily report: {e}")

    def _save_state_snapshot_dual(self, topic: str, state_data: dict) -> None:
        """Persist the local device state snapshot to disk.

        Args:
            topic: Virtual topic identifying the device.
            state_data: Local state dict for the device.
        """
        try:
            safe_topic = topic.replace("/", "_")
            self.LOCAL_STATE_DIR.mkdir(parents=True, exist_ok=True)
            snapshot = self._to_spanish_time(state_data)
            snapshot["stored_topic_key"] = topic
            with open(
                self.LOCAL_STATE_DIR / f"{safe_topic}_LOCAL.json", "w", encoding="utf-8"
            ) as f:
                json.dump(snapshot, f, default=str, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Error saving local state snapshot: {e}")

    def _restore_state_from_disk(self) -> None:
        """Reload persisted state from the local state directory on startup.

        Handles three file suffixes:
        - ``_COUNTERS.json`` — global article sequence counters.
        - ``_GLOBAL.json``   — daily production history.
        - ``_LOCAL.json``    — per-device current batch state.

        Stop alert flags from a previous day are forced to ``True`` to prevent
        spurious alerts caused by overnight silence.
        """
        if not self.LOCAL_STATE_DIR.exists():
            logger.info("No previous state directory found — starting clean")
            return

        logger.info("Restoring state from disk...")
        restored_count = 0
        today_str = datetime.now(_MADRID_TZ).strftime("%Y-%m-%d")

        for file_path in self.LOCAL_STATE_DIR.glob("*.json"):
            try:
                with open(file_path, encoding="utf-8") as f:
                    data = json.load(f)
                if not isinstance(data, dict):
                    continue

                name = file_path.name

                if "_COUNTERS.json" in name:
                    key = data.get("stored_key") or name.replace(".json", "").replace(
                        "_", "/"
                    )
                    self.state_memory[key] = {
                        "type": "GLOBAL_COUNTERS",
                        "counters": data.get("counters", {}),
                        "created_utc": data.get("created_utc"),
                    }
                    restored_count += 1

                elif "_GLOBAL.json" in name:
                    topic = data.get("topic")
                    if not topic:
                        continue
                    history = [
                        self._parse_timestamps(item)
                        for item in data.get("productions_completed", [])
                    ]
                    self.state_memory[topic] = {
                        "type": "SHARED_HISTORY",
                        "logic_date": data.get("date"),
                        "previous": history[-self.MAX_PREVIOUS_BUFFER :],
                        "daily_history": history,
                    }
                    restored_count += 1

                elif "_LOCAL.json" in name:
                    topic = data.get("stored_topic_key") or name.replace(
                        "_LOCAL.json", ""
                    ).replace("_", "/")
                    current = self._parse_timestamps(data.get("current"))
                    logic_date_str = data.get("logic_date")

                    # Force the stop alert flag on stale (previous-day) state to
                    # prevent overnight silence from triggering an alert on startup
                    stop_alert = data.get("stop_alert_sent", False)
                    if logic_date_str != today_str:
                        logger.info(
                            f"Stale state for '{topic}' ({logic_date_str}) "
                            "— suppressing startup stop alert"
                        )
                        stop_alert = True

                    self.state_memory[topic] = {
                        "type": "LOCAL_STATE",
                        "current": current,
                        "logic_date": logic_date_str,
                        "stop_alert_sent": stop_alert,
                        "last_connection_lag_seconds": data.get(
                            "last_connection_lag_seconds", 0.0
                        ),
                    }
                    if current:
                        restored_count += 1

            except Exception as e:
                logger.warning(f"Error reading state file '{file_path}': {e}")

        logger.info(f"State restore complete: {restored_count} entries loaded")

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------

    def _to_spanish_time(self, data) -> object:
        """Recursively convert datetime objects to Madrid-local strings for serialisation.

        Args:
            data: Any value — dict, list, datetime, pd.Timestamp, or scalar.

        Returns:
            Same structure with datetimes replaced by ``"%Y-%m-%d %H:%M:%S"`` strings.
        """
        if isinstance(data, dict):
            return {k: self._to_spanish_time(v) for k, v in data.items()}
        if isinstance(data, list):
            return [self._to_spanish_time(v) for v in data]
        if isinstance(data, (datetime, pd.Timestamp)):
            dt = data.to_pydatetime() if isinstance(data, pd.Timestamp) else data
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            try:
                return dt.astimezone(_MADRID_TZ).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return dt.isoformat()
        return data

    def _parse_timestamps(self, obj: dict | None) -> dict | None:
        """Parse ``start_time`` and ``end_time`` string fields back to UTC datetimes.

        Args:
            obj: Dict that may contain serialised timestamp strings.

        Returns:
            Dict with timestamp strings replaced by UTC-aware datetimes,
            or ``None`` if *obj* is falsy or not a dict.
        """
        if not obj or not isinstance(obj, dict):
            return obj

        parsed = obj.copy()
        for key in ("start_time", "end_time"):
            if key not in parsed or not isinstance(parsed[key], str):
                continue
            try:
                dt = pd.to_datetime(parsed[key])
                if dt.tz is None:
                    dt = dt.tz_localize(_MADRID_TZ)
                parsed[key] = dt.tz_convert(timezone.utc)
            except Exception:
                parsed[key] = None

        return parsed

    def _detect_device_column(self, df: pd.DataFrame) -> str | None:
        """Return the first non-null device identifier column found in *df*.

        Args:
            df: DataFrame to inspect.

        Returns:
            Column name, or ``None`` if none of the candidate columns are present
            or populated.
        """
        for col in ("DeviceName", "linea", "device_name", "device_id"):
            if col in df.columns and not df[col].isnull().all():
                return col
        return None

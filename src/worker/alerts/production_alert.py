"""
Asynchronous email alert system for production events (line stops and product returns).
"""

import json
import os
import queue
import re
import smtplib
import threading
import time
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from config import Config
from core.logger import setup_logger

logger = setup_logger(__name__)

_MADRID_TZ = ZoneInfo("Europe/Madrid")


class AlertNotifier:
    """Send production alert emails asynchronously via a background worker thread.

    Alerts are enqueued by :meth:`send_alert` (producer) and dispatched by a
    daemon thread (consumer) that holds the SMTP connection open for the duration
    of each batch to minimise connection overhead.

    Contact configuration is loaded from a JSON file with the schema::

        {
            "default": {"recipients": [{"email": "...", "manager": "..."}]},
            "topics": {
                "factory/line_01": {"recipients": [...]}
            }
        }

    Args:
        contacts_file: Path to the JSON contacts config file.
    """

    def __init__(self, contacts_file: str = "/app/data/contacts.json"):
        self.contacts_file = contacts_file
        self.contacts: dict = {}
        self.default_contact: dict = {}
        self._mailbox: queue.Queue = queue.Queue()
        self.running = True

        self._load_contacts()

        self._worker_thread = threading.Thread(
            target=self._mailbox_worker,
            daemon=True,
            name="EmailWorker",
        )
        self._worker_thread.start()
        logger.info(f"Alert notifier started (sender: {Config.SMTP_USER})")

    # ------------------------------------------------------------------
    # Contact management
    # ------------------------------------------------------------------

    def _load_contacts(self) -> None:
        """Load the contact list from disk. Logs a warning if the file is absent."""
        try:
            if os.path.exists(self.contacts_file):
                with open(self.contacts_file, encoding="utf-8") as f:
                    data = json.load(f)
                self.contacts = data.get("topics", {})
                self.default_contact = data.get("default", {})
            else:
                logger.warning(f"Contacts file not found: {self.contacts_file}")
        except Exception as e:
            logger.error(f"Error loading contacts: {e}")

    def get_contact_config(self, topic: str) -> dict:
        """Return the contact configuration for *topic*, falling back to default.

        Args:
            topic: MQTT topic string.

        Returns:
            Contact configuration dict.
        """
        return self.contacts.get(topic, self.default_contact)

    # ------------------------------------------------------------------
    # Producer
    # ------------------------------------------------------------------

    def send_alert(
        self,
        topic: str,
        message: str,
        alert_type: str = "RETURN DETECTED",
        data: dict | None = None,
    ) -> None:
        """Enqueue an alert for asynchronous email dispatch.

        Args:
            topic: MQTT topic associated with the event.
            message: Human-readable alert body.
            alert_type: Short type label used in the email subject.
            data: Optional structured data dict for HTML template rendering.
        """
        self._mailbox.put(
            {
                "topic": topic,
                "message": message,
                "alert_type": alert_type,
                "timestamp": datetime.now(timezone.utc),
                "data": data,
            }
        )

        queue_depth = self._mailbox.qsize()
        if queue_depth > 2:
            logger.debug(f"Alert enqueued — pending: {queue_depth}")

    # ------------------------------------------------------------------
    # Consumer
    # ------------------------------------------------------------------

    def _mailbox_worker(self) -> None:
        """Background thread: drain the alert queue and dispatch emails."""
        while self.running:
            try:
                try:
                    pkg = self._mailbox.get(timeout=1.0)
                except queue.Empty:
                    continue

                self._process_dispatch(pkg)
                self._mailbox.task_done()

            except Exception as e:
                logger.error(f"Email worker error: {e}")
                time.sleep(1)

    def _process_dispatch(self, pkg: dict) -> None:
        """Resolve recipients and hand off to the SMTP sender.

        Args:
            pkg: Alert package dict from the mailbox queue.
        """
        topic = pkg["topic"]
        config = self.get_contact_config(topic)
        recipients = config.get("recipients", [])

        # Support legacy single-recipient config format
        if not recipients and "email" in config:
            recipients = [config]

        valid_recipients = [r for r in recipients if r.get("email")]
        if not valid_recipients:
            return

        self._send_smtp_bulk(
            recipients=valid_recipients,
            subject=f"ALERT: {pkg['alert_type']} — {topic}",
            message=pkg["message"],
            alert_type=pkg["alert_type"],
            timestamp=pkg["timestamp"],
            topic=topic,
            alert_data=pkg.get("data"),
        )

    def _send_smtp_bulk(
        self,
        recipients: list,
        subject: str,
        message: str,
        alert_type: str,
        timestamp: datetime,
        topic: str,
        alert_data: dict | None = None,
    ) -> None:
        """Open a single SMTP connection and send to all recipients.

        Individual send failures are logged but do not abort delivery to
        remaining recipients.

        Args:
            recipients: List of contact dicts with at least an ``"email"`` key.
            subject: Email subject line.
            message: Plain-text alert body (used for logging; HTML is generated).
            alert_type: Short type label for template selection.
            timestamp: UTC datetime of the alert event.
            topic: Originating MQTT topic.
            alert_data: Structured data forwarded to the HTML template.
        """
        try:
            with smtplib.SMTP(Config.SMTP_HOST, Config.SMTP_PORT) as server:
                server.starttls()
                server.login(Config.SMTP_USER, Config.SMTP_PASSWORD)
                logger.info(
                    f"SMTP connected — sending to {len(recipients)} recipient(s)"
                )

                for person in recipients:
                    dest_email = person.get("email")
                    try:
                        html_body = self._render_html(
                            topic, person, message, alert_type, timestamp, alert_data
                        )
                        msg = MIMEMultipart()
                        msg["From"] = f"{Config.SMTP_FROM_NAME} <{Config.SMTP_USER}>"
                        msg["To"] = dest_email
                        msg["Subject"] = subject
                        msg.attach(MIMEText(html_body, "html"))

                        server.send_message(msg)
                        logger.info(f"Alert sent to {dest_email}")
                        time.sleep(
                            0.5
                        )  # Brief pause between sends to avoid rate limits

                    except Exception as e:
                        logger.error(f"Failed to send alert to {dest_email}: {e}")

                logger.info("Alert batch complete")

        except smtplib.SMTPResponseException as e:
            if e.smtp_code in (554, 535):
                logger.error(f"SMTP authentication error: {e}")
            else:
                logger.error(f"SMTP error: {e}")
        except Exception as e:
            logger.error(f"SMTP connection failed: {e}")

    # ------------------------------------------------------------------
    # HTML template
    # ------------------------------------------------------------------

    def _render_html(
        self,
        topic: str,
        person: dict,
        message: str,
        alert_type: str,
        timestamp: datetime,
        alert_data: dict | None = None,
    ) -> str:
        """Render the alert email as an HTML string.

        The template adapts its colour scheme and layout depending on whether
        the alert is a line stop or a product return event.

        Args:
            topic: Originating MQTT topic (shown in the email footer).
            person: Recipient contact dict (used for the manager name).
            message: Plain-text alert summary (unused in HTML; kept for signature).
            alert_type: Type label that controls template branching.
            timestamp: UTC event timestamp.
            alert_data: Structured data dict with product, line, and timing fields.

        Returns:
            Fully rendered HTML string.
        """

        def _fmt_time(dt: datetime | None) -> str:
            if not dt:
                return "--:--"
            try:
                local = (
                    dt.astimezone(_MADRID_TZ)
                    if dt.tzinfo
                    else dt.replace(tzinfo=timezone.utc).astimezone(_MADRID_TZ)
                )
                return local.strftime("%H:%M:%S")
            except Exception:
                return str(dt)

        def _fmt_date(dt: datetime) -> str:
            try:
                local = (
                    dt.astimezone(_MADRID_TZ)
                    if dt.tzinfo
                    else dt.replace(tzinfo=timezone.utc).astimezone(_MADRID_TZ)
                )
                return local.strftime("%d/%m/%Y")
            except Exception:
                return str(dt)

        def _fmt_line(raw: str | None) -> str:
            if not raw:
                return "N/A"
            s = str(raw).strip()
            if "PARADA" in s.upper():
                return s
            match = re.search(r"[Ll][ií]nea[_\s]*(\d+)", s)
            if match:
                return f"LINEA {match.group(1)}"
            return s.replace("Linea_", "Linea ").replace("_", " ").upper()

        is_stop = "PARADA" in str(alert_type).upper()
        d = alert_data or {}

        product_name = d.get("product", "Unknown Product")
        origin_line = _fmt_line(d.get("origin_line", "N/A"))
        current_line = _fmt_line(d.get("current_line", topic.split("/")[-1]))

        gap_sec = d.get("gap_seconds", 0)
        gap_str = f"{int(gap_sec // 3600)}h {int((gap_sec % 3600) // 60)}m"

        # Theme colours
        if is_stop:
            form_url = Config.FORMS_STOP
            button_text = "Justificar Parada"
            main_title = "Alerta de Parada"
            label_top = "LÍNEA DETENIDA"
            header_bg = "#F57C00"
            curr_bg = "#FFF3E0"
            curr_border = "#FF9800"
            curr_text = "#E65100"
            status_label = "LÍNEA AFECTADA"
        else:
            form_url = Config.FORMS_RETURN
            button_text = "Justificar Incidencia"
            main_title = "Alerta de Retorno"
            label_top = "REFERENCIA REPETIDA"
            header_bg = "#C62828"
            curr_bg = "#FFEBEE"
            curr_border = "#D32F2F"
            curr_text = "#B71C1C"
            status_label = "ESTADO ACTUAL"

        body_bg = "#F4F7F6"
        text_dark = "#263238"

        if is_stop:
            content_html = f"""
            <div style="background-color:{curr_bg};border:2px solid {curr_border};padding:20px;border-radius:8px;text-align:center;">
                <div style="font-size:11px;font-weight:700;color:{curr_text};text-transform:uppercase;margin-bottom:8px;">{status_label}</div>
                <div style="font-size:22px;font-weight:800;color:{curr_text};">{current_line}</div>
                <div style="font-size:14px;color:{curr_text};margin-top:10px;">
                    Inactive since: <strong>{_fmt_time(d.get("curr_start"))}</strong>
                </div>
                <div style="margin-top:15px;border-top:1px dashed {curr_border};padding-top:10px;font-size:13px;color:{curr_text};">
                    Minimum stop duration: <strong>{gap_str}</strong>
                </div>
            </div>"""
        else:
            content_html = f"""
            <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:separate;border-spacing:0;">
                <tr>
                    <td width="45%" style="vertical-align:top;">
                        <div style="font-size:10px;font-weight:700;text-transform:uppercase;color:#546E7A;margin-bottom:5px;">Last activity on:</div>
                        <div style="background-color:#ECEFF1;border-left:4px solid #B0BEC5;padding:12px;border-radius:4px;margin-bottom:8px;">
                            <div style="font-size:14px;font-weight:700;color:#546E7A;">{origin_line}</div>
                        </div>
                        <div style="font-size:12px;color:#78909C;">End: <strong>{_fmt_time(d.get("prev_end"))}</strong></div>
                    </td>
                    <td width="10%" style="text-align:center;vertical-align:middle;padding:0 5px;">
                        <div style="font-size:22px;color:#B0BEC5;">→</div>
                        <div style="font-size:10px;font-weight:700;color:{header_bg};margin-top:-5px;">{gap_str}</div>
                    </td>
                    <td width="45%" style="vertical-align:top;">
                        <div style="font-size:10px;font-weight:700;text-transform:uppercase;color:{curr_text};margin-bottom:5px;">{status_label}:</div>
                        <div style="background-color:{curr_bg};border-left:4px solid {curr_border};padding:12px;border-radius:4px;margin-bottom:8px;">
                            <div style="font-size:14px;font-weight:700;color:{curr_text};">{current_line}</div>
                        </div>
                        <div style="font-size:12px;color:{curr_text};">Detected: <strong>{_fmt_time(d.get("curr_start"))}</strong></div>
                    </td>
                </tr>
            </table>"""

        return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width,initial-scale=1.0">
    <title>{main_title}</title>
</head>
<body style="margin:0;padding:0;font-family:'Segoe UI',Helvetica,Arial,sans-serif;background-color:{body_bg};color:{text_dark};">
    <div style="max-width:600px;margin:20px auto;background-color:#ffffff;border-radius:8px;overflow:hidden;box-shadow:0 4px 12px rgba(0,0,0,0.1);">
        <div style="background-color:{header_bg};padding:20px 25px;">
            <table width="100%" cellpadding="0" cellspacing="0">
                <tr>
                    <td style="color:#ffffff;font-weight:700;font-size:16px;text-transform:uppercase;letter-spacing:1px;">{main_title}</td>
                    <td style="text-align:right;color:rgba(255,255,255,0.8);font-size:13px;">{_fmt_date(timestamp)} &bull; {_fmt_time(timestamp)}</td>
                </tr>
            </table>
        </div>
        <div style="padding:30px;">
            <div style="text-align:center;margin-bottom:35px;">
                <span style="font-size:11px;font-weight:700;text-transform:uppercase;color:#90A4AE;letter-spacing:1.2px;">{label_top}</span>
                <h1 style="margin:8px 0 0 0;font-size:26px;color:{text_dark};font-weight:800;">{product_name}</h1>
            </div>
            {content_html}
            <div style="margin-top:40px;text-align:center;border-top:1px solid #ECEFF1;padding-top:25px;">
                <p style="font-size:13px;margin-bottom:20px;color:{text_dark};">
                    Action required from: <strong>{person.get("manager", "Manager")}</strong>
                </p>
                <a href="{form_url}" target="_blank"
                   style="background-color:{header_bg};color:#ffffff;padding:14px 30px;text-decoration:none;border-radius:6px;font-weight:700;font-size:14px;display:inline-block;box-shadow:0 3px 6px rgba(0,0,0,0.1);">
                    {button_text}
                </a>
            </div>
        </div>
        <div style="background-color:#FAFAFA;padding:15px;text-align:center;border-top:1px solid #EEEEEE;">
            <div style="font-size:11px;color:#90A4AE;font-family:monospace;">TOPIC: {topic}</div>
        </div>
    </div>
</body>
</html>"""

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def stop(self) -> None:
        """Signal the worker thread to stop and wait for it to finish."""
        self.running = False
        if self._worker_thread.is_alive():
            self._worker_thread.join(timeout=5)

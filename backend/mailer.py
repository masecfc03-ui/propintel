"""
Mailer — sends PropIntel reports via email.
Supports SendGrid API (preferred) and SMTP fallback.

Setup:
  Option A: Set SENDGRID_API_KEY in .env (free tier = 100/day)
  Option B: Set SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS in .env
"""
import os
import smtplib
import json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import Optional

SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY", "")
MAILGUN_API_KEY = os.environ.get("MAILGUN_API_KEY", "")
MAILGUN_DOMAIN = os.environ.get("MAILGUN_DOMAIN", "mathislandco.com")
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")

FROM_EMAIL = os.environ.get("PROPINTEL_FROM_EMAIL", "mason@mathislandco.com")
FROM_NAME = "PropIntel Reports"
REPORT_BASE_URL = os.environ.get("REPORT_BASE_URL", "https://masecfc03-ui.github.io/propintel")


def send_report(to_email: str, to_name: str, address: str,
                tier: str, report_html: str, report_id: str,
                order_id: str = "", report_token: str = "") -> dict:
    """
    Send the PropIntel report to a customer.

    Returns: {"success": True/False, "method": "sendgrid|smtp|none", "error": str}
    """
    if not to_email:
        return {"success": False, "method": "none", "error": "No recipient email"}

    subject = f"PropIntel {'Full Intel' if tier == 'pro' else 'Public Record'} Report — {address}"
    html_body = _build_email_body(to_name, address, tier, report_html, report_id, order_id, report_token)

    # Try Mailgun first (already configured)
    if MAILGUN_API_KEY and MAILGUN_DOMAIN:
        result = _send_mailgun(to_email, to_name, subject, html_body)
        if result["success"]:
            return result

    # Try SendGrid
    if SENDGRID_API_KEY:
        result = _send_sendgrid(to_email, to_name, subject, html_body)
        if result["success"]:
            return result

    # Fallback to SMTP
    if SMTP_USER and SMTP_PASS:
        result = _send_smtp(to_email, to_name, subject, html_body)
        return result

    return {"success": False, "method": "none", "error": "No email provider configured"}


def _build_email_body(name: str, address: str, tier: str,
                       report_html: str, report_id: str, order_id: str,
                       report_token: str = "") -> str:
    """Build a clean email with embedded report link + summary."""
    tier_label = "Full Intel" if tier == "pro" else "Public Record"
    tier_color = "#22c55e" if tier == "pro" else "#3b82f6"
    # Use token-based URL (no admin auth needed) when available
    if report_token:
        report_url = f"{REPORT_BASE_URL}/report.html?token={report_token}"
    else:
        report_url = f"{REPORT_BASE_URL}/report.html?tier={tier}&report_id={report_id}"

    greeting = f"Hi{' ' + name.split()[0] if name else ''},"

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: #f8fafc; margin: 0; padding: 0; }}
  .wrap {{ max-width: 600px; margin: 0 auto; background: white; }}
  .header {{ background: #1a1a1a; padding: 32px 40px; text-align: center; }}
  .logo {{ font-size: 1.2rem; font-weight: 800; color: white; text-decoration: none; }}
  .logo-dot {{ display: inline-block; background: #3b82f6; border-radius: 6px;
    width: 22px; height: 22px; text-align: center; line-height: 22px;
    font-size: 12px; margin-right: 6px; vertical-align: middle; }}
  .body {{ padding: 40px; }}
  .subject-box {{ background: #f1f5f9; border-left: 4px solid {tier_color};
    border-radius: 8px; padding: 16px 20px; margin: 24px 0; }}
  .subject-label {{ font-size: 0.75rem; font-weight: 700; color: #64748b;
    text-transform: uppercase; letter-spacing: 1px; margin-bottom: 4px; }}
  .subject-address {{ font-size: 1.1rem; font-weight: 700; color: #0f172a; }}
  .tier-pill {{ display: inline-block; padding: 3px 10px; border-radius: 20px;
    font-size: 0.72rem; font-weight: 700; color: white;
    background: {tier_color}; margin-top: 8px; }}
  .cta {{ display: block; text-align: center; background: #3b82f6;
    color: white; padding: 16px 32px; border-radius: 50px;
    font-weight: 700; font-size: 1rem; text-decoration: none;
    margin: 32px 0; }}
  .features {{ background: #f8fafc; border-radius: 10px; padding: 20px 24px; margin: 24px 0; }}
  .feat-title {{ font-size: 0.8rem; font-weight: 700; color: #475569;
    text-transform: uppercase; letter-spacing: 1px; margin-bottom: 12px; }}
  .feat {{ display: flex; align-items: flex-start; gap: 10px;
    padding: 6px 0; font-size: 0.88rem; color: #334155; }}
  .footer {{ background: #f1f5f9; padding: 24px 40px; text-align: center;
    font-size: 0.75rem; color: #94a3b8; }}
</style>
</head>
<body>
<div class="wrap">

  <div class="header">
    <a href="https://propertyvalueintel.com" class="logo">
      <span class="logo-dot">🔍</span>PropIntel
    </a>
  </div>

  <div class="body">
    <p style="font-size:1rem;color:#334155">{greeting}</p>
    <p style="color:#475569;margin-top:12px;line-height:1.6">
      Your PropIntel <strong>{tier_label} Report</strong> is ready.
      Every data point comes directly from a verified government source.
    </p>

    <div class="subject-box">
      <div class="subject-label">Subject Property</div>
      <div class="subject-address">{address}</div>
      <div class="tier-pill">{tier_label.upper()} REPORT</div>
    </div>

    <a href="{report_url}" class="cta">📊 View Your Report →</a>

    <div class="features">
      <div class="feat-title">What's in your report</div>
      {"".join(_get_features(tier))}
    </div>

    <p style="font-size:0.82rem;color:#94a3b8;line-height:1.6;margin-top:24px">
      Report ID: <strong style="color:#475569">{report_id}</strong><br>
      {'Order ID: ' + order_id + '<br>' if order_id else ''}
      This report is for informational purposes only. Not investment advice.
    </p>

    <p style="font-size:0.88rem;color:#475569;margin-top:20px">
      Questions? Reply to this email or visit
      <a href="https://propertyvalueintel.com" style="color:#3b82f6">propertyvalueintel.com</a>
    </p>
  </div>

  <div class="footer">
    &copy; 2026 PropIntel · propertyvalueintel.com<br>
    This report contains only verified public record data.
    PropIntel is not a licensed broker, appraiser, or attorney.
  </div>
</div>
</body>
</html>"""


def _get_features(tier: str) -> list:
    starter = [
        ("✅", "FEMA Flood Zone — NFHL REST API"),
        ("✅", "Census Demographics — ACS 5-Year Estimates"),
        ("✅", "County Parcel Record — DCAD"),
        ("✅", "Tax Delinquency Check"),
        ("✅", "Business Registrations at Address — TX SOS"),
        ("✅", "Automated Risk Flags"),
    ]
    pro_extra = [
        ("🔍", "Owner Intelligence — DataZapp Skip Trace"),
        ("🎯", "Seller Motivation Score — 7 verified indicators"),
        ("🔗", "LLC Pierce — TX SOS filing history"),
        ("📄", "Pre-Filled Letter of Intent (LOI)"),
        ("🔒", "Lien Search — County Clerk"),
        ("📊", "Deal Analyzer — DSCR, Cash-on-Cash, CAP"),
    ]
    features = starter + pro_extra if tier == "pro" else starter
    return [f'<div class="feat"><span>{icon}</span><span>{label}</span></div>'
            for icon, label in features]


def _send_mailgun(to_email: str, to_name: str, subject: str, html_body: str) -> dict:
    """Send via Mailgun API (mathislandco.com domain)."""
    import urllib.request
    import urllib.parse
    import urllib.error
    import base64

    url = "https://api.mailgun.net/v3/{}/messages".format(MAILGUN_DOMAIN)
    from_addr = "{} <{}>".format(FROM_NAME, FROM_EMAIL)
    to_addr = "{} <{}>".format(to_name or "", to_email) if to_name else to_email

    data = urllib.parse.urlencode({
        "from": from_addr,
        "to": to_addr,
        "subject": subject,
        "html": html_body,
    }).encode("utf-8")

    credentials = base64.b64encode("api:{}".format(MAILGUN_API_KEY).encode()).decode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": "Basic {}".format(credentials),
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            if resp.status in (200, 202):
                return {"success": True, "method": "mailgun"}
            return {"success": False, "method": "mailgun",
                    "error": "Mailgun returned {}".format(resp.status)}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        return {"success": False, "method": "mailgun",
                "error": "{}: {}".format(e.code, body[:200])}
    except Exception as e:
        return {"success": False, "method": "mailgun", "error": str(e)}


def _send_sendgrid(to_email: str, to_name: str, subject: str, html_body: str) -> dict:
    """Send via SendGrid HTTP API."""
    import urllib.request
    import urllib.error

    payload = json.dumps({
        "personalizations": [{"to": [{"email": to_email, "name": to_name or ""}]}],
        "from": {"email": FROM_EMAIL, "name": FROM_NAME},
        "subject": subject,
        "content": [{"type": "text/html", "value": html_body}],
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data=payload,
        headers={
            "Authorization": f"Bearer {SENDGRID_API_KEY}",
            "Content-Type": "application/json",
        },
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            if resp.status in (200, 202):
                return {"success": True, "method": "sendgrid"}
            return {"success": False, "method": "sendgrid",
                    "error": f"SendGrid returned {resp.status}"}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        return {"success": False, "method": "sendgrid", "error": f"{e.code}: {body[:200]}"}
    except Exception as e:
        return {"success": False, "method": "sendgrid", "error": str(e)}


def _send_smtp(to_email: str, to_name: str, subject: str, html_body: str) -> dict:
    """Send via SMTP (Gmail, etc.)."""
    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = f"{FROM_NAME} <{SMTP_USER}>"
        msg["To"] = to_email
        msg["Subject"] = subject

        msg.attach(MIMEText("Your PropIntel report is ready. Please view in HTML.", "plain"))
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)

        return {"success": True, "method": "smtp"}
    except Exception as e:
        return {"success": False, "method": "smtp", "error": str(e)}

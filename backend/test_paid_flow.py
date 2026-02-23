"""
PropIntel — End-to-End Paid Flow Test
Tests: analyze → generate-token → report accessible → admin endpoints

Usage:
  cd backend
  python3 test_paid_flow.py                       # hit live Render
  python3 test_paid_flow.py --local               # hit local :5001
  python3 test_paid_flow.py --address "123 Main"  # custom address
"""
import sys
import json
import time
import urllib.request
import urllib.error
import argparse

# ── CONFIG ───────────────────────────────────────────────────────────────────
PROD_BASE  = "https://propintel-htij.onrender.com"
LOCAL_BASE = "http://localhost:5001"
ADMIN_KEY  = "propintel-mason-2026"
TEST_ADDR  = "3229 Forest Ln, Garland TX 75042"
TEST_EMAIL = "masonmathis03@gmail.com"

RESET  = "\033[0m"
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
BOLD   = "\033[1m"

def ok(msg):   print(f"{GREEN}  ✓ {msg}{RESET}")
def fail(msg): print(f"{RED}  ✗ {msg}{RESET}")
def info(msg): print(f"{BLUE}  ℹ {msg}{RESET}")
def warn(msg): print(f"{YELLOW}  ⚠ {msg}{RESET}")
def hdr(msg):  print(f"\n{BOLD}{msg}{RESET}")


def _req(url, data=None, headers=None, method=None):
    """Simple HTTP helper — returns (status_code, json_or_text)."""
    req_headers = {"Content-Type": "application/json"}
    if headers:
        req_headers.update(headers)
    body = json.dumps(data).encode() if data is not None else None
    meth = method or ("POST" if body else "GET")
    req  = urllib.request.Request(url, data=body, headers=req_headers, method=meth)
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode()
            try:
                return resp.status, json.loads(raw)
            except Exception:
                return resp.status, raw
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, raw


def test_health(base):
    hdr("1. Health Check")
    status, data = _req(f"{base}/api/health")
    if status == 200 and isinstance(data, dict):
        ok(f"API up — version {data.get('version','?')}")
        for k, v in (data.get("services") or {}).items():
            sym = "✓" if v == "ok" else "⚠"
            clr = GREEN if v == "ok" else YELLOW
            print(f"      {clr}{sym} {k}: {v}{RESET}")
        return True
    else:
        fail(f"Health check failed: {status} — {data}")
        return False


def test_analyze(base, address):
    hdr("2. Analyze Endpoint")
    start = time.time()
    status, data = _req(f"{base}/api/analyze", {"input": address, "tier": "pro"})
    elapsed = time.time() - start
    if status == 200 and isinstance(data, dict) and not data.get("error"):
        ok(f"Pipeline ran in {elapsed:.1f}s")
        p = data.get("parcel") or {}
        ok(f"APN: {p.get('apn','N/A')} | Owner: {p.get('owner_name','N/A')}")
        ok(f"Assessed: ${(p.get('assessed_total') or 0):,.0f}")
        m = data.get("motivation") or {}
        ok(f"Motivation: {m.get('score','N/A')}/100 [{m.get('tier','?')}]")
        f = data.get("flood") or {}
        ok(f"Flood zone: {f.get('zone','N/A')} — {f.get('description','')}")
        return True, data
    elif status == 429:
        warn(f"Rate limited — wait and retry. {data}")
        return False, None
    else:
        fail(f"Analyze failed: {status} — {data}")
        return False, None


def test_generate_token(base, address):
    hdr("3. Generate Token (No Email)")
    status, data = _req(
        f"{base}/api/admin/generate-token",
        {"address": address, "tier": "pro"},
        headers={"X-Admin-Key": ADMIN_KEY}
    )
    if status == 200 and isinstance(data, dict) and data.get("token"):
        ok(f"Token generated: {data['token'][:16]}…")
        ok(f"Report URL: {data.get('report_url','N/A')}")
        info(f"Motivation score in token response: {data.get('motivation_score','N/A')}")
        return True, data.get("token"), data.get("report_url")
    else:
        fail(f"generate-token failed: {status} — {data}")
        return False, None, None


def test_report_access(base, token):
    hdr("4. Report Access via Token")
    if not token:
        warn("No token to test — skipping")
        return False
    status, data = _req(f"{base}/api/reports/{token}")
    if status == 200 and isinstance(data, dict):
        ok(f"Report accessible: {len(json.dumps(data))} bytes")
        ok(f"Address in report: {data.get('resolved_address') or data.get('input','N/A')}")
        return True
    else:
        fail(f"Report access failed: {status} — {data}")
        return False


def test_admin_stats(base):
    hdr("5. Admin Stats")
    status, data = _req(
        f"{base}/api/stats",
        headers={"X-Admin-Key": ADMIN_KEY}
    )
    if status == 200 and isinstance(data, dict):
        ok(f"Total orders: {data.get('total_orders',0)}")
        ok(f"Revenue: ${data.get('total_revenue',0):.2f}")
        ok(f"Pro reports: {data.get('pro_orders',0)} | Starter: {data.get('starter_orders',0)}")
        return True
    else:
        fail(f"Stats failed: {status} — {data}")
        return False


def test_email_send(base, address, email):
    hdr(f"6. Test Email → {email}")
    status, data = _req(
        f"{base}/api/admin/test-email?admin_key={ADMIN_KEY}",
        {"address": address, "email": email, "tier": "pro"}
    )
    if status == 200 and isinstance(data, dict) and data.get("sent"):
        ok(f"Email sent via {data.get('method','?')}")
        ok(f"PDF attached: {data.get('pdf_attached', False)}")
        if data.get("pdf_error"):
            warn(f"PDF error: {data['pdf_error']}")
        return True
    else:
        fail(f"Email failed: {status} — {data}")
        return False


def main():
    parser = argparse.ArgumentParser(description="PropIntel E2E flow test")
    parser.add_argument("--local",   action="store_true", help="Test local server :5001")
    parser.add_argument("--address", default=TEST_ADDR,   help="Property address to test")
    parser.add_argument("--email",   default=TEST_EMAIL,  help="Email to send test report to")
    parser.add_argument("--skip-email", action="store_true", help="Skip email send test")
    args = parser.parse_args()

    base    = LOCAL_BASE if args.local else PROD_BASE
    address = args.address

    print(f"\n{BOLD}PropIntel E2E Test{RESET}")
    print(f"  Target: {BLUE}{base}{RESET}")
    print(f"  Address: {address}")

    results = {}

    results["health"]  = test_health(base)
    if not results["health"]:
        print(f"\n{RED}API is down — aborting{RESET}")
        sys.exit(1)

    results["analyze"], report_data, _ = (*test_analyze(base, address), None)

    ok_token, token, report_url = test_generate_token(base, address)
    results["generate_token"] = ok_token

    results["report_access"] = test_report_access(base, token)
    results["admin_stats"]   = test_admin_stats(base)

    if not args.skip_email:
        results["email"] = test_email_send(base, address, args.email)

    # Summary
    hdr("Summary")
    passed = sum(1 for v in results.values() if v)
    total  = len(results)
    for k, v in results.items():
        sym = f"{GREEN}PASS{RESET}" if v else f"{RED}FAIL{RESET}"
        print(f"  {sym}  {k}")

    if report_url:
        print(f"\n  {BLUE}Report URL:{RESET} {report_url}")

    color = GREEN if passed == total else (YELLOW if passed >= total * 0.7 else RED)
    print(f"\n  {color}{BOLD}{passed}/{total} tests passed{RESET}\n")
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()

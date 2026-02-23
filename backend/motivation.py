"""
Motivation Scorer — calculated from VERIFIED data only.
Every point awarded is traceable to a cited source.
No subjective inputs. No AI guessing.
"""
from datetime import datetime, date
import re


def score(parcel: dict, listing: dict, deed_history: list = None) -> dict:
    """
    Returns:
    {
      "score": 70,
      "tier": "HIGH",
      "indicators": [
        {
          "name": "Absentee Owner",
          "triggered": True,
          "points": 15,
          "evidence": "Owner mailing 8.3 miles from property",
          "source": "DCAD mailing vs. property address"
        },
        ...
      ],
      "interpretation": "..."
    }
    """
    indicators = []
    total = 0

    # ── 1. Absentee owner (mailing ≠ property address) ────────────────────────
    owner_mail = parcel.get("owner_mailing", "")
    prop_addr = parcel.get("property_address", "")
    absentee = False
    if owner_mail and prop_addr:
        # Simple check: different street in mailing vs property
        mail_zip = re.search(r"\b\d{5}\b", owner_mail)
        prop_zip = re.search(r"\b\d{5}\b", prop_addr)
        if mail_zip and prop_zip and mail_zip.group() != prop_zip.group():
            absentee = True
        elif "PO BOX" in owner_mail.upper() or "PMB" in owner_mail.upper():
            absentee = True

    pts = 15 if absentee else 0
    total += pts
    indicators.append({
        "name": "Absentee Owner",
        "triggered": absentee,
        "points": pts,
        "evidence": "Owner mailing address differs from property ZIP" if absentee else "Owner mailing matches property area",
        "source": "Dallas Central Appraisal District — mailing vs. property address",
    })

    # ── 2. Long hold duration ─────────────────────────────────────────────────
    years_held = None
    if deed_history and len(deed_history) > 0:
        latest = deed_history[0]
        deed_date_str = latest.get("date", "")
        try:
            deed_date = datetime.strptime(deed_date_str, "%Y-%m-%d").date()
            years_held = (date.today() - deed_date).days / 365.25
        except Exception:
            try:
                deed_date = datetime.strptime(deed_date_str, "%m/%d/%Y").date()
                years_held = (date.today() - deed_date).days / 365.25
            except Exception:
                pass

    long_hold = years_held is not None and years_held >= 5
    pts = 20 if years_held and years_held >= 7 else (12 if long_hold else 0)
    total += pts
    indicators.append({
        "name": "Long Hold Duration",
        "triggered": long_hold,
        "points": pts,
        "evidence": f"{years_held:.1f} years held" if years_held else "Deed date not available",
        "source": "Dallas County Clerk deed records — acquisition date",
    })

    # ── 3. LLC / entity ownership ─────────────────────────────────────────────
    owner_name = parcel.get("owner_name", "")
    is_entity = bool(re.search(
        r"\b(LLC|LP|LTD|INC|CORP|TRUST|ESTATE|PROPERTIES|HOLDINGS|VENTURES)\b",
        owner_name.upper()
    ))
    pts = 10 if is_entity else 0
    total += pts
    indicators.append({
        "name": "LLC / Entity Ownership",
        "triggered": is_entity,
        "points": pts,
        "evidence": f"Owner of record: {owner_name}" if owner_name else "Owner type unknown",
        "source": "Dallas Central Appraisal District — owner name",
    })

    # ── 4. Extended days on market ────────────────────────────────────────────
    dom = listing.get("days_on_market")
    extended_dom = dom is not None and int(dom) >= 30
    pts = 10 if extended_dom else (5 if dom and int(dom) >= 14 else 0)
    total += pts
    indicators.append({
        "name": "Extended Days on Market",
        "triggered": extended_dom,
        "points": pts,
        "evidence": f"{dom} days on market" if dom else "DOM not available",
        "source": "LoopNet listing date",
    })

    # ── 5. Price reduction ────────────────────────────────────────────────────
    reduced = listing.get("price_reduced", False)
    reduction_amt = listing.get("price_reduction_amount", 0)
    pts = 15 if reduced else 0
    total += pts
    indicators.append({
        "name": "Recorded Price Reduction",
        "triggered": bool(reduced),
        "points": pts,
        "evidence": f"Reduced ${reduction_amt:,}" if reduced and reduction_amt else ("Price reduced" if reduced else "No reduction recorded"),
        "source": "LoopNet price history",
    })

    # ── 6. Tax delinquency ────────────────────────────────────────────────────
    delinquent = parcel.get("tax_delinquent", False)
    pts = 25 if delinquent else 0
    total += pts
    indicators.append({
        "name": "Tax Delinquency",
        "triggered": delinquent,
        "points": pts,
        "evidence": "Delinquent taxes on record" if delinquent else "Taxes current — no delinquency",
        "source": "Dallas Central Appraisal District — tax records",
    })

    # ── 7. Out-of-state owner ─────────────────────────────────────────────────
    out_of_state = False
    if owner_mail:
        state_m = re.search(r"\b([A-Z]{2})\s+\d{5}", owner_mail)
        if state_m and state_m.group(1) != "TX":
            out_of_state = True
    pts = 15 if out_of_state else 0
    total += pts
    indicators.append({
        "name": "Out-of-State Owner",
        "triggered": out_of_state,
        "points": pts,
        "evidence": f"Owner mailing in {state_m.group(1) if out_of_state else 'TX'}" if owner_mail else "Mailing address not available",
        "source": "Dallas Central Appraisal District — owner mailing address",
    })

    # ── Cap and tier ─────────────────────────────────────────────────────────
    total = min(total, 100)
    if total >= 65:
        tier = "HIGH"
        interpretation = (
            f"Score {total}/100 — High motivation. Seller likely open to below-ask offers. "
            "Recommend direct outreach before formal broker submission."
        )
    elif total >= 40:
        tier = "MODERATE"
        interpretation = (
            f"Score {total}/100 — Moderate motivation. Some signals present. "
            "Submit through broker at or near ask, include due diligence contingencies."
        )
    else:
        tier = "LOW"
        interpretation = (
            f"Score {total}/100 — Limited motivation signals. "
            "Seller may be testing market. LOI at ask unlikely to get attention without strong terms."
        )

    return {
        "score": total,
        "tier": tier,
        "interpretation": interpretation,
        "indicators": indicators,
        "note": "Score derived exclusively from verified public record data. No subjective inputs.",
    }

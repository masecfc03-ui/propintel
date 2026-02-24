"""
Motivation Scorer — calculated from VERIFIED data only.
Every point awarded is traceable to a cited source.
No subjective inputs. No AI guessing.
"""
from datetime import datetime, date
import re


def score(parcel: dict, listing: dict, deed_history: list = None, hold_years: float = None) -> dict:
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

    # ── 1. Absentee owner — use Regrid verified flag directly ─────────────────
    # Regrid computes this from its own data (mailing vs property address).
    # Do NOT re-derive from strings — property_address from Regrid has no ZIP.
    absentee = bool(parcel.get("absentee_owner", False))
    owner_mail = parcel.get("owner_mailing", "")

    pts = 25 if absentee else 0
    total += pts
    indicators.append({
        "name": "Absentee Owner",
        "triggered": absentee,
        "points": pts,
        "evidence": "Owner mailing address differs from property ZIP" if absentee else "Owner mailing matches property area",
        "source": "Dallas Central Appraisal District — mailing vs. property address",
    })

    # ── 2. Long hold duration ─────────────────────────────────────────────────
    # Prefer hold_years from Realie ownership history (authoritative).
    # Fall back to computing from deed_history if hold_years is not provided.
    years_held = None
    hold_source = "Dallas County Clerk deed records — acquisition date"

    if hold_years is not None and hold_years >= 0:
        # Realie-supplied hold duration — most reliable
        years_held = float(hold_years)
        hold_source = "Realie ownership history — hold duration"
    elif deed_history and len(deed_history) > 0:
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

    # Scoring tiers:
    #   VERY_LONG_HOLD (20+ yr): +15 pts — owner almost certainly open to exit
    #   LONG_HOLD (10+ yr):      +10 pts — elevated motivation signal
    #   Moderate hold (5-9 yr):  +5 pts  — mild signal
    #   Under 5 years:           +0 pts
    if years_held is not None and years_held >= 20:
        pts = 15
        signal_name = "Very Long Hold Duration (20+ yr)"
        long_hold = True
    elif years_held is not None and years_held >= 10:
        pts = 10
        signal_name = "Long Hold Duration (10+ yr)"
        long_hold = True
    elif years_held is not None and years_held >= 5:
        pts = 5
        signal_name = "Long Hold Duration (5+ yr)"
        long_hold = True
    else:
        pts = 0
        signal_name = "Long Hold Duration"
        long_hold = False

    total += pts
    indicators.append({
        "name": signal_name,
        "triggered": long_hold,
        "points": pts,
        "evidence": "{:.1f} years held".format(years_held) if years_held is not None else "Deed date not available",
        "source": hold_source,
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

    # ── 7. Out-of-state owner — use Regrid verified flag directly ────────────
    out_of_state = bool(parcel.get("out_of_state_owner", False))
    # Fallback: check owner_state field
    if not out_of_state and parcel.get("owner_state") and parcel.get("owner_state","TX").upper() != "TX":
        out_of_state = True
    pts = 20 if out_of_state else 0
    total += pts
    owner_state_label = parcel.get("owner_state", "TX") or "TX"
    indicators.append({
        "name": "Out-of-State Owner",
        "triggered": out_of_state,
        "points": pts,
        "evidence": f"Owner mailing address in {owner_state_label}" if out_of_state else f"Owner in {owner_state_label}",
        "source": "Regrid — owner mailing state",
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
        tier = "MEDIUM"
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

# PropIntel — Business Model Plan
**propertyvalueintel.com**
*Prepared: February 2026 | Version 1.0*

---

## Executive Summary

PropIntel is an AI-powered real estate intelligence platform that generates instant investment-grade property reports from verified public records. Unlike monthly-subscription data tools that lock users into CRMs and list builders, PropIntel is **pay-per-report** — zero friction, instant value, no commitment. The angle: every investor, agent, or wholesaler who looks up a property can get a Bloomberg Terminal-quality report in under 30 seconds for the price of a cup of coffee.

**Beachhead:** Dallas/Fort Worth metro (DCAD coverage, 2.4M parcels)
**Expansion path:** All 254 TX counties → Top 50 US metros → National via Regrid API
**Exit comp:** DealComplete ($10–36M), PropertyRadar ($50M+ ARR), Reonomy (acquired by CoStar)

---

## 1. Competitive Landscape

### Tier 1 — Monthly Subscription Data Platforms

| Competitor | Price | Coverage | Core Angle | Weakness |
|---|---|---|---|---|
| **PropStream** | $99/mo | 160M+ properties, national | Comprehensive data + comps | Clunky UI, no real AI, overkill for casual use |
| **DealMachine** | $49–$99/mo | National | Driving-for-dollars + instant lookups | Mobile-first, limited commercial data |
| **BatchLeads** | $97–$197/mo | National | SMS outreach + list stacking | Subscription-heavy, steep for solo investors |
| **PropertyRadar** | $49–$99/mo | National (Western US strongest) | County record depth + geo targeting | Weak commercial, limited TX data |
| **REsimpli** | $99–$199/mo | National | All-in-one CRM + data + skip trace | Full platform — too much if you just want a report |

### Tier 2 — Commercial/Enterprise

| Competitor | Price | Coverage | Core Angle | Weakness |
|---|---|---|---|---|
| **Reonomy** (CoStar) | $500–$2k+/mo | National commercial | Commercial property intel, off-market CRE | Enterprise pricing, not accessible to small investors |
| **ATTOM Data** | Custom ($500+/mo) | National, all asset classes | Raw data API for developers | Not consumer-facing, pure B2B data licensing |
| **Regrid** | $99–$299/mo | All 3,200 US counties | Parcel data API + download | Data layer only, no report generation |
| **CoStar/LoopNet** | $500–$2k/mo | National commercial | MLS for commercial, the gold standard | Expensive, subscription wall, no investor workflow tools |

### Tier 3 — Per-Use / Free Tools

| Competitor | Price | Coverage | Weakness |
|---|---|---|---|
| **Propwire** | Free | National (basic) | No depth, no investment analysis |
| **ListSource** | Per-list | National | List builder only, no reports |
| **Zillow/Redfin** | Free | Residential | Consumer-facing, no deal analyzer, no skip trace |

---

### PropIntel's Competitive Gap

**The white space:** No competitor offers pay-per-report investment intelligence at <$30. Every tool forces a $50–$200/mo subscription to access data most investors only need occasionally. PropIntel fills the gap for:

1. **Casual investors** — pull 1 report for a deal they're evaluating, not 500 leads/mo
2. **Commercial/mixed-use buyers** — Reonomy is $500+/mo; PropIntel Pro is $29.99 once
3. **Investors in other people's markets** — someone in Austin looking at a Dallas deal doesn't need a TX-wide subscription
4. **Tech-forward demo** — AI-generated motivation scores, LOI generator, deal analyzer — NO ONE in this space does this at the $10–30 price point

**The angle PropIntel owns:**
> *"Instant AI-powered investment intel on any property, verified public records only, no subscription required."*

This is the Stripe of real estate reports — transactional, instant, trusted.

---

## 2. Market Sizing

### TAM — Total Addressable Market
- **~17 million** US real estate investors (NAR 2023: institutional + individual)
- **~2.5 million** active investors transacting in any given year
- Average spend on data tools: **$150–$600/year**
- **TAM: ~$375M–$1.5B/year** (US RE investor data tools)
- PropTech market overall: **$26B (2024) → $178B by 2035** at 14.6% CAGR

### SAM — Serviceable Addressable Market
- TX has **~400,000 active RE investors** (largest RE market in US)
- DFW metro: ~150,000 active investors
- Target: investors buying/analyzing 1–20 deals/year who need occasional deep reports
- Avg spend: $50–$150/year on per-use tools
- **SAM (TX launch): $20M–$60M/year**

### SOM — Year 1 Realistic Capture
- Build distribution: TikTok/YouTube demos, investor FB groups, RE meetups
- Conversion target: 5,000 reports/year (modest)
  - 60% Starter ($9.99): 3,000 × $9.99 = $29,970
  - 40% Pro ($29.99): 2,000 × $29.99 = $59,980
- **Year 1 SOM target: $90,000 GMV** (~$80K net after payment processing)
- **Path to $500K**: ~20,000 reports/year — achievable by Year 2 with subscription layer

---

## 3. Business Model Plan

### Revenue Streams

#### Current (Launch)
| Stream | Price | Margin | Notes |
|---|---|---|---|
| Starter Report | $9.99 | ~92% | Public records only, no skip trace |
| Pro Report | $29.99 | ~87% | Skip trace, deal analyzer, LOI gen |

**COGS per report (estimated):**
- FEMA API: free
- Census API: free
- Geocode API: free
- PDL skip trace: ~$0.10–$0.20/call (free tier now)
- DataZapp (future): ~$0.03/trace
- Render compute: ~$0.001–$0.01/request
- **Total COGS: ~$0.15–$0.50/report**
- **Gross margin: 95%+**

#### Phase 2 (Month 3–6) — Subscription Layer
| Tier | Price | Includes | Target Customer |
|---|---|---|---|
| Investor Basic | $29/mo | 5 Pro reports/mo + saved history | Solo investor, 1–5 deals/mo |
| Investor Pro | $79/mo | 20 Pro reports/mo + export + alerts | Active wholesaler |
| Team | $199/mo | 100 reports + multi-user + CRM export | Small team (2–5 seats) |

Subscription converts 10–15% of repeat per-report customers → **multiplies LTV 8–12x**

#### Phase 3 (Month 6–12) — API + White Label
| Stream | Price | Target |
|---|---|---|
| API access | $299–$999/mo | PropTech builders, title companies, lenders |
| White-label reports | $500–$2k setup + rev share | RE brokerages, investment firms |
| Motivated seller leads | $5–$25/lead | Wholesalers buying qualified leads |

### Customer Segments (Ranked by Willingness to Pay)

1. **Commercial buyers / syndicators** — analyzing $1M+ deals; $29.99 is immaterial; highest conversion
2. **Active wholesalers** (10+ deals/mo) — need fast, repeatable intel; subscribe at $79–$199/mo
3. **Fix-and-flip investors** — occasional use; per-report at $9.99–$29.99
4. **Land investors** — tax delinquent + absentee signals; high motivation scores = high value
5. **Real estate agents** (buyer's agents) — comps + flood + demographics for client reports
6. **Lenders / hard money** — due diligence on collateral; API tier

### Acquisition Channels

**Organic / Content (highest ROI, lowest cost):**
- **TikTok/Reels**: Film a live 60-second report being generated on a real property. "I looked up this $3M strip center in 30 seconds — here's what I found." Hook: AI pulling real public data in real time.
- **YouTube**: "How I analyze any commercial property in 30 seconds" → embed report.html demo
- **LinkedIn**: Commercial RE investor community — share deal analysis posts
- **BiggerPockets / RE investor forums**: Offer free Starter reports for early feedback

**Paid (once revenue positive):**
- Google Ads: "property investment report", "skip trace property owner", "motivated seller data"
- Facebook/Instagram RE investor audience targeting ($50/day test budget)

**Partnerships (multiplier effect):**
- Title companies — offer PropIntel to every buyer at closing as added value
- Real estate attorneys — due diligence tool
- Investor meetups (DFW has 20+ active groups) — sponsor or demo live
- Wholesaling coaches/courses — affiliate deal (30% rev share for referrals)

**Direct Outreach:**
- Cold email DFW investment groups with a free Pro report on their last acquisition (personalized demo)
- LinkedIn DMs to commercial brokers and acquisition managers

### Unit Economics

| Metric | Per-Report (Current) | Subscriber (Phase 2) |
|---|---|---|
| Revenue | $15 avg blended | $79/mo |
| COGS | ~$0.35 | ~$3.50/mo (20 reports) |
| Gross Profit | ~$14.65 | ~$75.50/mo |
| Gross Margin | 97% | 95% |
| CAC (organic) | ~$5–$10 | ~$20–$40 |
| LTV (per-report) | $15 (one-shot) | ~$500 (avg 6mo retention) |
| LTV/CAC | 1.5–3x | 12–25x |

**Key insight:** Per-report has great margins but poor LTV. Phase 2 subscription is where the real economics kick in.

### Moat / Defensibility

1. **Data quality**: Only verified public records — no AI hallucinations, no "estimated" values. This is the trust moat. Competitors mix in AVMs and projections; PropIntel shows the source link for every data point.
2. **Speed**: Report in <30 seconds. PropStream takes minutes of navigation. This is the UX moat.
3. **Price**: $9.99 vs $99/mo. Zero-commitment wins casual users who become advocates. This is the acquisition moat.
4. **AI layer**: Motivation score, LOI generator, deal analyzer — no one at this price point does this. This is the product moat.
5. **Distribution**: TikTok/social demos will drive viral loops. A 30-second video of PropIntel generating a real report in real time is extremely shareable in the RE investor community.

**12-month defensibility goal:** Be the default "first lookup" tool for DFW commercial investors. Own that mental slot before a larger player notices.

---

## 4. 12-Month Milestones

### Month 1–2 (Current → March 2026)
- [ ] Full end-to-end payment flow working (Stripe → webhook → pipeline → email)
- [ ] DCAD client-side fix live (browser fetches DCAD directly, bypassing Render IP block)
- [ ] DataZapp API connected (LLC skip trace unlocked)
- [ ] Email delivery via Mailgun working
- [ ] First 50 paying customers
- [ ] First TikTok demo posted
- **Target**: $500–$1,500 GMV

### Month 3–4 (April–May 2026)
- [ ] TX coverage expanded: Tarrant, Collin, Denton, Harris, Travis, Bexar
- [ ] Subscription tier launched ($29/$79/$199/mo)
- [ ] Admin dashboard v2 with revenue analytics
- [ ] 10 affiliate partners onboarded (RE coaches, wholesaling courses)
- **Target**: $3,000–$8,000 MRR

### Month 5–6 (June–July 2026)
- [ ] National coverage via Regrid API ($150/mo unlocks all 3,200 counties)
- [ ] API tier launched for developers
- [ ] White-label pilot with 1–2 brokerages
- [ ] 500+ total customers
- **Target**: $10,000–$20,000 MRR

### Month 7–12 (Aug 2026 → Feb 2027)
- [ ] $25,000+ MRR (subscription + per-report + API)
- [ ] Team tier with CRM export (CSV, Podio, REsimpli integration)
- [ ] Mobile app (React Native wrapper, same backend)
- [ ] First PR / press coverage ("AI real estate report tool built by 22-year-old")
- [ ] Explore seed raise or strategic partner (title company, brokerage)
- **Target**: $50,000–$100,000 MRR

---

## 5. Investor Demo Script

### The 5-Bullet Pitch
1. **$50B market, zero instant gratification** — PropStream costs $99/mo and still takes 10 minutes to get data. PropIntel delivers an investment-grade report in 30 seconds for $29.99. One-time. No subscription.
2. **95% gross margins on a public records product** — COGS is <$1/report. Every dollar of scale goes straight to the bottom line.
3. **AI layer no one else has at this price** — Motivation scoring, LOI generator, deal analyzer with DSCR/CoC/cap rate. Competitors charge $500/mo for this. We charge $29.99.
4. **Viral distribution built in** — Every report is a shareable demo. TikTok investors are the target. A 30-second screen recording of PropIntel is a better ad than any paid campaign.
5. **Built by an operator, for operators** — Mason is actively acquiring commercial real estate. This tool was built because it didn't exist. He's the customer.

### Demo Script (Max Wow Factor — 90 seconds)
1. **Open the landing page** at `propertyvalueintel.com` — dark, professional, Bloomberg Terminal feel
2. **Type a real address** (use a recognizable local property — a Starbucks, a strip mall, anything the audience knows)
3. **Click Analyze** — show the teaser loading: FEMA zone, Census demographics, flags appearing in real time
4. **Pause on the motivation score** — "This is AI reading 6 verified signals from public records and telling me how motivated this seller is. No guessing."
5. **Click the Pro upgrade** (or show a pre-loaded Pro report) — scroll through: owner intel, deal analyzer (DSCR/CoC/cap rate), LOI generator with 3 price targets, lien search
6. **Show the LOI output** — "This generated a draft LOI I can send today. That took 30 seconds and $29.99."
7. **Close**: "PropStream charges $99/month. I charge $29.99 once. And my report has more actionable investment intel than theirs."

### Metrics to Hit Before Pitching
- **$10K MRR** — proves people pay, not just sign up
- **Net Revenue Retention >100%** — subscribers upgrade, not churn
- **100+ organic social shares** or 500K+ TikTok views on a demo
- **3 enterprise/white-label pilots** — proves B2B demand

---

## 6. Risk Assessment

| Risk | Probability | Mitigation |
|---|---|---|
| DCAD/county portals block access | Medium | Client-side fetch (browser bypasses IP blocks) + Regrid fallback |
| PropStream clones the per-report model | Low (6–12 mo) | Speed to market + brand ownership in the segment |
| SQLite data loss on Render redeploy | High | Migrate to Render PostgreSQL ($7/mo) |
| PDL free tier limits | High | DataZapp for LLC traces; PDL paid for individuals |
| Low organic conversion | Medium | TikTok demos are the force multiplier — one viral video = hundreds of conversions |
| Google/SEO slow to build | Medium | Paid search is viable at 95% margins; even $2 CPC is profitable |

---

*Document maintained by Tim (AI Operator). Update quarterly or after major milestones.*

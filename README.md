# Higgsfield Creator Scoring

Automated system to discover, enrich, score, classify, and prioritize YouTube creators whose audience is likely to convert into paid Higgsfield users.

## What this implements

- Category-driven discovery queries (7 strategic categories)
- Raw discovery hit logging and channel dedup by `channel_id`
- Channel + recent upload enrichment (10-20 videos)
- Signal extraction across titles/descriptions/comments
- Scoring engine with base dimensions, bonuses, penalties, and tiering
- Fit labeling (`strong_fit`, `medium_fit`, `weak_fit`, `disqualify`)
- OpenAI-powered 1-3 sentence fit comment (with fallback)
- Google Sheets output tabs:
  - `category_config`
  - `raw_discovery`
  - `creator_master`

## Setup

1. Create virtual environment and install deps:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Copy env vars and fill values:

```bash
cp .env.example .env
```

3. Export env vars from `.env` (or use your preferred env loader).

Required environment variables:

- `APIFY` (Apify API token)
- `GOOGLE_SHEETS_SPREADSHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_JSON_PATH`

Optional:

- `OPENAI_API_KEY` (for commentary generation)

4. Ensure your Google service account has edit access to the target sheet.

## Run

```bash
python main.py --config config/default_config.yaml --log-level INFO
```

## How the Scoring Logic Works

The scoring system was built to answer one practical growth question:

**How likely is this creator’s audience to understand Higgsfield’s value, try it in a real workflow, and eventually convert into paid usage?**

This means the score is **not** a popularity score.  
It is a **conversion-fit score**.

A creator becomes more valuable for Higgsfield when their audience is likely to:

- care about content quality
- care about speed and workflow
- use content for business, clients, or growth
- notice tools and want to copy workflows
- benefit from Higgsfield-specific features

That is why subscriber count was not treated as the main signal. A large audience can still be weak if it is broad, passive, or mainly watching for entertainment.

---

## Scoring Dimensions

### 1. Audience Fit
This asks:

**Are these viewers actually creators, operators, freelancers, agencies, marketers, or brand builders?**

Signals used:
- creator/business/workflow terms in titles, descriptions, and category matches
- whether the channel clearly serves people making content professionally or semi-professionally

This received high weight because if the audience is wrong, the rest matters much less.

---

### 2. Monetization Relevance
This asks:

**Does this audience already think in ROI terms?**

Signals used:
- monetization
- clients
- creator business
- brand deals
- conversions
- campaign performance
- digital products

This matters because a monetization-aware audience is more likely to pay for software if the upside is clear.

---

### 3. Higgsfield-Specific Fit
This asks:

**Can Higgsfield be shown naturally in this creator’s world?**

This was more specific than general tool relevance.

Signals used:
- cinematic visuals
- creative direction
- character consistency
- visual identity
- moodboards
- shot planning
- transitions
- camera control
- polished branded content

This received high weight because a creator can be generally relevant but still be a weak fit if Higgsfield would feel unnatural in their content.

---

### 4. Demoability
This asks:

**Can this creator make Higgsfield look useful on camera?**

Examples:
- workflow channels can demonstrate process improvement
- filmmaking channels can demonstrate cinematic upgrade
- creator tool educators can demonstrate integration into workflow
- ad creators can demonstrate asset iteration or visual polish

This matters because even a relevant audience will convert poorly if the creator cannot show the product in a concrete and convincing way.

---

### 5. Premium Aesthetic Fit
This asks:

**Does this creator operate in a visual world where better-looking output matters?**

Signals used:
- cinematic
- visual identity
- storytelling
- premium visuals
- commercial quality
- art direction
- aesthetic systems

This matters because Higgsfield is not just a utility tool. Its value is stronger when the audience cares about how content looks, not just how fast it ships.

---

### 6. Engagement Quality
Originally, this dimension was meant to include comment signals because comments often reveal:
- tool-buying intent
- workflow curiosity
- replication intent

In this version, comment analysis was disabled because of API / usage limits, so engagement quality was inferred more lightly from channel type, recent content pattern, and content context.

---

### 7. Posting Frequency
This asks:

**Does this creator publish often enough to feel production pain?**

A creator who posts consistently is more likely to:
- need faster workflows
- care about production leverage
- have an audience that also values efficiency

This mattered, but less than fit and product relevance.

---

## Bonuses and Penalties

This is where the score reflects growth judgment, not just classification.

### Bonuses

#### Mid-Tier Bonus
Mid-sized creators were rewarded because they often offer the best mix of:
- trust
- affordability
- authenticity
- action rate

This is a growth decision, not a data-cleanliness decision.

#### Workflow Bonus
If a creator repeatedly talks about systems, process, production, or behind-the-scenes workflow, that is a strong sign Higgsfield can be positioned as leverage.

#### Feature Match Bonus
If the channel strongly matches Higgsfield-style use cases, it gets boosted because partnership messaging becomes easier and stronger.

#### Creative Direction Bonus
If the creator operates in a world of visuals, campaigns, art direction, or brand storytelling, they get extra credit because Higgsfield is easier to position there.

---

### Penalties

#### Generic AI Novelty Penalty
Creators were penalized if their audience seems interested in AI as novelty rather than as a real production tool.

This matters because AI curiosity does not automatically turn into paid usage.

#### Broad Entertainment Penalty
If the audience is too broad or passive, reach may not convert.

#### Low Visual Relevance Penalty
If visuals are not central to the creator’s value, Higgsfield becomes harder to position.

#### Weak Demoability Penalty
If the creator cannot naturally show the product in action, conversion potential drops.

#### Inflated Subscriber Penalty
Large but stale channels should not outrank smaller but healthier ones.

#### Inactive Penalty
If the creator posts infrequently, they are less likely to feel workflow pain and less likely to drive current influence.

---

## Why the Weights Look Like This

The heavier weights were placed on:
- audience fit
- Higgsfield-specific fit
- demoability
- premium aesthetic fit
- monetization relevance

These are closest to the real growth question:

**Will this creator help Higgsfield get paying users?**

Raw reach was weighted less heavily because reach without fit often produces:
- awareness without action
- trials without activation
- curiosity without payment

That is why this system is more useful than a simple “big channel + AI topic” filter.

---

## What the Score Actually Represents

A high-scoring creator is not just relevant to AI or video.

A high-scoring creator is someone whose audience is:
- commercially valuable
- visually aligned
- likely to understand Higgsfield as a practical production tool
- more likely to convert into real product usage

In other words, the score is best understood as a **partnership-quality proxy**.

> I used the score as a proxy for expected partnership quality. A high-scoring creator was not just relevant to AI or video, but someone whose audience was commercially valuable, visually aligned, and likely to understand Higgsfield as a practical production tool.

---

## Simple Interpretation

A creator might score high if:
- their audience is made up of creators, freelancers, agencies, or operators
- their content is about workflow, branded visuals, or production systems
- they publish consistently
- Higgsfield can be shown through cinematic output, visual systems, or faster production
- their audience is likely to buy tools

A creator might score low if:
- they talk about AI only in a generic way
- their audience is mostly passive
- the content is broad entertainment
- Higgsfield would be hard to demonstrate naturally

So the score is really a **conversion-fit score**, not a popularity score.


## Notes

- If `OPENAI_API_KEY` is missing, the system still runs and uses deterministic fallback commentary.
- Quota-safe defaults are enabled in `config/default_config.yaml` (`include_channel_search: false`, `use_comment_analysis: false`).
- Scoring logic is deterministic and data-first; OpenAI is used only for label/comment support.
- YouTube data is sourced via Apify actors, not the YouTube Data API.

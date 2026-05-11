# catalog_demo

A multi-domain workspace seed for populating the Thyme frontend catalog with
realistic metadata variety while iterating on UI work.

## What's inside

Three loosely-related domains, ~6 featuresets total:

| Domain | Source(s) | Datasets | Pipelines | Featuresets |
|---|---|---|---|---|
| commerce | `transactions_pg` (Postgres) | `Transaction`, `UserTxnStats` | `compute_user_txn_stats` | `UserSpendFeatures`, `UserRiskFeatures` |
| content | `pageviews` (Kinesis), `articles_pg` (Postgres) | `Pageview`, `Article`, `UserEngagementStats` | `compute_user_engagement` | `UserEngagementFeatures`, `UserEngagementFeaturesLegacy` (deprecated) |
| identity | `user_profile_pg` (Postgres) | `UserProfile` | — | `UserProfileFeatures`, `UserCompositeFeatures` (cross-domain) |

Deliberate variety:
- 3 connectors (Postgres ×2, Kinesis ×1)
- 4+ distinct owners (`payments@`, `risk@`, `growth@`, `content-platform@`, `platform@`, `ml-platform@`)
- Tag axes: domain, tier (production/experimental/deprecated), pii
- A deprecated featureset with `replacement` pointer (lineage card showpiece)
- A cross-domain composite featureset depending on all three domains (graph showpiece)

Extractors return constants — this seed is **catalog-focused**, it does not
require the engine to be running. Feature values returned from
`thyme query` will be the stubbed constants.

## Usage

```bash
thyme commit examples/catalog_demo/workspace.py
```

Re-running is idempotent: commit upserts by `(name, version)`. To wipe and
re-seed cleanly (e.g. after renaming entities), use the frontend's
`npm run seed:dev:reset`.

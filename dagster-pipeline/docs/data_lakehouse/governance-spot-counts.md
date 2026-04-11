# Data Lakehouse — Governance conventions

## Spot counts: only non-deleted spots

In the governance pipeline, **user-level spot counts use only non-deleted spots**.

- **`user_spot_count`** in **lk_users** (gold `gold_lk_users_new`) is computed from **stg_s2p_spots_new** filtered by **`deleted_at IS NULL`**. So each user’s count is the number of spots that are not deleted.
- Legacy **lk_users** may use a different source or logic (e.g. including deleted spots or a different view). Discrepancies between legacy and governance for `user_spot_count` are expected when legacy includes deleted spots; governance is stricter and excludes them by design.

This is intentional: governance tables reflect only non-deleted, active data for spot counts.

**References**: `gold_lk_users_new.py` (read of `stg_s2p_spots_new`, filter `deleted_at.is_null()`, aggregation by `user_id`).

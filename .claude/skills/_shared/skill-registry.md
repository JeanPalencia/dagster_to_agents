# Skill Registry — dagster project

Project-scoped skills for Dagster data lakehouse. Auto-loaded by orchestrator and agents based on code context and task type.

---

## Compact Rules

### conventional-commits

**When to use**: Writing or reviewing commit messages, creating or naming branches, reviewing PRs for compliance. Any `git commit` or `git checkout -b` operation.

**Key patterns**:
- **Commit format**: `<type>[scope][!]: <description>` (imperative mood, lowercase type, no period at end)
- **Types**: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`
- **Breaking changes**: append `!` after type/scope OR add `BREAKING CHANGE:` footer
- **Branch format**: `<type>/<optional-scope>/<short-description>` (lowercase, hyphens only)
- **Scope**: optional but recommended (e.g., `feat(auth): add OAuth`, `fix/maintenance/dynamic-storage-dir`)

**Examples**:
- Commit: `feat(amenity-consistency): redirect output to dagster_agent_ prefix for test env`
- Branch: `feat/amenity-consistency/dagster-agent-test-env`
- Breaking: `feat!: replace load_from_defs_folder with explicit asset imports`

**Gotchas**:
- Description MUST be imperative: "add feature" not "added feature"
- No uppercase in branch names (invalid: `Feature/AddOAuth`)
- No underscores in branches (invalid: `fix_login_bug`)
- Breaking changes need `!` OR footer, not both required

**Full documentation**: `.claude/skills/conventional-commits/SKILL.md`

---

### lk-sst

**When to use**: Building ad-hoc metrics, reports, inventories, counts, or analysis from `lk_spot_status_history` (SST enriched event log). Supports SQL output (for Metabase/BIs) and Python/Dagster output (for `rpt_*` pipeline tables).

**Key patterns**:
- `lk_spot_status_history` is a **state-change log**, not a simple event table. Each row = time range with `source_sst_created_at` (start) and `next_source_sst_created_at` (end; NULL = still active)
- Use **temporal overlap** for "how many spots were in state X on date Y" — never pre-expand dates × dimensions cartesian
- JOIN `lk_spots` for dimensions (geography, type, sector, modality, area, prices, user). Default INNER JOIN, LEFT JOIN for hard-deleted spots
- JOIN `lk_spot_status_dictionary` when filtering/grouping by status **name** instead of ID
- Core dimensions (11): `spot_type`, `spot_sector`, `spot_state`, `spot_municipality`, `spot_modality`, `user_affiliation`, `user_industria_role`, `spot_origin`, `spot_status`, `spot_status_reason`, `spot_status_full` (+ `_id` variants)
- Extended dimensions: ~187 columns in `lk_spots` (prices, fine geography, quality, user details, listing metadata)

**Metric patterns**:
1. **Point-in-time count**: `generate_series` + temporal overlap WHERE clause (`source_sst_created_at <= period_date AND (next_source_sst_created_at >= period_date OR NULL)`)
2. **Transition count**: `DATE_TRUNC` + filter `prev_spot_status_id` → `spot_status_id` transitions in period
3. **Duration aggregations**: `next_source_sst_created_at - source_sst_created_at` as `duration_days` + percentiles/avg/max
4. **Funnel analysis**: CTEs stacking multiple state filters + LEFT JOINs to preserve all stages

**Gotchas**:
- Always filter `spot_hard_deleted_id = 0` unless explicitly including deleted spots
- Use both `_id` (bigint) and name (varchar) columns when grouping — BI tools need both
- `next_source_sst_created_at IS NULL` = spot is currently in this state (active)
- Never `SELECT *` from `lk_spots` — explicitly list dimension columns needed

**Full documentation**: `.claude/skills/lk-sst/SKILL.md` (mental model, full patterns, validation scripts)

---

### lk-leads

**Status**: 🚧 Not yet implemented

**Purpose**: Deep context for `lk_leads_new` gold table — lead funnel, L1-L5 hierarchy, business logic, use cases, gotchas.

**When to use**: Working with `lk_leads_new`, modifying lead-related columns, building lead reports, or understanding lead funnel progression.

**Placeholder**: Create `.claude/skills/lk-leads/SKILL.md` when needed.

---

### lk-spots

**Status**: 🚧 Not yet implemented

**Purpose**: Deep context for `lk_spots_new` gold table — property listings master, dimensions, business rules, common joins.

**When to use**: Working with `lk_spots_new`, modifying spot-related columns, building inventory reports.

**Placeholder**: Create `.claude/skills/lk-spots/SKILL.md` when needed.

---

### lk-projects

**Status**: 🚧 Not yet implemented

**Purpose**: Deep context for `lk_projects_new` gold table — project pipeline, stages, business logic.

**When to use**: Working with `lk_projects_new`, modifying project-related columns, building project reports.

**Placeholder**: Create `.claude/skills/lk-projects/SKILL.md` when needed.

---

## User Skills

Skills auto-load based on **code context** (file extensions, paths being modified) AND **task context** (what actions are being performed).

| Context | Skill to load |
| ------- | ------------- |
| Creating commits, naming branches, or reviewing git messages | conventional-commits |
| Building metrics from lk_spot_status_history or SST event log | lk-sst |
| Working with gold table lk_leads_new | lk-leads |
| Working with gold table lk_spots_new | lk-spots |
| Working with gold table lk_projects_new | lk-projects |

---

## Registry Maintenance

- **Add new skill**: Create `.claude/skills/{skill-name}/SKILL.md`, add Compact Rules entry above, add trigger row in User Skills table
- **Update skill**: Edit the skill's SKILL.md + update Compact Rules section here (compact rules = cached summary for orchestrator context efficiency)
- **Naming convention**: Domain-based (`lk-sst`, `lk-leads`) NOT table-exact (`gold-table-lk_leads_new`). Shorter, more natural, allows one skill to cover multiple related tables.
- **Multi-file structure**: `SKILL.md` (main), `table-reference.md` (columns/schema), `examples.md` (SQL/Python snippets), `scripts/` (validation/generation tools)

**Why project-scoped**: These skills only make sense within the dagster project. They must be committable and deployable to other instances (not global `~/.claude/`).

**Skill Resolver Protocol**: Orchestrator reads this registry once per session, caches Compact Rules, injects matching rules into sub-agent prompts. This is compaction-safe because each delegation re-reads if cache is lost.

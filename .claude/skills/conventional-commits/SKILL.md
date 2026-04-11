---
name: conventional-commits
description: >
  Enforces Conventional Commits (conventionalcommits.org) for commit messages
  and Conventional Branch (conventional-branch.github.io) for branch names.
  Trigger: When creating a commit, naming a branch, or reviewing commit/branch names.
license: MIT
metadata:
  author: jeanpalencia
  version: "1.0"
---

## When to Use

- Writing or reviewing a commit message
- Creating or naming a branch
- Reviewing PRs for commit message compliance
- Any `git commit` or `git checkout -b` operation

---

## Commit Message Format

```
<type>[optional scope][optional !]: <description>

[optional body]

[optional footer(s)]
```

### Types

| Type | Use when |
|---|---|
| `feat` | New feature |
| `fix` | Bug fix |
| `docs` | Documentation only |
| `style` | Formatting, missing semicolons — no logic change |
| `refactor` | Code change that is neither a fix nor a feature |
| `perf` | Performance improvement |
| `test` | Adding or fixing tests |
| `build` | Build system, dependencies (uv, pip, Docker) |
| `ci` | CI/CD configuration (GitHub Actions, Railway, CodeBuild) |
| `chore` | Maintenance tasks (cleanup, repo hygiene) |
| `revert` | Reverts a previous commit |

### Rules

1. **type** MUST be lowercase
2. **description** MUST follow the colon + space immediately; no period at end
3. **description** MUST be imperative mood: "add feature" not "added feature"
4. **scope** is optional, in parentheses: `feat(auth): add OAuth`
5. **Breaking changes**: append `!` after type/scope OR add `BREAKING CHANGE: <desc>` footer
6. **Body** begins one blank line after description; explains the *why*, not the *what*
7. **Footers** begin one blank line after body; format: `Token: value` or `Token #value`

### Examples

```
feat(amenity-consistency): redirect output to dagster_agent_ prefix for test env

fix: derive STORAGE_DIR from $DAGSTER_HOME instead of hardcoded EC2 path

chore: add in_process_executor to adc job to prevent OOM on Railway

feat!: replace load_from_defs_folder with explicit asset imports

BREAKING CHANGE: definitions.py no longer auto-loads all flows
```

---

## Branch Name Format

```
<type>/<optional-scope>/<short-description>
```

### Rules

1. All **lowercase**
2. Words separated by **hyphens** (`-`), never underscores or spaces
3. Scope is optional but recommended for clarity
4. Keep descriptions short (2-5 words)
5. No special characters except `/` as separator and `-` within segments

### Types (same as commits)

`feat` · `fix` · `docs` · `style` · `refactor` · `perf` · `test` · `build` · `ci` · `chore` · `hotfix` · `release`

### Examples

```
feat/amenity-consistency/dagster-agent-test-env
fix/maintenance/dynamic-storage-dir
chore/railway/in-process-executor
ci/railway/add-healthcheck
hotfix/auth/expired-sts-credentials
release/v1.2.0
docs/claude-md/add-commit-conventions
```

### Invalid examples (avoid)

```
Feature/AddOAuth          ❌ uppercase, no type prefix
fix_login_bug             ❌ underscores
add-new-feature           ❌ no type
feat/Add_New_Feature      ❌ uppercase + underscores
```

---

## Quick Reference

| Operation | Format |
|---|---|
| New feature | `feat(scope): description` / branch `feat/scope/description` |
| Bug fix | `fix(scope): description` / branch `fix/scope/description` |
| Breaking change | `feat!: description` + `BREAKING CHANGE:` footer |
| Docs update | `docs: update CLAUDE.md with X` |
| Dependency bump | `build: upgrade dagster to 1.12.0` |
| Railway/CI change | `ci: add in_process_executor to reduce memory usage` |

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A GitHub analytics pipeline + dashboard for the jsPsych project. Python code in `src/` fetches PRs, issues, reviews, comments, and releases from the GitHub REST API (hardcoded to `jspsych/jsPsych` in `src/github_client.py`), stores them in SQLite, and computes metrics. `dashboard/` is a Quarto website whose `.qmd` pages run embedded Python against that database.

## Setup

- `pip install -r requirements.txt` (CI uses Python 3.13.6). The Quarto CLI must be installed separately — the `quarto` pip package is just a helper.
- Create a `.env` at the repo root with `GITHUB_TOKEN=...` (no `.env.example` exists). The pipeline raises if it's missing.

## Commands

Run everything from the repo root:

- Full data sync: `python -m src.cli full` (add `--render` to also render the dashboard)
- Incremental sync: `python -m src.cli incremental` — status uncertain; CI only ever runs `full`, so don't assume incremental works without testing it
- Render dashboard: `quarto render dashboard`
- Lint: `ruff check .` (config in `ruff.toml`)
- Tests: `python3 -m pytest tests/ -q` (deterministic, no network; CI runs them on push/PR)
- For dashboard-page changes, also render locally and check the output pages: `source .venv/bin/activate && quarto render dashboard`

## Gotchas

- **Working directory matters.** The `.qmd` files hardcode `db_path="../data/analytics.db"` and insert the repo root into `sys.path` to import `src.*`. Always run `quarto render dashboard` from the repo root; the wrong cwd breaks both the import and the DB path.
- `data/analytics.db` (repo root) is the canonical committed database; all code must reference it explicitly (the `.qmd` pages use `../data/analytics.db`). Never let a `DatabaseManager()` default-path call run with cwd `dashboard/` — it silently creates a second database there.
- `_quarto.yml` sets `output-dir: docs`, but `dashboard/docs/` is gitignored — never commit rendered output. Deployment happens in CI (`.github/workflows/full-sync.yml`): a daily full sync then Quarto publish to the `gh-pages` branch.

## Workflow

- Feature branches PR'd into `main`. The `dev` and `test` branches are historical; `gh-pages` is deploy-only.

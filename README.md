This repo houses the GitHub analytics pipeline and dashboard for jsPsych. It fetches PRs, issues, reviews, comments, and releases from the GitHub REST API, stores them in SQLite, computes basic metrics, and publishes a static dashboard via GitHub Actions.

- Data sync logic lives in [`src/data_pipeline.py`](src/data_pipeline.py), database access in [`src/database.py`](src/database.py), and schema/helpers in [`src/models.py`](src/models.py).
- Dashboard content is authored in Quarto (`.qmd`) in [`dashboard/`](dashboard/), and published to GitHub Pages by a workflow in [`.github/workflows/full-sync.yml`](.github/workflows/full-sync.yml).
- The local SQLite database is stored at [`data/analytics.db`](data/analytics.db).

### Project structure

```txt
.
├─ README.md
├─ requirements.txt
├─ .github/
│  └─ workflows/
├─ dashboard/
│  ├─ assets/
│  │  └─ styles/
├─ data/
├─ src/
```

- `dashboard/`: Quarto project with `.qmd` sources.
- `data/`: Contains the SQLite DB (`analytics.db`) created at runtime.
- `src/`: Python package with data pipeline, DB access, and GitHub client.
- `.github/workflows/`: CI to run the full sync and publish the site.



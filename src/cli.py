import argparse
import logging
import subprocess
import sys

from .data_pipeline import GitHubDataPipeline
from .repos import repo_names

logger = logging.getLogger(__name__)


def render_quarto(dashboard_dir: str) -> int:
    try:
        result = subprocess.run(["quarto", "render", dashboard_dir], check=True)
        return result.returncode
    except FileNotFoundError:
        print("Error: `quarto` CLI not found on PATH.")
        return 1
    except subprocess.CalledProcessError as e:
        print(f"Quarto render failed with exit code {e.returncode}")
        return e.returncode

def run(mode: str, db_path: str, render: bool, dashboard_path: str) -> int:
    if mode not in ("full", "incremental"):
        print("Unsupported mode. Use 'full' or 'incremental'.")
        return 2

    repos = repo_names()
    failures = 0
    for repo in repos:
        logger.info(f"===== Syncing {repo} ({mode}) =====")
        try:
            pipeline = GitHubDataPipeline(repo=repo, db_path=db_path)
            if mode == "full":
                pipeline.sync_all_data()
            else:
                pipeline.sync_incremental()
        except Exception:
            failures += 1
            logger.error(f"Failed to sync {repo}; continuing with remaining repos", exc_info=True)

    if failures:
        logger.error(f"{failures} of {len(repos)} repositories failed to sync")

    if render:
        return render_quarto(dashboard_path)

    return 0


def main():
    parser = argparse.ArgumentParser(description="Sync GitHub data. Optionally render Quarto locally.")
    parser.add_argument("mode", choices=["full", "incremental"], help="Sync mode")
    parser.add_argument("--db", dest="db_path", default="data/analytics.db", help="Path to SQLite DB")
    parser.add_argument("--render", action="store_true", help="Render Quarto dashboard after sync (local usage)")
    parser.add_argument("--dashboard-path", dest="dashboard_path", default="dashboard", help="Path to Quarto project directory")
    args = parser.parse_args()

    sys.exit(run(args.mode, db_path=args.db_path, render=args.render, dashboard_path=args.dashboard_path))


if __name__ == "__main__":
    main()




import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime

import pandas as pd

from .data_pipeline import GitHubDataPipeline
from .evaluation import (
    DEFINITIONS,
    GOAL_HEADLINE_COLUMN,
    GOAL_STATEMENTS,
    current_vs_previous,
    goal_series,
)
from .repos import repo_names

logger = logging.getLogger(__name__)

# Human-friendly section titles for the sustainability report.
_GOAL_TITLES = {
    'goal1_core_team_size': 'Goal 1: Contributors with merge access',
    'goal2_community_merge_time': 'Goal 2: Community PR review & merge time',
    'goal3_support_response': 'Goal 3: Support responsiveness & participation',
    'goal4_community_contributions': 'Goal 4: Community-repository contributions',
    'goal5_new_contributors': 'Goal 5: New-contributor growth (core packages)',
}


def _format_cell(value) -> str:
    """Render a single table cell for the markdown report."""
    if value is None or (pd.api.types.is_scalar(value) and pd.isna(value)):
        return '—'
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.strftime('%Y-%m-%d')
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f'{value:.2f}'
    return str(value)


def _df_to_markdown(df: pd.DataFrame) -> str:
    """Render a DataFrame as a compact GitHub-flavored markdown table."""
    headers = list(df.columns)
    lines = ['| ' + ' | '.join(headers) + ' |',
             '| ' + ' | '.join(['---'] * len(headers)) + ' |']
    for _, row in df.iterrows():
        lines.append('| ' + ' | '.join(_format_cell(row[c]) for c in headers) + ' |')
    return '\n'.join(lines)


def _format_change(comparison: dict) -> str:
    """Render a one-line current-vs-previous comparison."""
    current = comparison['current']
    previous = comparison['previous']
    change = comparison['change_pct']
    cur_s = '—' if current is None else f'{current:.2f}'.rstrip('0').rstrip('.')
    prev_s = '—' if previous is None else f'{previous:.2f}'.rstrip('0').rstrip('.')
    if change is None:
        change_s = 'n/a'
    else:
        change_s = f'{change:+.1f}%'
    return (f'Current window: **{cur_s}** vs. 6 months earlier: **{prev_s}** '
            f'(change: {change_s})')


def _summary_table(series: dict) -> str:
    """Render the cross-goal summary table shown at the top of the report.

    One row per goal giving the headline metric's current-window value, the
    non-overlapping 6-months-earlier value, and the percent change, so the
    board can prioritize by relative improvement at a glance.
    """
    rows = []
    for goal_id, df in series.items():
        headline = GOAL_HEADLINE_COLUMN.get(goal_id)
        comparison = current_vs_previous(df, headline) if headline else None
        title = _GOAL_TITLES.get(goal_id, goal_id)
        if comparison is None:
            rows.append({'Goal': title, 'Headline metric': headline or '—',
                         'Current': '—', 'Previous (6 mo ago)': '—', 'Change': '—'})
            continue
        change = comparison['change_pct']
        rows.append({
            'Goal': title,
            'Headline metric': f'`{headline}`',
            'Current': _format_cell(comparison['current']),
            'Previous (6 mo ago)': _format_cell(comparison['previous']),
            'Change': 'n/a' if change is None else f'{change:+.1f}%',
        })
    summary_df = pd.DataFrame(
        rows, columns=['Goal', 'Headline metric', 'Current',
                       'Previous (6 mo ago)', 'Change'])
    return '## Summary\n\n' + _df_to_markdown(summary_df)


def generate_report(db_path: str, output_dir: str) -> str:
    """
    Compute the goal series and write a dated sustainability report.

    Args:
        db_path (str): Path to the SQLite database.
        output_dir (str): Directory to write the report into (created if
                          missing).

    Returns:
        str: Path to the written markdown report.
    """
    series = goal_series(db_path=db_path)
    now = datetime.now()
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, f'sustainability-report-{now:%Y-%m-%d}.md')

    parts = [
        '# jsPsych Sustainability Report',
        '',
        f'Generated: {now:%Y-%m-%d}',
        '',
        ('Rolling 6-month windows (182 days) stepped quarterly across project '
         'history. Each table shows the most recent 8 windows.'),
        '',
        _summary_table(series),
        '',
    ]

    for goal_id, df in series.items():
        title = _GOAL_TITLES.get(goal_id, goal_id)
        statement = GOAL_STATEMENTS.get(goal_id, '')
        definition = DEFINITIONS.get(goal_id, '')
        headline = GOAL_HEADLINE_COLUMN.get(goal_id)
        comparison = current_vs_previous(df, headline) if headline else None

        parts.append(f'## {title}')
        parts.append('')
        parts.append(f'_{statement}_')
        parts.append('')
        if definition:
            parts.append(f'**How this is measured.** {definition}')
            parts.append('')
        if comparison is not None:
            parts.append(f'Headline metric (`{headline}`): {_format_change(comparison)}')
            parts.append('')
        parts.append(_df_to_markdown(df.tail(8)))
        parts.append('')

    with open(out_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(parts) + '\n')

    return out_path


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
    parser = argparse.ArgumentParser(
        description="Sync GitHub data, render Quarto, or write a sustainability report.")
    parser.add_argument("mode", choices=["full", "incremental", "report"],
                        help="'full'/'incremental' sync data; 'report' writes a sustainability report")
    parser.add_argument("--db", dest="db_path", default="data/analytics.db", help="Path to SQLite DB")
    parser.add_argument("--render", action="store_true", help="Render Quarto dashboard after sync (local usage)")
    parser.add_argument("--dashboard-path", dest="dashboard_path", default="dashboard", help="Path to Quarto project directory")
    parser.add_argument("--output", dest="output_dir", default="reports/",
                        help="Output directory for the 'report' command")
    args = parser.parse_args()

    if args.mode == "report":
        out_path = generate_report(db_path=args.db_path, output_dir=args.output_dir)
        print(f"Wrote sustainability report to {out_path}")
        sys.exit(0)

    sys.exit(run(args.mode, db_path=args.db_path, render=args.render, dashboard_path=args.dashboard_path))


if __name__ == "__main__":
    main()




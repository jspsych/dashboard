"""chartkit — the single chart vocabulary for the sustainability dashboard.

This module is the CHART SPEC OF RECORD. Every chart on the dashboard is built
through it so that a returning reader navigates on habit: one palette, one tick
discipline, one way of showing low-confidence data, one way of showing an empty
state, and one way of asking the reader a question (the chart title *is* the
question the chart answers).

The API is deliberately small and boring. The goal is consistency, not
flexibility: if you find yourself wanting a new option, prefer changing the
kit for everyone over adding a per-call knob.

Design commitments (from .impeccable.md):
  * Colorblind-safe: the Okabe–Ito palette, and meaning is NEVER carried by
    hue alone — every series also gets a distinct line style and marker shape,
    and (for <=3 series) a direct end-of-line label instead of a legend.
  * Honest axes: counts start at 0 and use integer ticks; gaps stay visible;
    low-confidence windows (too few samples behind a median) are drawn as
    hollow gray markers with the connecting line broken, not silently smoothed
    over or dropped.
  * Utilitarian restraint: light grid, no chart junk, readable type (>=11pt).

Pure matplotlib — no seaborn dependency. Works headless under the Agg backend.
"""

from typing import List, Optional, Tuple

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.ticker import FuncFormatter, MaxNLocator

# Re-exported from its canonical home in evaluation.py so chart code and
# evaluation code agree on the one date. See evaluation.DISCUSSIONS_LAUNCH for
# the documented definition (GitHub Discussions launch; migrated older threads
# may carry earlier dates).
from .evaluation import DISCUSSIONS_LAUNCH

__all__ = [
    'BLUE', 'VERMILLION', 'BLUISH_GREEN', 'ORANGE', 'REDDISH_PURPLE',
    'SKY_BLUE', 'MUTED_GRAY', 'PALETTE', 'LINE_STYLES', 'MARKERS',
    'DISCUSSIONS_LAUNCH',
    'apply_style', 'timeseries', 'bars', 'empty_state', 'question_figure',
]

# --- Okabe–Ito colorblind-safe palette -------------------------------------
# These eight hues stay distinguishable under the common forms of color-vision
# deficiency. Order matters: series are assigned colors from PALETTE in order.
BLUE = '#0072B2'
VERMILLION = '#D55E00'
BLUISH_GREEN = '#009E73'
ORANGE = '#E69F00'
REDDISH_PURPLE = '#CC79A7'
SKY_BLUE = '#56B4E9'
# Reserved for de-emphasis (low-confidence points, empty-state text). NOT part
# of the categorical cycle, so it never collides with a real series color.
MUTED_GRAY = '#999999'

# The ordered categorical cycle. Gray is deliberately excluded.
PALETTE = [BLUE, VERMILLION, BLUISH_GREEN, ORANGE, REDDISH_PURPLE, SKY_BLUE]

# Redundant encodings cycled ALONGSIDE color so meaning survives grayscale /
# color blindness. Index i of a series uses PALETTE[i], LINE_STYLES[i], and
# MARKERS[i] (each modulo its length).
LINE_STYLES = ['-', '--', '-.']
MARKERS = ['o', 's', '^']


def apply_style() -> None:
    """Set global matplotlib rcParams to the dashboard house style.

    Idempotent; safe to call before every figure. Produces a light,
    whitegrid-like look with no top/right spines, a faint horizontal grid,
    readable (>=11pt) type, and the Okabe–Ito categorical color cycle. Call it
    once per Quarto cell (``question_figure`` calls it for you).
    """
    plt.rcParams.update({
        # Surfaces: white, for the light public-reporting context.
        'figure.facecolor': 'white',
        'axes.facecolor': 'white',
        'savefig.facecolor': 'white',
        # Frame: keep only the left/bottom spines, drawn lightly.
        'axes.edgecolor': '#cccccc',
        'axes.linewidth': 0.8,
        'axes.spines.top': False,
        'axes.spines.right': False,
        # Grid: faint horizontal reference lines only; no chart junk.
        'axes.grid': True,
        'axes.grid.axis': 'y',
        'grid.color': '#e6e6e6',
        'grid.linewidth': 0.6,
        'grid.linestyle': '-',
        # Type: readable, hierarchy from title -> labels -> ticks.
        'font.size': 11,
        'axes.titlesize': 14,
        # 'bold' is available in every matplotlib font (DejaVu Sans has no
        # named 'semibold'/'medium' faces); at 14pt left-aligned it reads as a
        # firm heading, not shouty.
        'axes.titleweight': 'bold',
        'axes.titlelocation': 'left',
        'axes.labelsize': 12,
        'xtick.labelsize': 11,
        'ytick.labelsize': 11,
        'legend.fontsize': 11,
        'legend.frameon': False,
        # Ticks: unobtrusive.
        'xtick.color': '#444444',
        'ytick.color': '#444444',
        'axes.labelcolor': '#222222',
        'text.color': '#222222',
        # Categorical color cycle.
        'axes.prop_cycle': plt.cycler(color=PALETTE),
    })


def _quarter_formatter() -> FuncFormatter:
    """Formatter that labels a matplotlib date tick as ``YYYY Qn``."""
    def _fmt(x, _pos=None):
        d = mdates.num2date(x)
        quarter = (d.month - 1) // 3 + 1
        return f'{d.year} Q{quarter}'
    return FuncFormatter(_fmt)


def _apply_date_ticks(ax: Axes, x_dt: pd.Series) -> None:
    """Choose year vs. quarter ticks by the x span, per the tick discipline.

    Series wider than ~3 years get year-only ticks (otherwise the axis becomes
    a wall of rotated quarter labels); narrower series get quarter ticks at
    Jan/Apr/Jul/Oct, which stays at a readable density.
    """
    valid = x_dt.dropna()
    if valid.empty:
        return
    span_days = (valid.max() - valid.min()).days
    if span_days > 3 * 365:
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    else:
        ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(1, 4, 7, 10)))
        ax.xaxis.set_major_formatter(_quarter_formatter())


def _quarter_labels(values: pd.Series) -> List[str]:
    """Render a series of period timestamps as ``YYYY Qn`` strings.

    Falls back to ``str`` for anything that isn't datetime-like, so bar charts
    over non-date categories still work.
    """
    dt = pd.to_datetime(values, errors='coerce', utc=True)
    labels = []
    for original, d in zip(values, dt):
        if pd.isna(d):
            labels.append(str(original))
        else:
            labels.append(f'{d.year} Q{(d.month - 1) // 3 + 1}')
    return labels


def timeseries(ax: Axes, df: pd.DataFrame, x: str, series: List[dict],
               ylabel: str, integer_y: bool = False,
               min_n: Optional[int] = None) -> Axes:
    """Plot one or more metric series over time with the house discipline.

    Each series is drawn with a distinct color AND a distinct line style AND a
    distinct marker, so it stays readable without color. When a series carries
    a sample-size column and ``min_n`` is set, windows with too few samples are
    rendered as hollow gray markers with the connecting line broken through
    them — visibly low-confidence rather than invisible or over-confident. With
    <=3 series the lines are labeled directly at their right end instead of in
    a legend box.

    Args:
        ax (Axes): The axes to draw on (e.g. from ``question_figure``).
        df (pd.DataFrame): The data. ``x`` and each series ``col`` are columns.
        x (str): Name of the x column (typically a datetime ``window_end`` /
            ``period``).
        series (List[dict]): One dict per line, with keys:
            ``col`` (required) — the y column;
            ``label`` (required) — the human label;
            ``n_col`` (optional) — a per-row sample-size column used with
            ``min_n`` to flag low-confidence points.
        ylabel (str): The y-axis label.
        integer_y (bool): If True, force the y axis to start at 0 and use
            integer ticks (for count metrics; also fixes an integer metric
            being drawn on a spuriously fractional axis).
        min_n (Optional[int]): Minimum sample size for a point to be drawn as a
            confident, connected marker. Points below it (per a series'
            ``n_col``) become hollow gray markers with the line broken. Ignored
            for series without an ``n_col``.

    Returns:
        Axes: The same axes, for chaining.
    """
    if df is None or df.empty:
        return empty_state(ax, 'No data available for this period yet.')

    x_dt = pd.to_datetime(df[x], errors='coerce', utc=True)
    # Matplotlib plots datetimes fine; keep the raw column for positioning.
    x_vals = df[x]

    for i, spec in enumerate(series):
        col = spec['col']
        label = spec['label']
        color = PALETTE[i % len(PALETTE)]
        linestyle = LINE_STYLES[i % len(LINE_STYLES)]
        marker = MARKERS[i % len(MARKERS)]

        y = pd.to_numeric(df[col], errors='coerce')

        n_col = spec.get('n_col')
        if n_col and min_n is not None and n_col in df.columns:
            n = pd.to_numeric(df[n_col], errors='coerce').fillna(0)
            low = n < min_n
        else:
            low = pd.Series(False, index=df.index)

        # Confident line: NaN-out low-confidence points so the line BREAKS
        # through them (a low-n window shouldn't visually connect its
        # neighbors).
        y_line = y.where(~low)
        ax.plot(x_vals, y_line, color=color, linestyle=linestyle,
                marker=marker, markersize=5, linewidth=2, label=label,
                zorder=3)

        # Low-confidence points: hollow gray markers, no connecting line.
        if bool(low.any()):
            ax.plot(np.asarray(x_vals)[low.to_numpy()], y[low],
                    linestyle='none', marker=marker, markersize=5,
                    markerfacecolor='none', markeredgecolor=MUTED_GRAY,
                    markeredgewidth=1.2, zorder=2)

    ax.set_ylabel(ylabel)
    _apply_date_ticks(ax, x_dt)

    if integer_y:
        ax.set_ylim(bottom=0)
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))

    # Direct end-of-line labels (no legend) when the chart is simple enough to
    # read them; otherwise fall back to a frameless legend.
    if len(series) <= 3:
        _direct_labels(ax, df, x_vals, series)
    else:
        ax.legend(loc='best')

    return ax


def _direct_labels(ax: Axes, df: pd.DataFrame, x_vals, series: List[dict]) -> None:
    """Label each series in its own color at its last valid data point.

    Extends the right x-limit slightly so the labels have room, replacing the
    legend box the ``.impeccable.md`` guidance asks us to avoid.
    """
    ends = []
    for i, spec in enumerate(series):
        y = pd.to_numeric(df[spec['col']], errors='coerce')
        valid = y.dropna()
        if valid.empty:
            continue
        last_idx = valid.index[-1]
        pos = list(df.index).index(last_idx)
        ends.append({
            'x': np.asarray(x_vals)[pos],
            'y': float(valid.iloc[-1]),
            'label': spec['label'],
            'color': PALETTE[i % len(PALETTE)],
        })

    if not ends:
        return

    # Dodge vertically: series often end at similar values, and overlapping
    # labels are worse than a legend. Enforce a minimum gap (in y-data units)
    # between label anchors, pushing upward from the lowest label.
    y0, y1 = ax.get_ylim()
    min_gap = (y1 - y0) * 0.06
    ends.sort(key=lambda e: e['y'])
    label_y = []
    for e in ends:
        target = e['y']
        if label_y and target < label_y[-1] + min_gap:
            target = label_y[-1] + min_gap
        label_y.append(target)

    # Work in the axis's own (float) coordinates so date and numeric x-axes
    # behave identically; all labels sit in the right margin at a fixed x.
    x0, x1 = ax.get_xlim()
    ax.set_xlim(x0, x1 + (x1 - x0) * 0.14)
    label_x = x1 + (x1 - x0) * 0.015
    for e, ly in zip(ends, label_y):
        ax.text(
            label_x, ly, e['label'],
            va='center', ha='left',
            color=e['color'], fontsize=11, fontweight='bold',
            clip_on=False,
        )


def bars(ax: Axes, df: pd.DataFrame, x: str, series: List[dict],
         ylabel: str, stacked: bool = True, integer_y: bool = True) -> Axes:
    """Draw a quarterly bar / stacked-bar chart with the house discipline.

    Same palette and tick discipline as ``timeseries``: bars are placed at
    evenly spaced positions and labeled with ``YYYY Qn`` quarter labels thinned
    to at most ~12 labeled ticks (so 44 quarters don't produce 44 rotated
    labels). Counts start at 0 with integer ticks by default.

    Args:
        ax (Axes): The axes to draw on.
        df (pd.DataFrame): The data (one row per period).
        x (str): Name of the period column (datetime or category).
        series (List[dict]): One dict per category, keys ``col`` and ``label``.
            Stacked from the bottom in list order (or grouped side by side when
            ``stacked`` is False).
        ylabel (str): The y-axis label.
        stacked (bool): Stack the series (default) or place them side by side.
        integer_y (bool): Force y to start at 0 with integer ticks (default;
            appropriate for the count-valued bar charts).

    Returns:
        Axes: The same axes, for chaining.
    """
    if df is None or df.empty:
        return empty_state(ax, 'No data available for this period yet.')

    n = len(df)
    positions = np.arange(n)
    labels = _quarter_labels(df[x])

    if stacked:
        bottom = np.zeros(n)
        for i, spec in enumerate(series):
            vals = pd.to_numeric(df[spec['col']], errors='coerce').fillna(0).to_numpy()
            ax.bar(positions, vals, bottom=bottom, width=0.82,
                   color=PALETTE[i % len(PALETTE)], label=spec['label'],
                   edgecolor='white', linewidth=0.5)
            bottom = bottom + vals
    else:
        group_width = 0.82
        bar_width = group_width / max(len(series), 1)
        for i, spec in enumerate(series):
            vals = pd.to_numeric(df[spec['col']], errors='coerce').fillna(0).to_numpy()
            offset = -group_width / 2 + bar_width * (i + 0.5)
            ax.bar(positions + offset, vals, width=bar_width,
                   color=PALETTE[i % len(PALETTE)], label=spec['label'])

    # Thin the labeled ticks to at most ~12 so the axis stays readable.
    step = max(1, int(np.ceil(n / 12)))
    tick_positions = positions[::step]
    tick_labels = [labels[i] for i in range(0, n, step)]
    ax.set_xticks(tick_positions)
    ax.set_xticklabels(tick_labels)

    ax.set_ylabel(ylabel)
    if integer_y:
        ax.set_ylim(bottom=0)
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))

    if len(series) > 1:
        ax.legend(loc='upper left', ncol=min(len(series), 3))

    return ax


def empty_state(ax: Axes, reason: str) -> Axes:
    """Render a calm, muted empty-state message on an otherwise blank axes.

    Muted gray italic text — NOT alarming red. An empty window is normal (a
    feature that hadn't launched yet, a period before data exists), not an
    error, so it should read as an explanation, e.g.
    ``"No discussions before May 2020 — the feature launched then."``

    Args:
        ax (Axes): The axes to blank out and annotate.
        reason (str): The human explanation for why there's nothing to show.

    Returns:
        Axes: The same axes, for chaining.
    """
    ax.text(0.5, 0.5, reason, transform=ax.transAxes,
            ha='center', va='center', style='italic',
            color=MUTED_GRAY, fontsize=12, wrap=True)
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.grid(False)
    return ax


def question_figure(question: str,
                    figsize: Tuple[float, float] = (12, 6)) -> Tuple[Figure, Axes]:
    """Create a styled figure whose title IS the question the chart answers.

    This is the caption-as-question device: instead of a terse noun-phrase
    title ("Merge time"), the chart is titled with the question a reader came
    to answer ("Are community PRs merging faster than they used to?"). The
    title is left-aligned at a readable, non-shouty weight. ``apply_style`` is
    called for you so every figure is consistent.

    Args:
        question (str): The question to set as the (left-aligned) title.
        figsize (Tuple[float, float]): Figure size in inches (default 12x6,
            matching the dashboard's existing card charts).

    Returns:
        Tuple[Figure, Axes]: The new figure and its single axes.
    """
    apply_style()
    fig, ax = plt.subplots(figsize=figsize)
    ax.set_title(question, loc='left', pad=12)
    return fig, ax

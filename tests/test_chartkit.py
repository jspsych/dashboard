"""chartkit tests.

Two kinds: pure-value assertions (palette constants, the re-exported launch
date) and headless smoke tests that each drawing function runs under the Agg
backend on synthetic DataFrames and produces a figure with content. Rendering
correctness (pixels) is out of scope; we assert the functions don't raise and
leave the axes populated.
"""

import matplotlib

matplotlib.use("Agg")  # headless; must precede pyplot import

import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
import pytest  # noqa: E402

from src import chartkit  # noqa: E402


@pytest.fixture(autouse=True)
def _close_figures():
    yield
    plt.close("all")


# --- pure values -----------------------------------------------------------

def test_palette_constants_are_okabe_ito():
    assert chartkit.BLUE == "#0072B2"
    assert chartkit.VERMILLION == "#D55E00"
    assert chartkit.BLUISH_GREEN == "#009E73"
    assert chartkit.ORANGE == "#E69F00"
    assert chartkit.REDDISH_PURPLE == "#CC79A7"
    assert chartkit.SKY_BLUE == "#56B4E9"
    assert chartkit.MUTED_GRAY == "#999999"


def test_palette_is_ordered_and_excludes_gray():
    assert chartkit.PALETTE == [
        "#0072B2", "#D55E00", "#009E73", "#E69F00", "#CC79A7", "#56B4E9"
    ]
    assert chartkit.MUTED_GRAY not in chartkit.PALETTE


def test_redundant_encodings_present():
    # Meaning must never ride on hue alone: line styles + markers cycle too.
    assert chartkit.LINE_STYLES == ["-", "--", "-."]
    assert chartkit.MARKERS == ["o", "s", "^"]


def test_discussions_launch_reexport_matches_evaluation():
    from src import evaluation
    assert chartkit.DISCUSSIONS_LAUNCH == "2020-05-01"
    assert chartkit.DISCUSSIONS_LAUNCH == evaluation.DISCUSSIONS_LAUNCH


# --- smoke tests -----------------------------------------------------------

def _timeseries_df():
    return pd.DataFrame({
        "period": pd.date_range("2021-01-01", periods=8, freq="QE"),
        "community": [30.0, 25.0, 18.0, 12.0, 280.0, 9.0, 8.0, 7.0],
        "core": [5.0, 4.0, 4.5, 4.0, 3.5, 3.0, 3.2, 3.0],
        "community_n": [10, 8, 12, 9, 1, 11, 7, 6],  # the spike rests on n=1
        "core_n": [20, 18, 15, 17, 16, 19, 14, 12],
    })


def test_apply_style_sets_cycle():
    chartkit.apply_style()
    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    assert colors == chartkit.PALETTE


def test_question_figure_returns_titled_axes():
    fig, ax = chartkit.question_figure("Are community PRs merging faster?")
    assert fig is not None
    # Caption-as-question device: the title is set left-aligned, so it lives in
    # matplotlib's left-located title slot (the centered one stays empty).
    assert ax.get_title(loc="left") == "Are community PRs merging faster?"
    assert ax.get_title(loc="center") == ""


def test_timeseries_runs_headless_with_min_n():
    fig, ax = chartkit.question_figure("Are community PRs merging faster?")
    chartkit.timeseries(
        ax, _timeseries_df(), x="period",
        series=[
            {"col": "community", "label": "Community", "n_col": "community_n"},
            {"col": "core", "label": "Core team", "n_col": "core_n"},
        ],
        ylabel="Median days to merge", min_n=5,
    )
    # A figure with drawn lines exists; <=3 series -> direct labels, no legend.
    assert len(ax.lines) > 0
    assert ax.get_legend() is None
    assert fig.get_axes()


def test_timeseries_integer_y_starts_at_zero():
    df = pd.DataFrame({
        "period": pd.date_range("2021-01-01", periods=6, freq="QE"),
        "value": [3, 3, 4, 4, 5, 6],
    })
    fig, ax = chartkit.question_figure("How many people can merge?")
    chartkit.timeseries(ax, df, x="period",
                        series=[{"col": "value", "label": "Core team size"}],
                        ylabel="Contributors", integer_y=True)
    assert ax.get_ylim()[0] == 0


def test_timeseries_empty_df_shows_empty_state():
    fig, ax = chartkit.question_figure("Anything here?")
    chartkit.timeseries(ax, pd.DataFrame(), x="period",
                        series=[{"col": "v", "label": "V"}], ylabel="y")
    texts = [t.get_text() for t in ax.texts]
    assert any("No data" in t for t in texts)


def test_bars_runs_headless_stacked():
    df = pd.DataFrame({
        "period": pd.date_range("2021-01-01", periods=10, freq="QE"),
        "qa": [3, 5, 4, 6, 7, 5, 8, 6, 9, 7],
        "general": [1, 2, 2, 1, 3, 2, 1, 4, 2, 3],
    })
    fig, ax = chartkit.question_figure("What are people asking about?")
    chartkit.bars(ax, df, x="period",
                  series=[{"col": "qa", "label": "Q&A"},
                          {"col": "general", "label": "General"}],
                  ylabel="Discussions opened", stacked=True)
    assert len(ax.patches) > 0            # bars drawn
    assert ax.get_ylim()[0] == 0          # counts start at 0
    # At most ~12 labeled ticks even with many quarters.
    assert len(ax.get_xticks()) <= 12


def test_bars_empty_df_shows_empty_state():
    fig, ax = chartkit.question_figure("Anything here?")
    chartkit.bars(ax, pd.DataFrame(), x="period",
                  series=[{"col": "v", "label": "V"}], ylabel="y")
    texts = [t.get_text() for t in ax.texts]
    assert any("No data" in t for t in texts)


def test_empty_state_is_muted_not_red():
    fig, ax = chartkit.question_figure("Nothing yet")
    chartkit.empty_state(
        ax, "No discussions before May 2020 — the feature launched then.")
    assert len(ax.texts) == 1
    text = ax.texts[0]
    assert text.get_color() == chartkit.MUTED_GRAY
    assert text.get_style() == "italic"
    # Blanked axes: no ticks, no visible spines.
    assert list(ax.get_xticks()) == []
    assert all(not s.get_visible() for s in ax.spines.values())

import matplotlib
matplotlib.use("Agg")

from pipeline.figures import fig06_suppression as fig06


def test_plot_suppression_bars_ordered():
    rows = [
        {"scenario": "conv", "suppression_median_pct": 53},
        {"scenario": "idle", "suppression_median_pct": 91},
        {"scenario": "walk", "suppression_median_pct": 63},
        {"scenario": "churn", "suppression_median_pct": 62},
    ]
    fig = fig06.plot(rows)
    ax = fig.axes[0]
    labels = [t.get_text() for t in ax.get_xticklabels()]
    assert labels == ["idle", "walk", "churn", "conv"]
    heights = [bar.get_height() for bar in ax.patches]
    assert heights == [91, 63, 62, 53]

import logging

import matplotlib.pyplot as plt
import pandas as pd

_logger = logging.getLogger("chart")


def chart_network_latency():
    df = pd.read_csv("metrics/network_latency_experiment.csv")
    columns = ["read_latency", "write_latency"]
    max_y = max([max(df[c]) for c in columns])

    df_no_lease = df[df["leases_enabled"] == False]
    df_lease = df[df["leases_enabled"] == True]
    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, sharey=True)
    ax1.set(title="Latency of linearizable writes and reads (no leases)",
            ylabel="microseconds")
    ax2.set(title="Latency of linearizable writes and reads (with leases)",
            xlabel="one-way network latency (μs)",
            ylabel="microseconds")

    barwidth = 30
    linewidth = 2

    for df, ax in [(df_no_lease, ax1),
                   (df_lease, ax2)]:
        ax.set_ylim(-0.05 * max_y, 1.05 * max_y)
        # Remove borders
        for spine in ax.spines.values():
            spine.set_visible(False)
        for i, column in enumerate(columns):
            is_zeros = (df[column] == 0).all()
            rects = ax.bar(
                df_lease["one_way_latency_mean"] + i * (barwidth + linewidth * 2),
                df[column],
                barwidth,
                label=column,
                color=f"C{i}",
                edgecolor=f"C{i}",
                linewidth=linewidth)

            if is_zeros:
                ax.bar_label(rects, padding=3)

    ax1.legend(loc="center right", framealpha=1, fancybox=False)
    ax2.legend(loc="center right", framealpha=1, fancybox=False)
    plt.tight_layout()
    chart_path = "metrics/network_latency_experiment.pdf"
    fig.savefig(chart_path)
    _logger.info(f"Created {chart_path}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    chart_network_latency()

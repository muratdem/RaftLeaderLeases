import logging

import matplotlib.pyplot as plt
import pandas as pd

_logger = logging.getLogger("chart")

BARWIDTH = 10
LINEWIDTH = 2


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

    for df, ax in [(df_no_lease, ax1),
                   (df_lease, ax2)]:
        ax.set_ylim(-0.05 * max_y, 1.05 * max_y)
        # Remove borders
        for spine in ax.spines.values():
            spine.set_visible(False)
        for i, column in enumerate(columns):
            is_zeros = (df[column] == 0).all()
            rects = ax.bar(
                df_lease["one_way_latency_mean"] + i * (BARWIDTH + LINEWIDTH * 2),
                df[column],
                BARWIDTH,
                label=column,
                color=f"C{i}",
                edgecolor=f"C{i}",
                linewidth=LINEWIDTH)

            if is_zeros:
                ax.bar_label(rects, padding=3)

    ax1.legend(loc="center right", framealpha=1, fancybox=False)
    ax2.legend(loc="center right", framealpha=1, fancybox=False)
    plt.tight_layout()
    chart_path = "metrics/network_latency_experiment.pdf"
    fig.savefig(chart_path)
    _logger.info(f"Created {chart_path}")


def chart_unavailability():
    df_csv = pd.read_csv("metrics/unavailability_experiment.csv")

    def resample_data(df_micros):
        # Maybe I'm debugging unavailability_experiment.py didn't generate all the data.
        if len(df_micros) == 0:
            _logger.warning("No data")
            return df_micros

        df_micros["timestamp"] = pd.to_datetime(df_micros["execution_ts"], unit="us")
        resampled = df_micros.resample("10ms", on="timestamp").sum()
        return resampled[["reads", "writes"]]

    df_no_lease = resample_data(
        df_csv[df_csv["leases_enabled"] == False].copy())
    df_unoptimized = resample_data(
        df_csv[(df_csv["leases_enabled"] == True)
               & (df_csv["read_lease_optimization_enabled"] == False)].copy())
    df_optimized = resample_data(
        df_csv[df_csv["read_lease_optimization_enabled"] == True].copy())

    columns = ["reads", "writes"]
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, sharex=True, sharey=True)
    dfs_and_axes = [(df_no_lease, ax1), (df_unoptimized, ax2), (df_optimized, ax3)]

    ax1.set(title="Throughput without leases",
            ylabel="Ops/sec")
    ax2.set(title="Throughput without read lease optimization",
            ylabel="Ops/sec")
    ax3.set(title="Throughput with read lease optimization",
            ylabel="Ops/sec",
            xlabel="Milliseconds")

    for df, ax in dfs_and_axes:
        # Remove borders
        for spine in ax.spines.values():
            spine.set_visible(False)

        if len(df) == 0:
            continue

        for i, column in enumerate(columns):
            ax.plot((df.index - df.index.min()).total_seconds() * 1000,
                    df[column],
                    label=column)

    ax2.legend(loc="center", framealpha=1, fancybox=False)
    fig.tight_layout()
    fig.subplots_adjust(hspace=0.4)
    chart_path = "metrics/unavailability_experiment.pdf"
    fig.savefig(chart_path)
    _logger.info(f"Created {chart_path}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    chart_network_latency()
    chart_unavailability()

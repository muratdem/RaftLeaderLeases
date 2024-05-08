import logging

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.lines import Line2D

_logger = logging.getLogger("chart")

BARWIDTH = 10
LINEWIDTH = 2
FIGSIZE = (5, 5)  # Matplotlib default is 6.4 x 4.8.


def chart_network_latency():
    df = pd.read_csv("metrics/network_latency_experiment.csv")
    columns = ["read_latency", "write_latency"]
    max_y = max([max(df[c]) for c in columns])

    df_no_lease = df[df["lease_enabled"] == False]
    df_lease = df[df["lease_enabled"] == True]
    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, sharey=True, figsize=FIGSIZE)
    ax2.set(xlabel="one-way network latency (μs)")

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

    fig.legend(loc="upper center",
               bbox_to_anchor=(0.5, 1.005),
               ncol=2,
               handles=[Line2D([0], [0], color=color) for color in ["C0", "C1"]],
               labels=["read latency", "write latency"])
    fig.text(0.002, 0.55, "microseconds", va="center", rotation="vertical")
    fig.text(0.95, 0.75, "no leases", va="center", rotation="vertical")
    fig.text(0.95, 0.25, "LeaseGuard", va="center", rotation="vertical")
    chart_path = "metrics/network_latency_experiment.pdf"
    fig.tight_layout()
    fig.savefig(chart_path)
    _logger.info(f"Created {chart_path}")


def chart_unavailability():
    from unavailability_experiment import SUB_EXPERIMENT_PARAMS

    csv = pd.read_csv("metrics/unavailability_experiment.csv")
    fig, axes = plt.subplots(
        len(SUB_EXPERIMENT_PARAMS), 1, sharex=True, sharey=True, figsize=FIGSIZE)

    def resample_data(lease_enabled,
                      inherit_lease_enabled,
                      defer_commit_enabled,
                      **_):
        df_micros = csv[
            (csv["lease_enabled"] == lease_enabled)
            & (csv["inherit_lease_enabled"] == inherit_lease_enabled)
            & (csv["defer_commit_enabled"] == defer_commit_enabled)
            ].copy()

        df_micros.sort_values(by=["end_ts"], inplace=True)
        df_micros["timestamp"] = pd.to_datetime(df_micros["end_ts"], unit="us")
        resampled = df_micros.resample("10ms", on="timestamp").sum()
        # Remove first and last rows, which have low throughput due to artifacts.
        return resampled[["reads", "writes"]].iloc[1:-1]

    dfs = [resample_data(**options) for options in SUB_EXPERIMENT_PARAMS]
    # Use max read throughput as y limit. Max write throughput could be very high.
    y_lim = 2 * max(df["reads"].max() for df in dfs)
    axes[-1].set(xlabel="Milliseconds")

    for i, df in enumerate(dfs):
        ax = axes[i]
        sub_exp_params = SUB_EXPERIMENT_PARAMS[i]
        # Remove borders
        for spine in ax.spines.values():
            spine.set_visible(False)

        if len(df) > 0:
            for column in ["reads", "writes"]:
                ax.plot((df.index - pd.Timestamp(0)).total_seconds() * 1000,
                        df[column],
                        label=column)
                ax.set_ylim(0, y_lim)

        # sub_exp_params are in microseconds, the x axis is in milliseconds.
        # Leader crash.
        ax.axvline(
            x=sub_exp_params.stepdown_time / 1000,
            color="red",
            linestyle="dotted")
        # New leader elected.
        ax.axvline(
            x=(sub_exp_params.stepup_time) / 1000,
            color="green",
            linestyle="dotted")
        if sub_exp_params.lease_enabled:
            # Old lease expires.
            ax.axvline(
                x=(sub_exp_params.stepdown_time + sub_exp_params.lease_timeout) / 1000,
                color="purple",
                linestyle="dotted")

        ax.text(1.02, 0.5, sub_exp_params.title, va="center", ha="center",
                rotation="vertical",
                transform=ax.transAxes)

    axes[0].text(SUB_EXPERIMENT_PARAMS[0].stepdown_time / 1000 + 40,
                 int(y_lim * 0.85),
                 "← leader crash",
                 color="red",
                 bbox=dict(facecolor="white", edgecolor="none"))
    axes[0].text(SUB_EXPERIMENT_PARAMS[0].stepup_time / 1000 + 40,
                 int(y_lim * 0.6),
                 "← new leader elected",
                 color="green")
    axes[1].text((SUB_EXPERIMENT_PARAMS[0].stepdown_time
                  + SUB_EXPERIMENT_PARAMS[0].lease_timeout) / 1000 - 40,
                 int(y_lim * 0.75),
                 "old lease expires → ",
                 color="purple",
                 bbox=dict(facecolor="white", edgecolor="none"),
                 horizontalalignment="right")
    fig.legend(loc="upper center",
               bbox_to_anchor=(0.5, 1.005),
               ncol=2,
               handles=[Line2D([0], [0], color=color) for color in ["C0", "C1"]],
               labels=["reads", "writes"])
    fig.text(0.002, 0.5, "operations per millisecond", va="center", rotation="vertical")
    fig.tight_layout()
    fig.subplots_adjust(hspace=0.4, top=0.92)
    chart_path = "metrics/unavailability_experiment.pdf"
    fig.savefig(chart_path)
    _logger.info(f"Created {chart_path}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    plt.rcParams.update({"font.size": 11})
    chart_network_latency()
    chart_unavailability()

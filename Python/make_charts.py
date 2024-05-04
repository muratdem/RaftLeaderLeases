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
    from unavailability_experiment import SUB_EXPERIMENT_PARAMS

    csv = pd.read_csv("metrics/unavailability_experiment.csv")
    fig, axes = plt.subplots(len(SUB_EXPERIMENT_PARAMS), 1, sharex=True, sharey=True)

    def resample_data(leases_enabled,
                      read_lease_opt_enabled,
                      speculative_write_opt_enabled,
                      **_):
        df_micros = csv[
            (csv["leases_enabled"] == leases_enabled)
            & (csv["read_lease_opt_enabled"] == read_lease_opt_enabled)
            & (csv["speculative_write_opt_enabled"] == speculative_write_opt_enabled)
            ].copy()

        # Maybe I'm debugging unavailability_experiment.py didn't generate all the data.
        if len(df_micros) == 0:
            _logger.warning("No data")
            return df_micros

        df_micros["timestamp"] = pd.to_datetime(df_micros["execution_ts"], unit="us")
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
        ax.set(title=sub_exp_params["title"])
        # Remove borders
        for spine in ax.spines.values():
            spine.set_visible(False)

        if len(df) == 0:
            continue

        for column in ["reads", "writes"]:
            ax.plot((df.index - df.index.min()).total_seconds() * 1000,
                    df[column],
                    label=column)
            ax.set_ylim(0, y_lim)

        # These parameters are in microseconds, the x axis is in milliseconds.
        ax.axvline(
            x=sub_exp_params.stepdown_time / 1000,
            color="red")
        ax.axvline(
            x=(sub_exp_params.stepdown_time + sub_exp_params.election_timeout) / 1000,
            color="green")
        ax.axvline(
            x=(sub_exp_params.stepdown_time + sub_exp_params.lease_timeout) / 1000,
            color="purple")

    axes[0].text(SUB_EXPERIMENT_PARAMS[0].stepdown_time /1000,
                 int(y_lim * 0.8),
                 "leader crash →  ",
                 color="red",
                 horizontalalignment="right")
    axes[0].text((SUB_EXPERIMENT_PARAMS[0].stepdown_time
                 + SUB_EXPERIMENT_PARAMS[0].election_timeout) / 1000 + 45,
                 int(y_lim * 0.8),
                 "← new leader elected",
                 color="green",
                 bbox=dict(facecolor="white", edgecolor="none"))
    axes[0].text((SUB_EXPERIMENT_PARAMS[0].stepdown_time
                 + SUB_EXPERIMENT_PARAMS[0].lease_timeout) / 1000 + 45,
                 int(y_lim * 0.5),
                 "← old lease expires", color="purple")
    axes[1].legend(loc="center", framealpha=1, fancybox=False)
    fig.tight_layout()
    fig.subplots_adjust(hspace=0.4)
    chart_path = "metrics/unavailability_experiment.pdf"
    fig.savefig(chart_path)
    _logger.info(f"Created {chart_path}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    chart_network_latency()
    chart_unavailability()

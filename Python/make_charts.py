import logging

import matplotlib.font_manager as font_manager
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

_logger = logging.getLogger("chart")


def chart_network_latency():
    csv = pd.read_csv("metrics/network_latency_experiment.csv")
    df_no_lease = csv[csv["lease_enabled"] == False]
    df_lease = csv[csv["lease_enabled"] == True]

    BARWIDTH = 6
    LINEWIDTH = 2
    fig, ax = plt.subplots(figsize=(5, 3))
    ax.set(xlabel="one-way network latency (μs)")
    ax.tick_params(axis="x", bottom=False)

    for offset, color, df, column in [
        (-2, "C1", df_no_lease, "write_latency"),
        (-1, "C0", df_no_lease, "read_latency"),
        (1, "C1", df_lease, "write_latency"),
        (2, "C0", df_lease, "read_latency"),
    ]:
        is_zeros = (df[column] == 0).all()
        rects = ax.bar(
            df["one_way_latency_mean"] + offset * (BARWIDTH + LINEWIDTH * 2),
            df[column],
            BARWIDTH,
            label=column,
            color=color,
            edgecolor=color,
            linewidth=LINEWIDTH)

        if is_zeros:
            ax.bar_label(rects, padding=3)

    fig.legend(loc="upper center",
               bbox_to_anchor=(0.5, .95),
               ncol=2,
               handles=[Patch(color=color) for color in ["C1", "C0"]],
               handleheight=0.65,
               handlelength=0.65,
               labels=["write latency", "read latency"])
    ax.text(df_no_lease["one_way_latency_mean"].min() - 3 * BARWIDTH - 2 * LINEWIDTH,
            df_no_lease["write_latency"].min() + 50,
            r"$\leftarrow$ no lease",
            rotation="vertical")
    ax.text(df_lease["one_way_latency_mean"].min() + BARWIDTH + LINEWIDTH,
            df_lease["write_latency"].min() + 50,
            r"$\leftarrow$ lease",
            rotation="vertical")
    fig.text(0.002, 0.55, "microseconds", va="center", rotation="vertical")

    # Remove chart borders
    for spine in ax.spines.values():
        spine.set_visible(False)

    fig.tight_layout()
    fig.subplots_adjust(top=0.9)
    chart_path = "metrics/network_latency_experiment.pdf"
    fig.savefig(chart_path, bbox_inches="tight", pad_inches=0)
    _logger.info(f"Created {chart_path}")


def chart_unavailability():
    from unavailability_experiment import SUB_EXPERIMENT_PARAMS

    csv = pd.read_csv("metrics/unavailability_experiment.csv")
    fig, axes = plt.subplots(
        len(SUB_EXPERIMENT_PARAMS), 1, sharex=True, sharey=True, figsize=(5, 5))

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
    # Max read and write throughput spike in the final case, so use the middle case.
    y_lim = 1.5 * dfs[2]["writes"].max()
    axes[-1].set(xlabel=r"time in milliseconds $\rightarrow$")

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
                 r"$\leftarrow$ leader crash",
                 color="red",
                 bbox=dict(facecolor="white", edgecolor="none"))
    axes[0].text(SUB_EXPERIMENT_PARAMS[0].stepup_time / 1000 + 40,
                 int(y_lim * 0.6),
                 r"$\leftarrow$ new leader elected",
                 color="green")
    axes[1].text((SUB_EXPERIMENT_PARAMS[0].stepdown_time
                  + SUB_EXPERIMENT_PARAMS[0].lease_timeout) / 1000 - 40,
                 int(y_lim * 0.75),
                 r"old lease expires $\rightarrow$ ",
                 color="purple",
                 bbox=dict(facecolor="white", edgecolor="none"),
                 horizontalalignment="right")
    fig.legend(loc="upper center",
               bbox_to_anchor=(0.5, 1.005),
               ncol=2,
               handles=[Line2D([0], [0], color=color) for color in ["C1", "C0"]],
               labels=["writes", "reads"])
    fig.text(0.002, 0.5, "operations per millisecond", va="center", rotation="vertical")
    fig.tight_layout()
    fig.subplots_adjust(hspace=0.4, top=0.92)
    chart_path = "metrics/unavailability_experiment.pdf"
    fig.savefig(chart_path, bbox_inches="tight", pad_inches=0)
    _logger.info(f"Created {chart_path}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    plt.rcParams.update({"font.size": 12})
    font_path = "cmunrm.ttf"  # Computer Modern Roman, like Latex's default.
    font_manager.fontManager.addfont(font_path)
    font_properties = font_manager.FontProperties(fname=font_path)
    plt.rcParams["font.family"] = font_properties.get_name()
    chart_network_latency()
    chart_unavailability()

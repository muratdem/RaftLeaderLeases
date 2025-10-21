import logging

import matplotlib.font_manager as font_manager
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

_logger = logging.getLogger("chart")


def chart_network_latency():
    csv = pd.read_csv("metrics/network_latency_experiment.csv")
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, sharex=True, figsize=(5, 4))

    ax3.set(xlabel="added one-way network latency (ms)")

    ax1.yaxis.set_major_locator(plt.FixedLocator([0, 10, 20]))
    ax2.yaxis.set_major_locator(plt.FixedLocator([0, 10, 20]))
    ax3.yaxis.set_major_locator(plt.FixedLocator([0, 10, 20]))

    ax1.xaxis.set_major_locator(plt.NullLocator())
    ax2.xaxis.set_major_locator(plt.NullLocator())
    ax3.xaxis.set_major_locator(plt.MultipleLocator(1))

    for ax in [ax1, ax2, ax3]:
        ax.yaxis.grid(True, which='both', linestyle='--', linewidth=0.5)
        ax.set_axisbelow(True)

    # x-offset, color, config_name, df, operation_type, axes
    combos = [
        (-0.25, "C1", "inconsistent", "write_latency", ax1),
        (0.25, "C0", "inconsistent", "read_latency", ax1),
        (-0.25, "C1", "quorum", "write_latency", ax2),
        (0.25, "C0", "quorum", "read_latency", ax2),
        (-0.25, "C1", "LeaseGuard", "write_latency", ax3),
        (0.25, "C0", "LeaseGuard", "read_latency", ax3),
    ]

    for offset, color, config_name, operation_type, ax in combos:
        if config_name == "inconsistent":
            df = csv[(csv["leaseguard_enabled"] == False) & (
                csv["quorum_check_enabled"] == False)]
        elif config_name == "quorum":
            df = csv[csv["quorum_check_enabled"] == True]
        else:
            df = csv[csv["leaseguard_enabled"] == True]

        column = f"{operation_type}_p90"
        hatch = "//" if "write" in operation_type else "xx"

        # The x-axis "one_way_latency_mean" is the artificially added network latency.
        x = df["one_way_latency_mean"] / 1000 + offset
        # convert nanos to millis and ensure min height of 1
        y = df[column].apply(lambda x: max(x / 1000, 1))
        # Draw hatch only.
        ax.bar(
            x,
            y,
            label=f"{config_name} {operation_type}",
            color="none",
            width=0.3,
            edgecolor=color,
            hatch=hatch,
            facecolor="none",
            linewidth=0.5,
            zorder=2,
        )
        # Draw the edge.
        ax.bar(
            x,
            y,
            color="none",
            width=0.3,
            edgecolor="black",
            facecolor="none",
            linewidth=0.5,
            zorder=3,
        )
        # Add config_name to the upper left interior of each subplot
        ax.text(
            0.02, 0.7, config_name,
            transform=ax.transAxes,
            verticalalignment='top',
            horizontalalignment='left',
            fontsize=12,
        )

    # Draw hatch only.
    fig.legend(
        loc="upper center",
        ncol=2,
        handles=[
            Patch(
                facecolor="none", edgecolor="C1", label="write latency p90", hatch="//",
                linewidth=0.5
            ),
            Patch(
                facecolor="none", edgecolor="C0", label="read latency p90", hatch="xx",
                linewidth=0.5
            ),
        ],
        frameon=False,
    )
    # Draw edges.
    fig.legend(
        loc="upper center",
        ncol=2,
        handles=[
            Patch(
                facecolor="none", edgecolor="black", label="write latency p90",
                linewidth=0.5
            ),
            Patch(
                facecolor="none", edgecolor="black", label="read latency p90",
                linewidth=0.5
            ),
        ],
        frameon=False,
        labelcolor="none",
    )

    fig.text(-0.01, 0.5, "p90 latency (ms)", va="center", rotation="vertical")
    fig.tight_layout()
    fig.subplots_adjust(top=0.9)
    chart_path = "metrics/network_latency_experiment_simulation.pdf"
    fig.savefig(chart_path, bbox_inches="tight", pad_inches=0)
    _logger.info(f"Created {chart_path}")


def chart_unavailability():
    from unavailability_experiment import SUB_EXPERIMENT_PARAMS

    csv = pd.read_csv("metrics/unavailability_experiment.csv")
    fig, axes = plt.subplots(
        len(SUB_EXPERIMENT_PARAMS), 1, sharex=True, sharey=True, figsize=(5, 7))

    def resample_data(quorum_check_enabled,
                      leaseguard_enabled,
                      inherit_lease_enabled,
                      defer_commit_enabled,
                      **_):
        df_micros = csv[
            (csv["quorum_check_enabled"] == quorum_check_enabled)
            & (csv["leaseguard_enabled"] == leaseguard_enabled)
            & (csv["inherit_lease_enabled"] == inherit_lease_enabled)
            & (csv["defer_commit_enabled"] == defer_commit_enabled)
            ].copy()

        df_micros.sort_values(by=["end_ts"], inplace=True)
        df_micros["timestamp"] = pd.to_datetime(df_micros["end_ts"], unit="us")
        # 10ms moving average, then convert to seconds.
        resampled = 1000 * df_micros.resample("10ms", on="timestamp").sum() / 10
        # Remove first and last rows, which have low throughput due to artifacts.
        return resampled[["reads", "writes"]].iloc[1:-1]

    dfs = [resample_data(**options) for options in SUB_EXPERIMENT_PARAMS]
    y_lim = 1.3 * dfs[2]["reads"].max()
    axes[-1].set(xlabel=r"time in milliseconds $\rightarrow$")
    label_font_size = 12

    for i, df in enumerate(dfs):
        ax = axes[i]
        sub_exp_params = SUB_EXPERIMENT_PARAMS[i]
        for column in ["reads", "writes"]:
            ax.plot((df.index - pd.Timestamp(0)).total_seconds() * 1000,
                    df[column],
                    label=column,
                    linewidth=1.3)
            ax.set_ylim(0, y_lim)
            ax.set_xlim(100, 1900)

        # Defer commit lets write throughput spike off the chart when old lease expires.
        if sub_exp_params["defer_commit_enabled"] and len(df) > 0:
            # Get the first off-chart value
            off_chart_writes = df[df["writes"] > y_lim]
            off_chart_value = off_chart_writes["writes"].iloc[0]
            off_chart_time = (off_chart_writes.index[0]
                              - pd.Timestamp(0)).total_seconds() * 1000
            # Place text label just below the top of the subplot.
            ax.text(off_chart_time - 65, y_lim + 800,
                    f"off chart, {off_chart_value/1000:.0f}k ops/sec",
                    ha="right", va="top",
                    fontsize=label_font_size)
            ax.text(off_chart_time - 75, y_lim + 500,
                    r"$\searrow$",
                    ha="left", va="top")

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
        if sub_exp_params.leaseguard_enabled:
            # Old lease expires.
            ax.axvline(
                x=(sub_exp_params.stepdown_time
                   + sub_exp_params.lease_timeout) / 1000 - 35,
                color="purple",
                linestyle="dotted")

        ax.text(1.05, 0.5, sub_exp_params.title, va="center", ha="center",
                rotation="vertical",
                transform=ax.transAxes)

    EVENT_LABEL_HEIGHT = 1500
    axes[0].text(520,
                 EVENT_LABEL_HEIGHT,
                 "$\\leftarrow$ leader\n    crash",
                 color="red",
                 fontsize=label_font_size)
    axes[1].text(560,
                 EVENT_LABEL_HEIGHT,
                 "new leader\nelected    $\\rightarrow$",
                 color="green",
                 fontsize=label_font_size)
    axes[2].text(1080,
                 EVENT_LABEL_HEIGHT,
                 "old lease\nexpires  $\\rightarrow$ ",
                 color="purple",
                 fontsize=label_font_size)
    fig.legend(loc="upper center",
               bbox_to_anchor=(0.5, 0.98),
               ncol=2,
               handles=[Line2D([0], [0], color=color) for color in ["C1", "C0"]],
               labels=["writes", "reads"],
               frameon=False)
    fig.text(0.002, 0.5, "operations per second", va="center", rotation="vertical")
    fig.tight_layout()
    fig.subplots_adjust(hspace=0.4, top=0.92)
    chart_path = "metrics/unavailability_experiment_simulation.pdf"
    fig.savefig(chart_path, bbox_inches="tight", pad_inches=0)
    _logger.info(f"Created {chart_path}")


def chart_skewness_experiment():
    csv = pd.read_csv("metrics/skewness_experiment.csv")
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.plot(csv["zipf_skewness"],
            csv["max_possible_throughput_per_sec"],
            marker="o",
            linestyle="-",
            color="black")
    ax.plot(csv["zipf_skewness"],
            csv["experimental_throughput_per_sec"],
            marker="o",
            linestyle="-",
            color="C0")

    ax.legend(["Attempted reads/sec",
               "Successful reads/sec"], loc="best")
    ax.set_xlabel("Skewness")
    ax.set_ylabel("Read/sec with inherited lease")
    ax.set_title("Skewness Experiment")
    chart_path = "metrics/skewness_experiment.pdf"
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
    chart_skewness_experiment()

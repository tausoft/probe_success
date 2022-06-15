from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd
pd.options.mode.chained_assignment = None


class ProcessDataFrame:
    def __init__(self, file_name):
        # Load CSV file in DF
        try:
            # Start the timer
            print("Process is running, please wait...")
            timer_start = datetime.now()

            df = pd.read_csv(file_name)
            self.process_data(df)

            # Stop the timer and print out the time
            timer_end = datetime.now()
            time_done = timer_end - timer_start
            print("Finished in " + str(time_done))
        except Exception as e:
            print(f"Something went wrong: {e}")

    def process_data(self, df):
        # DF table rules
        df = (
            df.groupby(
                by=[
                    "name",
                    "platform",
                    "site",
                    "device_gen",
                    "probe_success",
                    "ts",
                    "step",
                ]
            )
            .count()
            .reset_index()
        )
        df = df.sort_values(by=["name", "ts"])

        # Get previous/next row data so that you can compare it with the current row
        df.loc[:, "previous_probe_success"] = df["probe_success"].astype(int).shift(1)
        df.loc[:, "previous_name"] = df["name"].astype(str).shift(1)
        df.loc[:, "next_name"] = df["name"].astype(str).shift(-1)
        df.loc[:, "date"] = pd.to_datetime(df["ts"], unit="s")

        df.loc[
            (df["probe_success"] == 0) & (df["name"] != df["previous_name"]),
            "went_down_time",
        ] = "N/A"
        df.loc[
            (df["probe_success"] == 0)
            & (df["previous_probe_success"] == 1)
            & (df["name"] == df["previous_name"]),
            "went_down_time",
        ] = df["date"].astype(str)
        df.loc[
            (df["probe_success"] == 0) & (df["name"] != df["next_name"]), "come_up_time"
        ] = "N/A"
        df.loc[
            (df["probe_success"] == 1)
            & (df["previous_probe_success"] == 0)
            & (df["name"] == df["previous_name"]),
            "come_up_time",
        ] = df["date"].astype(str)

        # Generate new Data Frame with in service data
        df_in_service_perc = df[["name", "probe_success", "step"]]
        df_in_service_perc = df_in_service_perc[
            df_in_service_perc["probe_success"] == 1
        ]
        df_in_service_perc = (
            df_in_service_perc.groupby(by=["name"])["step"]
            .sum()
            .reset_index(name="in_service_percentage")
        )

        # Remove NaN values and
        df.dropna(subset=["went_down_time", "come_up_time"], how="all", inplace=True)
        df = df.merge(df_in_service_perc, on="name", how="left")

        # Graph for Server state change in PNG format
        df_server_state = df[["ts", "site", "platform"]]
        df_server_state.loc[:, "site/platform"] = (
            df_server_state["site"] + "/" + df_server_state["platform"]
        )
        df_server_state.loc[:, "date"] = pd.to_datetime(
            df_server_state["ts"], unit="s"
        ).dt.date
        df_server_state = (
            df_server_state.groupby(by=["date", "site/platform"]).count().reset_index()
        )

        pivot = pd.pivot_table(
            data=df_server_state, index=["date"], columns=["site/platform"], values="ts"
        )
        ax = pivot.plot.bar(stacked=False, figsize=(8, 6))
        ax.set_title("Server State Change per Date", fontsize=20)
        for container in ax.containers:
            ax.bar_label(container)

        plt.tight_layout()
        plt.savefig("output/server_state_change.png")

        # Graph for CDN3-4 state change in PNG format
        df_platform_state = df[["ts", "platform"]]
        df_platform_state = df_platform_state.loc[
            df_platform_state["platform"] == "CDN3-4"
        ]
        df_platform_state.loc[:, "date"] = pd.to_datetime(
            df_platform_state["ts"], unit="s"
        ).dt.date
        df_platform_state = (
            df_platform_state.groupby(by=["date", "platform"]).count().reset_index()
        )

        pivot = pd.pivot_table(
            data=df_platform_state, index=["date"], columns=["platform"], values="ts"
        )
        ax = pivot.plot.bar(stacked=False, legend=False, figsize=(8, 6))
        ax.set_title("CDN3-4 State Change per Date", fontsize=20)
        for container in ax.containers:
            ax.bar_label(container)

        plt.tight_layout()
        plt.savefig("output/CDN3-4_state_change.png")

        # Data Frame table result
        df = df[
            [
                "name",
                "platform",
                "site",
                "device_gen",
                "went_down_time",
                "come_up_time",
                "in_service_percentage",
            ]
        ].reset_index(drop=True)
        df["in_service_percentage"] = round(
            (df["in_service_percentage"] / 604800 * 100), 2
        )
        df["in_service_percentage"] = df["in_service_percentage"].fillna(0.00)

        df["count"] = 1 + df.index // 2
        df = df.groupby("count", as_index=False).first()
        del df["count"]

        # Data Frame to HTML file
        probe_data_table = df.to_html(index=False)
        text_file = open("output/index.html", "w")
        text_file.write(
            "<html><head><meta name='viewport'content='width=device-width,initial-scale=1'><style>table,th,td{border:1px solid black;border-collapse:collapse;}table{margin-left:auto;margin-right: auto;}</style></head><body style='text-align:center;'>"
        )
        text_file.write(probe_data_table)
        text_file.write(
            "</br></br></br><img src='server_state_change.png'></br></br></br><img src='CDN3-4_state_change.png'></body></html>"
        )
        text_file.close()

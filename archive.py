import time
import boto3
import argparse
import asyncio
import pandas as pd
import datetime as dt
from scripts.load_markets import PerpMarket, SpotMarket, initialize_state
from scripts.event_parser import parse_event
from scripts.log_parser import get_logs_from_topledger
import io
import gc
from scripts.utils import chunks
from concurrent.futures import ThreadPoolExecutor, as_completed
import awswrangler as wr
from scripts.utils import snake_to_camel_df

PROFILE_NAME = "" # aws profile name with permissions to access destination bucket
DESTINATION_BUCKET_NAME = "" # destination bucket name
RPC_URL = "" # rpc url

PROGRAM_ID = "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH"
EVENT_TYPES = [
    "OrderActionRecord",
    "SettlePnlRecord",
    "DepositRecord",
    "InsuranceFundRecord",
    "InsuranceFundStakeRecord",
    "LiquidationRecord",
    "LPRecord",
    "FundingRateRecord",
    "FundingPaymentRecord",
]

session_default = boto3.Session(PROFILE_NAME)  # Change here to profile name
s3_resource = session_default.resource("s3")
DESTINATION_BUCKET = s3_resource.Bucket(DESTINATION_BUCKET_NAME) # Change here to destination bucket name 

def assume_role(arn, session_name):
    sts_client = boto3.client("sts")
    assumed_role = sts_client.assume_role(RoleArn=arn, RoleSessionName=session_name)

    credentials = assumed_role["Credentials"]
    return credentials


def filter_df(df, event_type):
    rez = df[df["event_type"] == event_type]
    if event_type == "OrderActionRecord":
        rez = rez[
            rez["args"].apply(
                lambda x: x.get("action") if isinstance(x, dict) else None
            )
            == "Fill"
        ]
    elif event_type == "SettlePnlRecord":
        rez["authority"] = rez["args"].apply(lambda x: x["userAuthority"])
        rez = rez[
            ~rez["authority"].isin(
                [
                    "5civ8nag6sdWWqqxu4oGwsNxAMXAPCSJbfevQGJ3QXNV",
                    "FionaRdGscg9iK3RA4H4pZrmFyFPDhRAUExeEHfZtUPm",
                    "HPVkxurXKioCF5AkgmzXxjLrZuXKFzBFd6UhNTNnnxvS",
                    "DJXgcKKf2n3rdpXt2mqCsqCm3vrDWyCPWw6kKGuGCLyv",
                    "GontTwDeBduvbW85oHyC8A7GekuT8X1NkZHDDdUWWvsV",
                    "D3VukrhsSXNM8f2wqx595cJaQtS22enTm9EgfumJT7Gv",
                    "ErenqwMN7zuun3NQh8dfgknNtfZGw6VRyw8dYybWdZ8",
                ]
            )
        ]
    elif event_type == "DepositRecord":
        ## Filter out the bad user
        rez["authority"] = rez["args"].apply(lambda x: x["userAuthority"])
        rez = rez[rez["authority"] != "882DFRCi5akKFyYxT4PP2vZkoQEGvm2Nsind2nPDuGqu"]

    return rez


def sanity_check(trades: pd.DataFrame):
    block_times = pd.to_datetime(trades["block_time"], format="%m/%d/%y %H:%M")
    date = block_times.iloc[0].date()

    after_midnight = dt.datetime.combine(date, dt.time(minute=1))
    noon = dt.datetime.combine(date, dt.time(hour=12))
    before_midnight = dt.datetime.combine(date, dt.time(hour=23, minute=59))

    window = pd.Timedelta(minutes=30)

    after_midnight_check = (block_times - after_midnight).abs().min() < window
    noon_check = (block_times - noon).abs().min() < window
    before_midnight_check = (block_times - before_midnight).abs().min() < window
    if not after_midnight_check:
        print(f"Missing data around 0:01 on {date}")
    if not noon_check:
        print(f"Missing data around 12:00 on {date}")
    if not before_midnight_check:
        print(f"Missing data around 23:59 on {date}")

    return after_midnight_check and noon_check and before_midnight_check


def process_trades(trades: pd.DataFrame, date, logs):
    from scripts.load_markets import PERP_MARKETS, SPOT_MARKETS

    userTradesMap = {}
    marketTradesMap = {}

    trades["logs"] = trades["tx_id"].apply(
        lambda x: list(
            filter(lambda event: event.name == "OrderActionRecord", logs.get(x, []))
        )
    )

    if not sanity_check(trades):
        print("Potentially missing data around 0:01, 12:00, or 23:59")

    for _, row in trades.iterrows():
        logs = row["logs"]
        for log in logs:
            parsed = parse_event(
                log, {"slot": row["block_slot"], "tx_sig": row["tx_id"]}
            )
            parsed["programId"] = PROGRAM_ID

            marketType = "perp" if parsed["marketType"] == "perp" else "spot"
            market: PerpMarket | SpotMarket = next(
                filter(
                    lambda m: m.marketIndex == parsed["marketIndex"],
                    PERP_MARKETS if marketType == "perp" else SPOT_MARKETS,
                ),
                None,
            )
            marketSymbol = market.symbol

            ## Log for maker
            if parsed["maker"] is not None:
                userPrefix = (
                    "program/"
                    + PROGRAM_ID
                    + "/user/{}/tradeRecords/{}".format(parsed["maker"], date.year)
                )
                if userPrefix not in userTradesMap:
                    userTradesMap[userPrefix] = []
                userTradesMap[userPrefix].append(parsed)

            ## Log for taker
            if parsed["taker"] is not None:
                userPrefix = (
                    "program/"
                    + PROGRAM_ID
                    + "/user/{}/tradeRecords/{}".format(parsed["taker"], date.year)
                )
                if userPrefix not in userTradesMap:
                    userTradesMap[userPrefix] = []
                userTradesMap[userPrefix].append(parsed)

            # Log for market
            marketPrefix = (
                "program/"
                + PROGRAM_ID
                + "/market/{}/tradeRecords/{}".format(marketSymbol, date.year)
            )
            if marketPrefix not in marketTradesMap:
                marketTradesMap[marketPrefix] = []
            marketTradesMap[marketPrefix].append(parsed)

    for marketPrefix in marketTradesMap.keys():
        df_to_write = pd.DataFrame(marketTradesMap[marketPrefix])
        ## De-duplicate
        df_to_write = df_to_write.drop_duplicates(
            subset=[
                "txSig",
                "taker",
                "maker",
                "takerOrderId",
                "makerOrderId",
                "marketIndex",
                "marketType",
                "action",
                "fillRecordId",
                "baseAssetAmountFilled",
            ]
        )

        ## Spot check missing fills before writing
        df_to_write = df_to_write[df_to_write["baseAssetAmountFilled"] != 0]
        df_to_write.dropna(subset=["fillRecordId"], inplace=True)
        if len(df_to_write) == 0:
            print(f"No fills for {marketPrefix} on {date}")
            continue
        df_to_write = df_to_write.sort_values("fillRecordId")
        full_range = pd.Series(
            range(
                int(df_to_write["fillRecordId"].min()),
                int(df_to_write["fillRecordId"].max()) + 1,
            )
        )
        missing_values = set(full_range) - set(df_to_write["fillRecordId"])
        if len(missing_values) > 0:
            print(
                f"Missing values for market {marketPrefix}, length: ",
                len(missing_values),
            )
            print(missing_values)
        else:
            print(f"No missing fill record ids for {marketPrefix} on {date}")

        csv_buffer = io.BytesIO()
        df_to_write = df_to_write.drop_duplicates(
            subset=[
                "txSig",
                "taker",
                "maker",
                "takerOrderId",
                "makerOrderId",
                "marketIndex",
                "marketType",
                "action",
                "fillRecordId",
                "baseAssetAmountFilled",
            ]
        )
        df_to_write.to_csv(csv_buffer, index=False, compression="gzip")
        object_path = "{}/{}".format(marketPrefix, date.strftime("%Y%m%d"))
        DESTINATION_BUCKET.put_object(
            Key=object_path,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv",
            ContentEncoding="gzip",
        )

    for userPrefix in userTradesMap.keys():
        df_to_write = pd.DataFrame(userTradesMap[userPrefix])
        ## De-duplicate
        df_to_write = df_to_write.drop_duplicates(
            subset=[
                "txSig",
                "taker",
                "maker",
                "takerOrderId",
                "makerOrderId",
                "marketIndex",
                "marketType",
                "action",
                "fillRecordId",
                "baseAssetAmountFilled",
            ]
        )

        csv_buffer = io.BytesIO()
        df_to_write.to_csv(csv_buffer, index=False, compression="gzip")
        object_path = "{}/{}".format(userPrefix, date.strftime("%Y%m%d"))
        DESTINATION_BUCKET.put_object(
            Key=object_path,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv",
            ContentEncoding="gzip",
        )


def process_settle_pnl(records, date, logs):
    userMap = {}

    records["logs"] = records["tx_id"].apply(
        lambda x: list(
            filter(lambda event: event.name == "SettlePnlRecord", logs.get(x, []))
        )
    )

    for _, row in records.iterrows():
        logs = row["logs"]
        for log in logs:
            parsed = parse_event(
                log, {"slot": row["block_slot"], "tx_sig": row["tx_id"]}
            )
            parsed["programId"] = PROGRAM_ID

            ## Log for taker
            if parsed["user"] is not None:
                userPrefix = (
                    "program/"
                    + PROGRAM_ID
                    + "/user/{}/settlePnlRecords/{}".format(parsed["user"], date.year)
                )
                if userPrefix not in userMap:
                    userMap[userPrefix] = []
                userMap[userPrefix].append(parsed)

    for userPrefix in userMap.keys():
        df_to_write = pd.DataFrame(userMap[userPrefix])
        ## De-duplicate
        df_to_write = df_to_write.drop_duplicates(
            subset=["txSig", "marketIndex", "user"]
        )

        csv_buffer = io.BytesIO()
        df_to_write.to_csv(csv_buffer, index=False, compression="gzip")
        object_path = "{}/{}".format(userPrefix, date.strftime("%Y%m%d"))
        DESTINATION_BUCKET.put_object(
            Key=object_path,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv",
            ContentEncoding="gzip",
        )


def process_deposit(records, date, logs):
    userMap = {}

    records["logs"] = records["tx_id"].apply(
        lambda x: list(
            filter(lambda event: event.name == "DepositRecord", logs.get(x, []))
        )
    )

    for _, row in records.iterrows():
        logs = row["logs"]
        for log in logs:
            parsed = parse_event(
                log, {"slot": row["block_slot"], "tx_sig": row["tx_id"]}
            )
            parsed["programId"] = PROGRAM_ID

            ## Log for taker
            if parsed["user"] is not None:
                userPrefix = (
                    "program/"
                    + PROGRAM_ID
                    + "/user/{}/depositRecords/{}".format(parsed["user"], date.year)
                )
                if userPrefix not in userMap:
                    userMap[userPrefix] = []
                userMap[userPrefix].append(parsed)

    for userPrefix in userMap.keys():
        df_to_write = pd.DataFrame(userMap[userPrefix])
        ## De-duplicate
        df_to_write = df_to_write.drop_duplicates(
            subset=[
                "txSig",
                "marketIndex",
                "depositRecordId",
            ]
        )

        csv_buffer = io.BytesIO()
        df_to_write.to_csv(csv_buffer, index=False, compression="gzip")
        object_path = "{}/{}".format(userPrefix, date.strftime("%Y%m%d"))
        DESTINATION_BUCKET.put_object(
            Key=object_path,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv",
            ContentEncoding="gzip",
        )


def process_insurance_fund(records, date, logs):
    from scripts.load_markets import SPOT_MARKETS

    marketMap = {}

    records["logs"] = records["tx_id"].apply(
        lambda x: list(
            filter(lambda event: event.name == "InsuranceFundRecord", logs.get(x, []))
        )
    )

    for _, row in records.iterrows():
        logs = row["logs"]
        for log in logs:
            parsed = parse_event(
                log, {"slot": row["block_slot"], "tx_sig": row["tx_id"]}
            )
            parsed["programId"] = PROGRAM_ID

            ## Log for market
            market: SpotMarket = next(
                filter(
                    lambda m: m.marketIndex == parsed["spotMarketIndex"],
                    SPOT_MARKETS,
                ),
                None,
            )

            marketPrefix = (
                "program/"
                + PROGRAM_ID
                + "/market/{}/insuranceFundRecords/{}".format(market.symbol, date.year)
            )
            if marketPrefix not in marketMap:
                marketMap[marketPrefix] = []
            marketMap[marketPrefix].append(parsed)

    for marketPrefix in marketMap.keys():
        df_to_write = pd.DataFrame(marketMap[marketPrefix])
        ## De-duplicate
        df_to_write = df_to_write.drop_duplicates(
            subset=["txSig", "ts", "perpMarketIndex", "spotMarketIndex"]
        )

        csv_buffer = io.BytesIO()
        df_to_write.to_csv(csv_buffer, index=False, compression="gzip")
        object_path = "{}/{}".format(marketPrefix, date.strftime("%Y%m%d"))
        DESTINATION_BUCKET.put_object(
            Key=object_path,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv",
            ContentEncoding="gzip",
        )


def process_insurance_fund_stake(records, date, logs):
    userMap = {}

    records["logs"] = records["tx_id"].apply(
        lambda x: list(
            filter(
                lambda event: event.name == "InsuranceFundStakeRecord",
                logs.get(x, []),
            )
        )
    )

    for _, row in records.iterrows():
        logs = row["logs"]
        for log in logs:
            parsed = parse_event(
                log, {"slot": row["block_slot"], "tx_sig": row["tx_id"]}
            )
            parsed["programId"] = PROGRAM_ID

            ## Log for taker
            if parsed["userAuthority"] is not None:
                userPrefix = (
                    "program/"
                    + PROGRAM_ID
                    + "/authority/{}/insuranceFundStakeRecords/{}".format(
                        parsed["userAuthority"], date.year
                    )
                )
                if userPrefix not in userMap:
                    userMap[userPrefix] = []
                userMap[userPrefix].append(parsed)

    for userPrefix in userMap.keys():
        df_to_write = pd.DataFrame(userMap[userPrefix])
        ## De-duplicate
        df_to_write = df_to_write.drop_duplicates(
            subset=["txSig", "ts", "marketIndex", "userAuthority"]
        )

        csv_buffer = io.BytesIO()
        df_to_write.to_csv(csv_buffer, index=False, compression="gzip")
        object_path = "{}/{}".format(userPrefix, date.strftime("%Y%m%d"))
        DESTINATION_BUCKET.put_object(
            Key=object_path,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv",
            ContentEncoding="gzip",
        )


def process_liquidation(records, date, logs):
    userMap = {}

    records["logs"] = records["tx_id"].apply(
        lambda x: list(
            filter(lambda event: event.name == "LiquidationRecord", logs.get(x, []))
        )
    )

    for _, row in records.iterrows():
        logs = row["logs"]
        for log in logs:
            parsed = parse_event(
                log, {"slot": row["block_slot"], "tx_sig": row["tx_id"]}
            )
            parsed["programId"] = PROGRAM_ID

            ## Log for taker
            if parsed["user"] is not None:
                userPrefix = (
                    "program/"
                    + PROGRAM_ID
                    + "/user/{}/liquidationRecords/{}".format(parsed["user"], date.year)
                )
                if userPrefix not in userMap:
                    userMap[userPrefix] = []
                userMap[userPrefix].append(parsed)

    for userPrefix in userMap.keys():
        df_to_write = pd.DataFrame(userMap[userPrefix])
        ## De-duplicate
        df_to_write = df_to_write.drop_duplicates(
            subset=[
                "txSig",
                "user",
                "liquidationId",
                "marginRequirement",
            ]
        )

        csv_buffer = io.BytesIO()
        df_to_write.to_csv(csv_buffer, index=False, compression="gzip")
        object_path = "{}/{}".format(userPrefix, date.strftime("%Y%m%d"))
        DESTINATION_BUCKET.put_object(
            Key=object_path,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv",
            ContentEncoding="gzip",
        )


def process_lp(records, date, logs):
    userMap = {}

    records["logs"] = records["tx_id"].apply(
        lambda x: list(filter(lambda event: event.name == "LPRecord", logs.get(x, [])))
    )

    for _, row in records.iterrows():
        logs = row["logs"]
        for log in logs:
            parsed = parse_event(
                log, {"slot": row["block_slot"], "tx_sig": row["tx_id"]}
            )
            parsed["programId"] = PROGRAM_ID

            ## Log for taker
            if parsed["user"] is not None:
                userPrefix = (
                    "program/"
                    + PROGRAM_ID
                    + "/user/{}/lpRecord/{}".format(parsed["user"], date.year)
                )
                if userPrefix not in userMap:
                    userMap[userPrefix] = []
                userMap[userPrefix].append(parsed)

    for userPrefix in userMap.keys():
        df_to_write = pd.DataFrame(userMap[userPrefix])
        ## De-duplicate
        df_to_write = df_to_write.drop_duplicates(
            subset=["txSig", "user", "marketIndex"]
        )

        csv_buffer = io.BytesIO()
        df_to_write.to_csv(csv_buffer, index=False, compression="gzip")
        object_path = "{}/{}".format(userPrefix, date.strftime("%Y%m%d"))
        DESTINATION_BUCKET.put_object(
            Key=object_path,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv",
            ContentEncoding="gzip",
        )


def process_funding_rate(records, date, logs):
    from scripts.load_markets import PERP_MARKETS

    marketMap = {}

    records["logs"] = records["tx_id"].apply(
        lambda x: list(
            filter(lambda event: event.name == "FundingRateRecord", logs.get(x, []))
        )
    )

    for _, row in records.iterrows():
        logs = row["logs"]
        for log in logs:
            parsed = parse_event(
                log, {"slot": row["block_slot"], "tx_sig": row["tx_id"]}
            )
            parsed["programId"] = PROGRAM_ID

            ## Log for market
            market: SpotMarket = next(
                filter(lambda m: m.marketIndex == parsed["marketIndex"], PERP_MARKETS),
                None,
            )

            marketPrefix = (
                "program/"
                + PROGRAM_ID
                + "/market/{}/fundingRateRecords/{}".format(market.symbol, date.year)
            )
            if marketPrefix not in marketMap:
                marketMap[marketPrefix] = []
            marketMap[marketPrefix].append(parsed)

    for marketPrefix in marketMap.keys():
        df_to_write = pd.DataFrame(marketMap[marketPrefix])
        ## De-duplicate
        df_to_write = df_to_write.drop_duplicates(
            subset=[
                "txSig",
                "marketIndex",
                "recordId",
            ]
        )

        csv_buffer = io.BytesIO()
        df_to_write.to_csv(csv_buffer, index=False, compression="gzip")
        object_path = "{}/{}".format(marketPrefix, date.strftime("%Y%m%d"))
        DESTINATION_BUCKET.put_object(
            Key=object_path,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv",
            ContentEncoding="gzip",
        )


def process_funding_payment(records, date, logs):
    userMap = {}

    records["logs"] = records["tx_id"].apply(
        lambda x: list(
            filter(lambda event: event.name == "FundingPaymentRecord", logs.get(x, []))
        )
    )

    for _, row in records.iterrows():
        logs = row["logs"]
        for log in logs:
            parsed = parse_event(
                log, {"slot": row["block_slot"], "tx_sig": row["tx_id"]}
            )
            parsed["programId"] = PROGRAM_ID

            ## Log for taker
            if parsed["user"] is not None:
                userPrefix = (
                    "program/"
                    + PROGRAM_ID
                    + "/user/{}/fundingPaymentRecords/{}".format(
                        parsed["user"], date.year
                    )
                )
                if userPrefix not in userMap:
                    userMap[userPrefix] = []
                userMap[userPrefix].append(parsed)

    for userPrefix in userMap.keys():
        df_to_write = pd.DataFrame(userMap[userPrefix])

        ## De-duplicate
        df_to_write = df_to_write.drop_duplicates(
            subset=["txSig", "user", "marketIndex", "userLastCumulativeFunding"]
        )

        csv_buffer = io.BytesIO()
        df_to_write.to_csv(csv_buffer, index=False, compression="gzip")
        object_path = "{}/{}".format(userPrefix, date.strftime("%Y%m%d"))
        DESTINATION_BUCKET.put_object(
            Key=object_path,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv",
            ContentEncoding="gzip",
        )


def read_and_filter_file(file_key, read_credentials, file_date):
    event_types_to_read = []
    for event_type in EVENT_TYPES:
        try:
            with open("./out/{}.txt".format(event_type), "r") as file:
                last_processed_date = pd.to_datetime(file.read()).date()
                if file_date > last_processed_date:
                    event_types_to_read.append(event_type)
                else:
                    print("Date already processed for event {}".format(event_type))
        except:
            event_types_to_read.append(event_type)

    attempts = 0
    while attempts < 3:
        try:
            return pd.read_parquet(
                f"s3://drift-topledger/{file_key}",
                filters=[[("event_type", "=", y)] for y in event_types_to_read],
                storage_options={
                    "key": read_credentials["access_key"],
                    "secret": read_credentials["secret_key"],
                },
            )
        except Exception as e:
            attempts += 1
            if attempts < 3:
                print(f"Retrying file {file_key}...")
                time.sleep(3)
                continue
            else:
                print(f"Failed to read file {file_key}. Error: {e}")
                return pd.DataFrame()  # Return empty DataFrame in case of failure


def process_event_type(event, df_filtered, date, logs):
    try:
        with open("./out/{}.txt".format(event), "r") as file:
            last_processed_date = pd.to_datetime(file.read()).date()
            if date <= last_processed_date:
                print("Date already processed for event {}".format(event))
                return
    except:
        pass

    print(f"Processing event {event}")
    if event == "OrderActionRecord":
        process_trades(
            df_filtered[(df_filtered["event_type"] == "OrderActionRecord")], date, logs
        )
    elif event == "SettlePnlRecord":
        process_settle_pnl(
            df_filtered[df_filtered["event_type"] == "SettlePnlRecord"], date, logs
        )
    elif event == "DepositRecord":
        process_deposit(
            df_filtered[df_filtered["event_type"] == "DepositRecord"], date, logs
        )
    elif event == "InsuranceFundRecord":
        process_insurance_fund(
            df_filtered[df_filtered["event_type"] == "InsuranceFundRecord"], date, logs
        )
    elif event == "InsuranceFundStakeRecord":
        process_insurance_fund_stake(
            df_filtered[df_filtered["event_type"] == "InsuranceFundStakeRecord"],
            date,
            logs,
        )
    elif event == "LiquidationRecord":
        process_liquidation(
            df_filtered[df_filtered["event_type"] == "LiquidationRecord"], date, logs
        )
    elif event == "LPRecord":
        process_lp(df_filtered[df_filtered["event_type"] == "LPRecord"], date, logs)
    elif event == "FundingRateRecord":
        process_funding_rate(
            df_filtered[df_filtered["event_type"] == "FundingRateRecord"], date, logs
        )
    elif event == "FundingPaymentRecord":
        process_funding_payment(
            df_filtered[df_filtered["event_type"] == "FundingPaymentRecord"], date, logs
        )

    with open(f"./out/{event}.txt", "w") as file:
        file.write(date.strftime("%Y%m%d"))


def archive(start_date, end_date):
    frozen_credentials = session_default.get_credentials().get_frozen_credentials()
    read_credentials = {
        "access_key": frozen_credentials.access_key,
        "secret_key": frozen_credentials.secret_key,
    }
    s3 = boto3.client(
        "s3",
        aws_access_key_id=read_credentials["access_key"],
        aws_secret_access_key=read_credentials["secret_key"],
    )

    bucket_name = "drift-topledger"
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        StartAfter="drift/events/{}/*".format(
            (start_date - dt.timedelta(days=7)).strftime("%Y-%m-%d")
        ),
        Prefix="drift/events",
    )

    txns_response = s3.list_objects_v2(
        Bucket=bucket_name,
        StartAfter="drift/txns/{}/*".format(
            (start_date - dt.timedelta(days=7)).strftime("%Y-%m-%d")
        ),
        Prefix="drift/txns",
    )

    files_to_process = {}
    for obj in response["Contents"]:
        date = pd.to_datetime(obj["Key"].split("/")[2]).date()
        if (date >= start_date) and (date <= end_date):
            for event in EVENT_TYPES:
                try:
                    with open("./out/{}.txt".format(event), "r") as file:
                        last_processed_date = pd.to_datetime(file.read()).date()
                        if date > last_processed_date:
                            if date not in files_to_process:
                                files_to_process[date] = []
                            files_to_process[date].append(obj["Key"])
                            break
                except FileNotFoundError:
                    if date not in files_to_process:
                        files_to_process[date] = []
                    files_to_process[date].append(obj["Key"])
                    break

    txn_files_to_process = {}
    for obj in txns_response["Contents"]:
        if not obj["Key"] or not obj["Key"].endswith(".parquet"):
            continue
        date = pd.to_datetime(obj["Key"].split("/")[2]).date()
        if (date >= start_date) and (date <= end_date):
            for event in EVENT_TYPES:
                try:
                    with open("./out/{}.txt".format(event), "r") as file:
                        last_processed_date = pd.to_datetime(file.read()).date()
                        if date > last_processed_date:
                            if date not in txn_files_to_process:
                                txn_files_to_process[date] = []
                            txn_files_to_process[date].append(obj["Key"])
                            break
                except FileNotFoundError:
                    if date not in txn_files_to_process:
                        txn_files_to_process[date] = []
                    txn_files_to_process[date].append(obj["Key"])
                    break

    for (events_date, events_files), (txns_date, txns_files) in zip(
        files_to_process.items(), txn_files_to_process.items()
    ):
        if str(events_date) != str(txns_date):
            print(
                f"Events date {events_date} and txns date {txns_date} do not match"
            )
            raise ValueError("Events date and txns date do not match")

        print(f"Processing events date {events_date}")
        print(f"Processing txs date {txns_date}")
        print(f"Events Files to process: {events_files}")
        print(f"Txns Files to process: {txns_files}")
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_file = {
                executor.submit(
                    read_and_filter_file, file, read_credentials, events_date
                ): file
                for file in events_files
            }
            daily_dfs = []
            for future in as_completed(future_to_file):
                daily_df = future.result()
                if not daily_df.empty:
                    daily_dfs.append(daily_df)
        print("Read files")
        if len(daily_dfs) == 0:
            continue
        df_filtered = pd.concat(daily_dfs, ignore_index=True)
        condition = (df_filtered["event_type"] == "OrderActionRecord") & (
            df_filtered["args"].apply(
                lambda x: x.get("action") if isinstance(x, dict) else None
            )
            != "Fill"
        )
        df_filtered.drop(df_filtered[condition].index, inplace=True)
        if df_filtered.empty:
            continue
        print("Number of unique txs: {}".format(df_filtered["tx_id"].nunique()))
        logs = asyncio.run(
            get_logs_from_topledger(
                df_filtered["tx_id"].unique().tolist(), read_credentials, txns_files
            )
        )

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(
                    process_event_type, event, df_filtered, events_date, logs
                )
                for event in EVENT_TYPES
            ]

        for future in futures:
            future.result()

    print("All done!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Archive data between two dates.")
    parser.add_argument(
        "--start-date",
        type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(),
        help="Start date in YYYY-MM-DD format",
        default=dt.date.today() - dt.timedelta(days=1),
    )
    parser.add_argument(
        "--end-date",
        type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(),
        help="End date in YYYY-MM-DD format",
        default=dt.date.today() - dt.timedelta(days=1),
    )
    args = parser.parse_args()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(initialize_state())
    archive(args.start_date, args.end_date)

import traceback
import time
import datetime as dt
import pandas as pd

from anchorpy import Provider, Wallet
from solders.keypair import Keypair  # type: ignore
from solana.rpc.async_api import AsyncClient
from driftpy import drift_client
from tqdm import tqdm
from driftpy.constants.config import DRIFT_PROGRAM_ID
from driftpy.events.parse import parse_logs

IDL_URL = "https://raw.githubusercontent.com/drift-labs/protocol-v2/e86757b6e033ac27583755739fb28cedec976204/sdk/src/idl/drift.json"

KP = Keypair()  # random wallet
WALLET = Wallet(KP)
CONNECTION = AsyncClient("https://api.mainnet-beta.solana.com")
PROVIDER = Provider(CONNECTION, WALLET)
CLIENT = drift_client.DriftClient(CONNECTION, WALLET)

async def get_logs_from_topledger(sigs, read_credentials, files):
    def parse_logs_wrapper(program, logs):
        try:
            return parse_logs(program, logs)
        except Exception as e:
            error_message = (
                f"An error occurred with txSig: {sig} and {corresponding_logs}: {e}"
            )
            print(traceback.format_exc())
            return []

    start = time.time()

    def fetch_and_parse_logs(file):
        logs = pd.read_parquet(
            f"s3://drift-topledger/{file}",
            storage_options={
                "key": read_credentials["access_key"],
                "secret": read_credentials["secret_key"],
            },
        )
        logs["signatures"] = logs["signatures"].str[0]

        filtered_logs = logs[logs["signatures"].isin(sigs)]

        filtered_logs["parsed_logs"] = filtered_logs["log_messages"].apply(
            lambda x: parse_logs_wrapper(CLIENT.program, x)
        )

        return filtered_logs

    all_logs = [fetch_and_parse_logs(file) for file in files]

    filtered_logs = pd.concat(all_logs)

    logs_dict = filtered_logs.set_index("signatures")["parsed_logs"].to_dict()

    print(f"fetched & parsed logs from topledger in: {time.time() - start}s")

    parsed_logs = {}

    for sig in sigs:
        corresponding_logs = logs_dict.get(sig, None)
        if corresponding_logs is None:
            print(f"Logs not found for signature: {sig}")
            continue
        for event in corresponding_logs:
            parsed_logs.setdefault(sig, []).append(event)

    return parsed_logs

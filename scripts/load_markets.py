import asyncio
from typing import Tuple
from dataclasses import dataclass

from anchorpy import Idl, Program, Provider, Wallet

import requests
from solana.rpc.async_api import AsyncClient

from solders.errors import SerdeJSONError
from driftpy.constants.config import find_all_market_and_oracles, DRIFT_PROGRAM_ID
from driftpy.decode.utils import decode_name
from driftpy.types import PerpMarketAccount, SpotMarketAccount
from archive import RPC_URL


@dataclass
class SpotMarket:
    symbol: str
    marketIndex: int
    mintPrecision: int
    marketPrecision: int


@dataclass
class PerpMarket:
    symbol: str
    marketIndex: int
    baseAssetSymbol: str


IDL_URL = "https://raw.githubusercontent.com/drift-labs/protocol-v2/9d630045c170ef382c2355996cec0f9ecdb9fb8f/sdk/src/idl/drift.json"

async def load_markets() -> Tuple[list[PerpMarket], list[SpotMarket]]:
    connection = AsyncClient(RPC_URL)
    wallet = Wallet.dummy()
    provider = Provider(connection, wallet)

    # try:
    #     program = await Program.at(DRIFT_PROGRAM_ID, provider)
    #     print("loaded idl from chain")
    # except:
    response = requests.get(IDL_URL)
    if response.status_code == 200:
        idl = Idl.from_json(response.text)
        print("loaded idl from github")
        program = Program(idl, DRIFT_PROGRAM_ID, provider)
    else:
        print(f"failed to fetch idl: {response.status_code}")
        raise Exception

    (perp_market_accounts, spot_market_accounts, _) = await find_all_market_and_oracles(
        program, True
    )

    perp_markets: list[PerpMarket] = []
    for data_and_slot in perp_market_accounts:
        market: PerpMarketAccount = data_and_slot.data
        market_index = market.market_index
        market_name = decode_name(market.name)

        base_asset_symbol = market_name.split("-PERP")[0]

        perp_market = PerpMarket(
            symbol=market_name,
            marketIndex=market_index,
            baseAssetSymbol=base_asset_symbol,
        )

        perp_markets.append(perp_market)

    spot_markets: list[SpotMarket] = []
    for data_and_slot in spot_market_accounts:
        market: SpotMarketAccount = data_and_slot.data
        market_index = market.market_index
        market_name = decode_name(market.name)
        mint_precision = market.decimals
        market_precision = 10**mint_precision

        spot_market = SpotMarket(
            symbol=market_name,
            marketIndex=market_index,
            mintPrecision=mint_precision,
            marketPrecision=market_precision,
        )

        spot_markets.append(spot_market)

    return perp_markets, spot_markets


PERP_MARKETS: list[PerpMarket] = []
SPOT_MARKETS: list[SpotMarket] = []


async def initialize_state():
    global PERP_MARKETS, SPOT_MARKETS
    PERP_MARKETS, SPOT_MARKETS = await load_markets()
    PERP_MARKETS.sort(key=lambda x: x.marketIndex)
    SPOT_MARKETS.sort(key=lambda x: x.marketIndex)

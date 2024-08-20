import anchorpy.program.common as anchorpy
from driftpy.constants import (
    QUOTE_PRECISION,
    BASE_PRECISION,
    PRICE_PRECISION,
    SPOT_BALANCE_PRECISION,
    SPOT_CUMULATIVE_INTEREST_PRECISION,
    AMM_RESERVE_PRECISION,
    FUNDING_RATE_PRECISION,
)
from driftpy.types import OrderActionExplanation, MarketType, PositionDirection
from typing import TypedDict
import re


class EventMetadata(TypedDict):
    tx_sig: str
    slot: int


def to_camel_case(type) -> str:
    # Extract the part after the dot and before the parentheses
    extracted = re.search(r"\.(.*?)\(\)", str(type))

    if not extracted:
        return ""

    # Split the string by underscores and convert to camelCase
    words = re.split(r"(?<=[a-z])(?=[A-Z])", extracted.group(1))
    return words[0].lower() + "".join(word.capitalize() for word in words[1:])


def parse_event(event: anchorpy.Event, metadata: EventMetadata):
    from scripts.load_markets import SPOT_MARKETS

    raw_data = event.data
    data = vars(raw_data)
    match event.name:
        case "OrderActionRecord":
            if to_camel_case(data.get("market_type", "")) == "spot":
                base_precision = next(
                    filter(
                        lambda x: x.marketIndex == int(data["market_index"]),
                        SPOT_MARKETS,
                    ),
                    None,
                ).marketPrecision
            else:
                base_precision = BASE_PRECISION

            return {
                "fillerReward": (data.get("filler_reward", 0) or 0) / QUOTE_PRECISION,
                "baseAssetAmountFilled": (data.get("base_asset_amount_filled", 0) or 0)
                / base_precision,
                "quoteAssetAmountFilled": (
                    data.get("quote_asset_amount_filled", 0) or 0
                )
                / QUOTE_PRECISION,
                "takerFee": (data.get("taker_fee", 0) or 0) / QUOTE_PRECISION,
                "makerRebate": (data.get("maker_fee", 0) or 0) / QUOTE_PRECISION,
                "referrerReward": (data.get("referrer_reward", 0) or 0)
                / QUOTE_PRECISION,
                "quoteAssetAmountSurplus": (
                    data.get("quote_asset_amount_surplus", 0) or 0
                )
                / QUOTE_PRECISION,
                "takerOrderBaseAssetAmount": (
                    data.get("taker_order_base_asset_amount", 0) or 0
                )
                / base_precision,
                "takerOrderCumulativeBaseAssetAmountFilled": (
                    data.get("taker_order_cumulative_base_asset_amount_filled", 0) or 0
                )
                / base_precision,
                "takerOrderCumulativeQuoteAssetAmountFilled": (
                    data.get("taker_order_cumulative_quote_asset_amount_filled", 0) or 0
                )
                / QUOTE_PRECISION,
                "makerOrderBaseAssetAmount": (
                    data.get("maker_order_base_asset_amount", 0) or 0
                )
                / base_precision,
                "makerOrderCumulativeBaseAssetAmountFilled": (
                    data.get("maker_order_cumulative_base_asset_amount_filled", 0) or 0
                )
                / base_precision,
                "makerOrderCumulativeQuoteAssetAmountFilled": (
                    data.get("maker_order_cumulative_quote_asset_amount_filled", 0) or 0
                )
                / QUOTE_PRECISION,
                "oraclePrice": (data.get("oracle_price", 0) or 0) / PRICE_PRECISION,
                "makerFee": (data.get("maker_fee", 0) or 0) / QUOTE_PRECISION,
                "txSig": metadata["tx_sig"],
                "slot": metadata["slot"],
                "ts": data["ts"],
                "action": "fill",
                "actionExplanation": to_camel_case(data.get("action_explanation", "")),
                "marketIndex": int(data["market_index"]),
                "marketType": to_camel_case(data.get("market_type", "")),
                "filler": data["filler"],
                "fillRecordId": data["fill_record_id"],
                "taker": data["taker"],
                "takerOrderId": data["taker_order_id"],
                "takerOrderDirection": to_camel_case(
                    data.get("taker_order_direction", "")
                ),
                "maker": data["maker"],
                "makerOrderId": data["maker_order_id"],
                "makerOrderDirection": to_camel_case(
                    data.get("maker_order_direction", "")
                ),
                "spotFulfillmentMethodFee": (
                    data.get("spot_fulfillment_method_fee", 0) or 0
                )
                / QUOTE_PRECISION,
            }
        case "SettlePnlRecord":
            return {
                "pnl": (data.get("pnl", 0) or 0) / PRICE_PRECISION,
                "user": data["user"],
                "baseAssetAmount": (data.get("base_asset_amount", 0) or 0)
                / BASE_PRECISION,
                "quoteAssetAmountAfter": (data.get("quote_asset_amount_after", 0) or 0)
                / QUOTE_PRECISION,
                "quoteEntryAmount": (data.get("quote_entry_amount", 0) or 0)
                / QUOTE_PRECISION,
                "settlePrice": (data.get("settle_price", 0) or 0) / QUOTE_PRECISION,
                "txSig": metadata["tx_sig"],
                "slot": metadata["slot"],
                "ts": data["ts"],
                "marketIndex": int(data["market_index"]),
                "explanation": to_camel_case(data.get("explanation", "")),
            }
        case "DepositRecord":
            token_precision = next(
                filter(
                    lambda x: x.marketIndex == int(data["market_index"]), SPOT_MARKETS
                ),
                None,
            ).mintPrecision
            return {
                "amount": (data.get("amount", 0) or 0) / 10**token_precision,
                "oraclePrice": (data.get("oracle_price", 0) or 0) / PRICE_PRECISION,
                "marketDepositBalance": (data.get("market_deposit_balance", 0) or 0)
                / SPOT_BALANCE_PRECISION,
                "marketWithdrawBalance": (data.get("market_withdraw_balance", 0) or 0)
                / SPOT_BALANCE_PRECISION,
                "marketCumulativeDepositInterest": (
                    data.get("market_cumulative_deposit_interest", 0) or 0
                )
                / SPOT_CUMULATIVE_INTEREST_PRECISION,
                "marketCumulativeBorrowInterest": (
                    data.get("market_cumulative_borrow_interest", 0) or 0
                )
                / SPOT_CUMULATIVE_INTEREST_PRECISION,
                "totalDepositsAfter": (data.get("total_deposits_after", 0) or 0)
                / QUOTE_PRECISION,
                "totalWithdrawsAfter": (data.get("total_withdraws_after", 0) or 0)
                / QUOTE_PRECISION,
                "txSig": metadata["tx_sig"],
                "slot": metadata["slot"],
                "ts": data["ts"],
                "depositRecordId": data["deposit_record_id"],
                "userAuthority": data["user_authority"],
                "user": data["user"],
                "direction": to_camel_case(data.get("direction", "")),
                "marketIndex": int(data["market_index"]),
                "explanation": to_camel_case(data.get("explanation", "")),
            }
        case "InsuranceFundRecord":
            token_precision = next(
                filter(
                    lambda x: x.marketIndex == int(data["spot_market_index"]),
                    SPOT_MARKETS,
                ),
                None,
            ).mintPrecision
            return {
                "vaultAmountBefore": (data.get("vault_amount_before", 0) or 0)
                / 10**token_precision,
                "insuranceVaultAmountBefore": (
                    data.get("insurance_vault_amount_before", 0) or 0
                )
                / 10**token_precision,
                "totalIfSharesBefore": (data.get("total_if_shares_before", 0) or 0)
                / QUOTE_PRECISION,
                "totalIfSharesAfter": (data.get("total_if_shares_after", 0) or 0)
                / QUOTE_PRECISION,
                "amount": (data.get("amount", 0) or 0) / 10**token_precision,
                "ts": data["ts"],
                "txSig": metadata["tx_sig"],
                "slot": metadata["slot"],
                "spotMarketIndex": data["spot_market_index"],
                "perpMarketIndex": data["perp_market_index"],
                "userIfFactor": data["user_if_factor"],
                "totalIfFactor": data["total_if_factor"],
            }
        case "InsuranceFundStakeRecord":
            token_precision = next(
                filter(
                    lambda x: x.marketIndex == int(data["market_index"]), SPOT_MARKETS
                ),
                None,
            ).mintPrecision
            return {
                "amount": (data.get("amount", 0) or 0) / 10**token_precision,
                "userAuthority": data["user_authority"],
                "action": to_camel_case(data.get("action", "")),
                "ts": data["ts"],
                "txSig": metadata["tx_sig"],
                "slot": metadata["slot"],
                "marketIndex": data["market_index"],
                "ifSharesBefore": (data.get("if_shares_before", 0) or 0)
                / QUOTE_PRECISION,
                "userIfSharesBefore": (data.get("user_if_shares_before", 0) or 0)
                / QUOTE_PRECISION,
                "totalIfSharesBefore": (data.get("total_if_shares_before", 0) or 0)
                / QUOTE_PRECISION,
                "ifSharesAfter": (data.get("if_shares_after", 0) or 0)
                / QUOTE_PRECISION,
                "userIfSharesAfter": (data.get("user_if_shares_after", 0) or 0)
                / QUOTE_PRECISION,
                "totalIfSharesAfter": (data.get("total_if_shares_after", 0) or 0)
                / QUOTE_PRECISION,
                "insuranceVaultAmountBefore": (
                    data.get("insurance_vault_amount_before", 0) or 0
                )
                / 10**token_precision,
            }
        case "LiquidationRecord":
            liquidatePerp = vars(raw_data.liquidate_perp)
            liquidateSpot = vars(raw_data.liquidate_spot)
            liquidateBorrowForPerpPnl = vars(raw_data.liquidate_borrow_for_perp_pnl)
            liquidatePerpPnlForDeposit = vars(raw_data.liquidate_perp_pnl_for_deposit)
            perpBankruptcy = vars(raw_data.perp_bankruptcy)
            spotBankruptcy = vars(raw_data.spot_bankruptcy)

            token_precision = next(
                filter(
                    lambda x: x.marketIndex
                    == int(liquidateSpot["liability_market_index"]),
                    SPOT_MARKETS,
                ),
                None,
            ).mintPrecision
            spot_token_precision = next(
                filter(
                    lambda x: x.marketIndex == int(spotBankruptcy["market_index"]),
                    SPOT_MARKETS,
                ),
                None,
            ).mintPrecision
            return {
                "ts": data["ts"],
                "txSig": metadata["tx_sig"],
                "slot": metadata["slot"],
                "liquidationType": to_camel_case(data.get("liquidation_type", "")),
                "user": data["user"],
                "liquidator": data["liquidator"],
                "marginRequirement": (data["margin_requirement"] or 0)
                / QUOTE_PRECISION,
                "totalCollateral": (data["total_collateral"] or 0) / QUOTE_PRECISION,
                "marginFreed": (data["margin_freed"] or 0) / QUOTE_PRECISION,
                "liquidationId": data["liquidation_id"],
                "bankrupt": data["bankrupt"],
                "canceledOrderIds": data["canceled_order_ids"],
                "liquidatePerp_marketIndex": liquidatePerp["market_index"],
                "liquidatePerp_oraclePrice": (liquidatePerp.get("oracle_price", 0) or 0)
                / PRICE_PRECISION,
                "liquidatePerp_baseAssetAmount": (
                    liquidatePerp.get("base_asset_amount", 0) or 0
                )
                / BASE_PRECISION,
                "liquidatePerp_quoteAssetAmount": (
                    liquidatePerp.get("quote_asset_amount", 0) or 0
                )
                / QUOTE_PRECISION,
                "liquidatePerp_lpShares": (liquidatePerp.get("lp_shares", 0) or 0)
                / AMM_RESERVE_PRECISION,
                "liquidatePerp_fillRecordId": liquidatePerp["fill_record_id"],
                "liquidatePerp_userOrderId": liquidatePerp["user_order_id"],
                "liquidatePerp_liquidatorOrderId": liquidatePerp["liquidator_order_id"],
                "liquidatePerp_liquidatorFee": (
                    liquidatePerp.get("liquidator_fee", 0) or 0
                )
                / QUOTE_PRECISION,
                "liquidatePerp_ifFee": (liquidatePerp.get("if_fee", 0) or 0)
                / QUOTE_PRECISION,
                "liquidateSpot_assetMarketIndex": liquidateSpot["asset_market_index"],
                "liquidateSpot_assetPrice": (liquidateSpot.get("asset_price", 0) or 0)
                / PRICE_PRECISION,
                "liquidateSpot_assetTransfer": (
                    liquidateSpot.get("asset_transfer", 0) or 0
                )
                / 10**spot_token_precision,
                "liquidateSpot_liabilityMarketIndex": liquidateSpot[
                    "liability_market_index"
                ],
                "liquidateSpot_liabilityPrice": (
                    liquidateSpot.get("liability_price", 0) or 0
                )
                / PRICE_PRECISION,
                "liquidateSpot_liabilityTransfer": (
                    liquidateSpot.get("liability_transfer", 0) or 0
                )
                / 10**token_precision,
                "liquidateSpot_ifFee": (liquidateSpot["if_fee"] or 0)
                / 10**token_precision,
                "liquidateBorrowForPerpPnl_perpMarketIndex": liquidateBorrowForPerpPnl[
                    "perp_market_index"
                ],
                "liquidateBorrowForPerpPnl_marketOraclePrice": (
                    liquidateBorrowForPerpPnl.get("market_oracle_price", 0) or 0
                )
                / PRICE_PRECISION,
                "liquidateBorrowForPerpPnl_pnlTransfer": (
                    liquidateBorrowForPerpPnl.get("pnl_transfer", 0) or 0
                )
                / QUOTE_PRECISION,
                "liquidateBorrowForPerpPnl_liabilityMarketIndex": liquidateBorrowForPerpPnl[
                    "liability_market_index"
                ],
                "liquidateBorrowForPerpPnl_liabilityPrice": (
                    liquidateBorrowForPerpPnl.get("liability_price", 0) or 0
                )
                / PRICE_PRECISION,
                "liquidateBorrowForPerpPnl_liabilityTransfer": (
                    liquidateBorrowForPerpPnl.get("liability_transfer", 0) or 0
                )
                / QUOTE_PRECISION,
                "liquidatePerpPnlForDeposit_perpMarketIndex": liquidatePerpPnlForDeposit[
                    "perp_market_index"
                ],
                "liquidatePerpPnlForDeposit_marketOraclePrice": (
                    liquidatePerpPnlForDeposit.get("market_oracle_price", 0) or 0
                )
                / PRICE_PRECISION,
                "liquidatePerpPnlForDeposit_pnlTransfer": (
                    liquidatePerpPnlForDeposit.get("pnl_transfer", 0) or 0
                )
                / QUOTE_PRECISION,
                "liquidatePerpPnlForDeposit_assetMarketIndex": liquidatePerpPnlForDeposit[
                    "asset_market_index"
                ],
                "liquidatePerpPnlForDeposit_assetPrice": (
                    liquidatePerpPnlForDeposit.get("asset_price", 0) or 0
                )
                / PRICE_PRECISION,
                "liquidatePerpPnlForDeposit_assetTransfer": (
                    liquidatePerpPnlForDeposit.get("asset_transfer", 0) or 0
                )
                / BASE_PRECISION,
                "perpBankruptcy_marketIndex": perpBankruptcy["market_index"],
                "perpBankruptcy_pnl": (perpBankruptcy.get("pnl", 0) or 0)
                / QUOTE_PRECISION,
                "perpBankruptcy_ifPayment": (perpBankruptcy.get("if_payment", 0) or 0)
                / QUOTE_PRECISION,
                "perpBankruptcy_clawbackUser": perpBankruptcy["clawback_user"],
                "perpBankruptcy_clawbackUserPayment": (
                    perpBankruptcy.get("clawback_user_payment", 0) or 0
                )
                / QUOTE_PRECISION,
                "perpBankruptcy_cumulativeFundingRateDelta": (
                    perpBankruptcy.get("cumulative_funding_rate_delta", 0) or 0
                )
                / PRICE_PRECISION,
                "spotBankruptcy_marketIndex": spotBankruptcy["market_index"],
                "spotBankruptcy_borrowAmount": (
                    spotBankruptcy.get("borrow_amount", 0) or 0
                )
                / 10**spot_token_precision,
                "spotBankruptcy_ifPayment": (spotBankruptcy.get("if_payment", 0) or 0)
                / 10**spot_token_precision,
                "spotBankruptcy_cumulativeDepositInterestDelta": (
                    spotBankruptcy.get("cumulative_deposit_interest_delta", 0) or 0
                )
                / SPOT_CUMULATIVE_INTEREST_PRECISION,
            }
        case "LPRecord":
            return {
                "ts": data["ts"],
                "txSig": metadata["tx_sig"],
                "slot": metadata["slot"],
                "user": data["user"],
                "action": to_camel_case(data.get("action", "")),
                "nShares": (data["n_shares"] or 0) / AMM_RESERVE_PRECISION,
                "marketIndex": data["market_index"],
                "deltaBaseAssetAmount": (data["delta_base_asset_amount"] or 0)
                / BASE_PRECISION,
                "deltaQuoteAssetAmount": (data["delta_quote_asset_amount"] or 0)
                / QUOTE_PRECISION,
                "pnl": (data["pnl"] or 0) / QUOTE_PRECISION,
            }
        case "FundingRateRecord":
            return {
                "ts": data["ts"],
                "txSig": metadata["tx_sig"],
                "recordId": data["record_id"],
                "slot": metadata["slot"],
                "marketIndex": data["market_index"],
                "fundingRate": (data["funding_rate"] or 0) / 1e9,
                "fundingRateLong": (data["funding_rate_long"] or 0) / 1e9,
                "fundingRateShort": (data["funding_rate_short"] or 0) / 1e9,
                "cumulativeFundingRateLong": (data["cumulative_funding_rate_long"] or 0)
                / 1e9,
                "cumulativeFundingRateShort": (
                    data["cumulative_funding_rate_short"] or 0
                )
                / 1e9,
                "oraclePriceTwap": (data["oracle_price_twap"] or 0) / PRICE_PRECISION,
                "markPriceTwap": (data["mark_price_twap"] or 0) / PRICE_PRECISION,
                "periodRevenue": (data["period_revenue"] or 0) / QUOTE_PRECISION,
                "baseAssetAmountWithAmm": (data["base_asset_amount_with_amm"] or 0)
                / BASE_PRECISION,
                "baseAssetAmountWithUnsettledLp": (
                    data["base_asset_amount_with_unsettled_lp"] or 0
                )
                / BASE_PRECISION,
            }
        case "FundingPaymentRecord":
            return {
                "ts": data["ts"],
                "txSig": metadata["tx_sig"],
                "slot": metadata["slot"],
                "userAuthority": data["user_authority"],
                "user": data["user"],
                "marketIndex": data["market_index"],
                "fundingPayment": (data["funding_payment"] or 0) / QUOTE_PRECISION,
                "baseAssetAmount": (data["base_asset_amount"] or 0) / BASE_PRECISION,
                "userLastCumulativeFunding": (data["user_last_cumulative_funding"] or 0)
                / FUNDING_RATE_PRECISION,
                "ammCumulativeFundingLong": (data["amm_cumulative_funding_long"] or 0)
                / FUNDING_RATE_PRECISION,
                "ammCumulativeFundingShort": (data["amm_cumulative_funding_short"] or 0)
                / FUNDING_RATE_PRECISION,
            }

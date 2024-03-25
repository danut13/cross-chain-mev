"""Module for analyzing cross-chain arbitrages.

"""
import collections
import logging

from src.api_utilities.polygon_bridge import PolygonBridgeInteractor
from src.blockchains.ethereum import EthereumService
from src.blockchains.polygon import PolygonService
from src.config import get_config
from src.domain import CrossChainMevExtraction
from src.domain import PolygonBridgeInteraction

_ETH_TO_POLYGON_TOKEN_MAP = collections.defaultdict(lambda: [])
_ETH_TO_POLYGON_TOKEN_MAP['0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'] = [
    '0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619',
    '0xAe740d42E4ff0C5086b2b5b5d149eB2F9e1A754F'
]

_logger = logging.getLogger(__name__)
"""Logger for this module."""


class CrossChainArbitrage:
    """Class for analyzing cross-chain arbitrage

    """
    def __init__(self):
        """Initialize and construct the instance.

        """
        self.__ethereum_service = EthereumService(
            get_config()['URL']['ethereum'])
        self.__polygon_service = PolygonService(get_config()['URL']['polygon'])
        self.__polygon_bridge_interactor = PolygonBridgeInteractor()

    def analayze_cross_chain_arbitrage(
            self, cross_chain_mev_extractions: list[CrossChainMevExtraction]):
        """Analyze cross chain arbitrages. Find cyclcic arbitrages
        and the profits.

        """
        for extraction in cross_chain_mev_extractions:
            direction = extraction.direction
            try:
                if direction is PolygonBridgeInteraction.FROM_ETHEREUM:
                    self.__analyze_from_ethereum_arbitrage(extraction)
                elif direction is PolygonBridgeInteraction.TO_ETHEREUM:
                    self.__analyze_to_ethereum_arbitrage(extraction)
                extraction.ethereum_leg.gas_paid = \
                    self.__ethereum_service.get_transaction_gas_paid(
                        extraction.ethereum_leg.transaction_hash)
                extraction.polygon_leg.bridge_transaction_gas_paid = \
                    self.__polygon_service.get_transaction_gas_paid(
                        extraction.polygon_leg.bridge_transaction_hash
                    )
                if (extraction.polygon_leg.bridge_transaction_hash ==
                        extraction.polygon_leg.swap_transaction_hash):
                    extraction.polygon_leg.swap_transaction_gas_paid = \
                        extraction.polygon_leg.bridge_transaction_gas_paid
                else:
                    extraction.polygon_leg.swap_transaction_gas_paid = \
                        self.__polygon_service.get_transaction_gas_paid(
                            extraction.polygon_leg.swap_transaction_hash
                        )
            except Exception:
                _logger.warning('unexpected exception for '
                                f'{extraction}', exc_info=True)

    def __analyze_from_ethereum_arbitrage(self,
                                          extraction: CrossChainMevExtraction):
        ethereum_token_started_wtih = extraction.ethereum_leg.swaps[0].token_in
        polygon_token_ended_with = extraction.polygon_leg.swaps[-1].token_out
        expected_polygon_token_ended_with = \
            ([self.__polygon_bridge_interactor.get_polygon_mapped_token(
                ethereum_token_started_wtih)] +
                _ETH_TO_POLYGON_TOKEN_MAP[ethereum_token_started_wtih])
        if (polygon_token_ended_with in expected_polygon_token_ended_with):
            extraction.is_cyclic_arbitrage = True
            profit_amount = (extraction.polygon_leg.swaps[-1].amount_out -
                             extraction.ethereum_leg.swaps[0].amount_in)
            token_symbol, token_amount = \
                self.__ethereum_service.get_token_symbol_and_parsed_amount(
                    ethereum_token_started_wtih, profit_amount)
            extraction.profit_token_symbol = token_symbol
            extraction.profit_amount = token_amount

    def __analyze_to_ethereum_arbitrage(self,
                                        extraction: CrossChainMevExtraction):
        polygon_token_started_wtih = extraction.polygon_leg.swaps[0].token_in
        ethereum_token_ended_with = extraction.ethereum_leg.swaps[-1].token_out
        expected_polygon_started_with = \
            ([self.__polygon_bridge_interactor.get_polygon_mapped_token(
                ethereum_token_ended_with)] +
                _ETH_TO_POLYGON_TOKEN_MAP[ethereum_token_ended_with])
        if (polygon_token_started_wtih in expected_polygon_started_with):
            extraction.is_cyclic_arbitrage = True
            profit_amount = (extraction.ethereum_leg.swaps[-1].amount_out -
                             extraction.polygon_leg.swaps[0].amount_in)
            token_symbol, token_amount = \
                self.__ethereum_service.get_token_symbol_and_parsed_amount(
                    ethereum_token_ended_with, profit_amount)
            extraction.profit_token_symbol = token_symbol
            extraction.profit_amount = token_amount

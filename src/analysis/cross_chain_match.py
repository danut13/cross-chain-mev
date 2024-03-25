"""Module for matching cross-chain transactions.

"""
import dataclasses
import logging
import typing

import web3.types

from src.api_utilities.findblock import FindBlock
from src.api_utilities.polygon_bridge import PolygonBridgeInteractor
from src.api_utilities.polygon_bridge import PolygonBridgeInteractorError
from src.blockchains.ethereum import EthereumService
from src.blockchains.ethereum import EthereumServiceError
from src.blockchains.polygon import PolygonService
from src.blockchains.swap import Swap
from src.blockchains.swap import SwapProcessor
from src.blockchains.swap import SwapProcessorError
from src.config import get_config
from src.database.access import get_block_timestamp
from src.domain import ADDRESS_ZERO
from src.domain import CrossChainMevExtraction
from src.domain import CrossChainMevFailedExtraction
from src.domain import EthereumLeg
from src.domain import PolygonBridgeInteraction
from src.domain import PolygonLeg
from src.domain import Transaction
from src.exceptions import BaseError

_logger = logging.getLogger(__name__)
"""Logger for this module."""

_POLYGON_AVERAGE_BLOCK_TIME = 3
"""The Polygon block average block time in seconds."""
_NUMBER_OF_BLOCKS_IN_5_HOURS = int(18000 / _POLYGON_AVERAGE_BLOCK_TIME)
_NUMBER_OF_BLOCKS_IN_1_HOUR = int(3600 / _POLYGON_AVERAGE_BLOCK_TIME)


class CrossChainMatchError(BaseError):
    """Exception class for cross-chain match errors.

    """


class MatchedLogsError(CrossChainMatchError):
    """Exception class for matched logs errors.

    """
    def __init__(self, message: str, matched_logs: list[web3.types.EventData]):
        super().__init__(message)
        self.matched_logs = matched_logs


class CrossChainMatch:
    """Class for matching cross-chain transactions.

    """
    @dataclasses.dataclass
    class __FindMatchResponse:
        """The response received when trying to match a cross-chain
        Ethereum-Polygon transaction.

        """
        is_arbitrage: bool
        bridge_transaction_hash: str
        swap_transaction_hash: typing.Optional[str] = None
        swap: typing.Optional[list[Swap]] = None
        second_bridge_transaction_hash: typing.Optional[str] = None

    def __init__(self):
        """Initialize and construct the instance.

        """
        self.__ethereum_service = EthereumService(
            get_config()['URL']['ethereum'])
        self.__polygon_service = PolygonService(get_config()['URL']['polygon'])
        self.__find_block = FindBlock()
        self.__polygon_bridge_interactor = PolygonBridgeInteractor()
        self.__ethereum_swap_processor = SwapProcessor(self.__ethereum_service)
        self.__polygon_swap_processor = SwapProcessor(self.__polygon_service)

    def match_cross_chain_mev_transactions(
            self,
            block_to_cross_chain_mev_transactions: dict[int,
                                                        list[Transaction]]) \
            -> tuple[list[CrossChainMevExtraction],
                     list[CrossChainMevFailedExtraction]]:
        """Match the Ethereum cross-chain MEV transactions
        with the Polygon leg transaction(s).

        Parameters
        ----------
        block_to_cross_chain_mev_transactions : dict of int, list
            Block numbers to cross-chain MEV transactions.

        Returns
        -------
        list of CrossChainMevExtraction
            The list of cross-chain MEV extractions.

        Raises
        ------
        AnalysisError
            If something went wrong when matching the transaction.

        """
        cross_chain_mev_extractions: list[CrossChainMevExtraction] = []
        cross_chain_mev_failed: list[CrossChainMevFailedExtraction] = []
        for key in block_to_cross_chain_mev_transactions:

            block_timestamp = get_block_timestamp(key)
            for transaction in block_to_cross_chain_mev_transactions[key]:
                try:
                    ethereum_swap_info = \
                        self.__ethereum_swap_processor.process_transaction(
                            transaction.transaction_hash)
                    ethereum_searcher_eoa, ethereum_searcher_contract = \
                        self.__ethereum_service.get_transaction_from_and_to(
                            transaction.transaction_hash)
                    assert transaction.polygon_bridge_interaction \
                        is not PolygonBridgeInteraction.NONE
                    if transaction.polygon_bridge_interaction \
                            is PolygonBridgeInteraction.FROM_ETHEREUM:
                        cross_chain_mev_extraction = \
                            self.__match_from_ethereum(
                                transaction, ethereum_searcher_eoa,
                                ethereum_searcher_contract, ethereum_swap_info,
                                block_timestamp)
                    elif transaction.polygon_bridge_interaction \
                            is PolygonBridgeInteraction.TO_ETHEREUM:
                        cross_chain_mev_extraction = self.__match_to_ethereum(
                            transaction, ethereum_searcher_eoa,
                            ethereum_searcher_contract, ethereum_swap_info,
                            block_timestamp)
                    if (type(cross_chain_mev_extraction)
                            is CrossChainMevExtraction):
                        cross_chain_mev_extractions.append(
                            cross_chain_mev_extraction)
                    elif (type(cross_chain_mev_extraction)
                          is CrossChainMevFailedExtraction):
                        cross_chain_mev_failed.append(
                            cross_chain_mev_extraction)
                except EthereumServiceError:
                    _logger.warning(
                        'unable to match cross-chain MEV transaction. '
                        'No token and amount log found for '
                        f'{transaction.transaction_hash}')
                except MatchedLogsError as error:
                    _logger.warning(
                        'unable to match cross-chain MEV transaction. '
                        f'Logs found for {transaction.transaction_hash}: '
                        f'{error.matched_logs}')
                except SwapProcessorError as error:
                    _logger.warning(
                        'unable to match cross-chain MEV transaction. '
                        'Multiple unrelated swaps detected for '
                        f'{error.transaction_hash}')
                except PolygonBridgeInteractorError:
                    _logger.warning('unable to find child mapped token for ',
                                    f'{transaction.transaction_hash}')
                except Exception:
                    _logger.warning(
                        'unexpected exception for '
                        f'{transaction.transaction_hash}', exc_info=True)

        return cross_chain_mev_extractions, cross_chain_mev_failed

    def __match_from_ethereum(
            self, transaction: Transaction, searcher_eoa: str,
            searcher_contract: str, ethereum_swap_info: list[Swap],
            block_timestamp: int) \
            -> CrossChainMevExtraction | CrossChainMevFailedExtraction:
        etherem_token, receiver, amount = \
            self.__ethereum_service.get_from_ethereum_bridge_operation_information(  # noqa
                transaction.transaction_hash)
        ethereum_leg = EthereumLeg(etherem_token, transaction.transaction_hash,
                                   searcher_eoa, searcher_contract,
                                   ethereum_swap_info)
        polygon_token = \
            self.__polygon_bridge_interactor.get_polygon_mapped_token(
                etherem_token)
        cross_chain_mev_extraction = \
            self.__process_cross_chain_mev_transaction_from_ethereum(
                ethereum_leg, polygon_token, receiver, amount, block_timestamp)
        return cross_chain_mev_extraction

    def __match_to_ethereum(
            self, transaction: Transaction, searcher_eoa: str,
            searcher_contract: str, ethereum_swap_info: list[Swap],
            block_timestamp: int) \
            -> CrossChainMevExtraction | CrossChainMevFailedExtraction:
        etherem_token, sender, amount = \
            self.__ethereum_service.get_to_ethereum_bridge_operation_information(  # noqa
                transaction.transaction_hash)
        ethereum_leg = EthereumLeg(etherem_token, transaction.transaction_hash,
                                   searcher_eoa, searcher_contract,
                                   ethereum_swap_info)
        polygon_token = \
            self.__polygon_bridge_interactor.get_polygon_mapped_token(
                etherem_token)
        cross_chain_mev_extraction = \
            self.__process_cross_chain_mev_transaction_to_ethereum(
                ethereum_leg, polygon_token, sender, amount,
                block_timestamp,)
        return cross_chain_mev_extraction

    def __process_cross_chain_mev_transaction_from_ethereum(
            self, ethereum_leg: EthereumLeg, polygon_token: str, receiver: str,
            amount: int, ethereum_timestamp: int) \
            -> CrossChainMevExtraction | CrossChainMevFailedExtraction:
        polygon_block_number = \
            self.__find_block.find_polygon_block_after_timestamp(
                ethereum_timestamp)
        find_match_response = self.__find_from_ethereum_mev_transactions(
            polygon_block_number, polygon_token, receiver, amount)
        if find_match_response.is_arbitrage:
            polygon_searcher_eoa, polygon_searcher_contract = \
                self.__polygon_service.get_transaction_from_and_to(
                    find_match_response.swap_transaction_hash
                )
            polygon_leg = PolygonLeg(
                polygon_token,
                find_match_response.bridge_transaction_hash,
                find_match_response.swap_transaction_hash,  # type: ignore
                polygon_searcher_eoa,
                polygon_searcher_contract,
                find_match_response.swap)  # type: ignore
            return CrossChainMevExtraction(
                ethereum_leg, polygon_leg,
                PolygonBridgeInteraction.FROM_ETHEREUM, amount)
        else:
            return CrossChainMevFailedExtraction(
                ethereum_leg,
                find_match_response.bridge_transaction_hash,
                find_match_response.
                second_bridge_transaction_hash,  # type: ignore
                PolygonBridgeInteraction.FROM_ETHEREUM,
                amount)

    def __process_cross_chain_mev_transaction_to_ethereum(
            self, ethereum_leg: EthereumLeg, polygon_token: str, sender: str,
            amount: int, ethereum_timestamp: int):
        polygon_block_number = \
            self.__find_block.find_polygon_block_before_timestamp(
                ethereum_timestamp)
        find_match_response = self.__find_to_ethereum_mev_transactions(
            polygon_block_number, polygon_token, sender, amount)
        if find_match_response.is_arbitrage:
            polygon_searcher_eoa, polygon_searcher_contract = \
                self.__polygon_service.get_transaction_from_and_to(
                    find_match_response.swap_transaction_hash
                )
            polygon_leg = PolygonLeg(
                polygon_token,
                find_match_response.bridge_transaction_hash,
                find_match_response.swap_transaction_hash,  # type: ignore
                polygon_searcher_eoa,
                polygon_searcher_contract,
                find_match_response.swap)  # type: ignore
            return CrossChainMevExtraction(
                ethereum_leg, polygon_leg,
                PolygonBridgeInteraction.TO_ETHEREUM, amount)
        else:
            return CrossChainMevFailedExtraction(
                ethereum_leg,
                find_match_response.
                second_bridge_transaction_hash,  # type: ignore
                find_match_response.bridge_transaction_hash,
                PolygonBridgeInteraction.TO_ETHEREUM,
                amount)

    def __find_from_ethereum_mev_transactions(
            self, polygon_block_number: int, polygon_token: str, receiver: str,
            amount: int) -> __FindMatchResponse:
        polygon_bridge_interaction = PolygonBridgeInteraction.FROM_ETHEREUM
        bridge_transaction_log = self.__match_polygon_transactions(
            polygon_block_number,
            polygon_block_number + _NUMBER_OF_BLOCKS_IN_1_HOUR, polygon_token,
            amount, True, polygon_bridge_interaction, receiver)
        bridge_transaction_hash = bridge_transaction_log[
            'transactionHash'].hex()
        bridge_transaction_block_number = bridge_transaction_log['blockNumber']

        try:
            swap_transaction_log = self.__match_polygon_transactions(
                bridge_transaction_block_number,
                bridge_transaction_block_number + _NUMBER_OF_BLOCKS_IN_1_HOUR,
                polygon_token, amount, False, polygon_bridge_interaction,
                receiver)
            swap_transaction_hash = swap_transaction_log[
                'transactionHash'].hex()
            swap = self.__polygon_swap_processor.process_transaction(
                swap_transaction_hash)
            return self.__FindMatchResponse(True, bridge_transaction_hash,
                                            swap_transaction_hash, swap)
        except MatchedLogsError:
            bridge_back_transaction_log = self.__match_polygon_transactions(
                bridge_transaction_block_number,
                bridge_transaction_block_number + _NUMBER_OF_BLOCKS_IN_1_HOUR,
                polygon_token, amount, True,
                PolygonBridgeInteraction.TO_ETHEREUM, receiver)
            bridge_back_transaction_hash = bridge_back_transaction_log[
                'transactionHash'].hex()
            return self.__FindMatchResponse(
                False, bridge_transaction_hash,
                second_bridge_transaction_hash=bridge_back_transaction_hash)

    def __find_to_ethereum_mev_transactions(
            self, polygon_block_number: int, polygon_token: str, sender: str,
            amount: int) -> __FindMatchResponse:
        polygon_bridge_interaction = PolygonBridgeInteraction.TO_ETHEREUM
        bridge_transaction_log = self.__match_polygon_transactions(
            polygon_block_number - _NUMBER_OF_BLOCKS_IN_5_HOURS,
            polygon_block_number, polygon_token, amount, True,
            polygon_bridge_interaction, sender)
        bridge_transaction_hash = bridge_transaction_log[
            'transactionHash'].hex()
        bridge_transaction_block_number = bridge_transaction_log['blockNumber']

        swap = self.__polygon_swap_processor.process_transaction(
            bridge_transaction_hash)
        if swap:
            return self.__FindMatchResponse(True, bridge_transaction_hash,
                                            bridge_transaction_hash, swap)
        else:
            try:
                swap_transaction_log = self.__match_polygon_transactions(
                    bridge_transaction_block_number -
                    _NUMBER_OF_BLOCKS_IN_1_HOUR,
                    bridge_transaction_block_number, polygon_token, amount,
                    False, polygon_bridge_interaction, sender)
                swap_transaction_hash = swap_transaction_log[
                    'transactionHash'].hex()
                swap = self.__polygon_swap_processor.process_transaction(
                    swap_transaction_hash)
                return self.__FindMatchResponse(True, bridge_transaction_hash,
                                                swap_transaction_hash, swap)
            except MatchedLogsError:
                bridge_back_transaction_log = \
                    self.__match_polygon_transactions(
                        bridge_transaction_block_number -
                        _NUMBER_OF_BLOCKS_IN_1_HOUR,
                        bridge_transaction_block_number, polygon_token, amount,
                        True, PolygonBridgeInteraction.FROM_ETHEREUM, sender)
                bridge_back_transaction_hash = bridge_back_transaction_log[
                    'transactionHash'].hex()
                return self.__FindMatchResponse(
                    False, bridge_transaction_hash,
                    second_bridge_transaction_hash=bridge_back_transaction_hash
                )

    def __match_polygon_transactions(
            self, from_block: int, to_block: int, polygon_token: str,
            amount: int, is_bridge_transaction: bool,
            polygon_bridge_interaction: PolygonBridgeInteraction,
            sender_or_receiver: str) -> web3.types.EventData:
        transfer_logs = self.__polygon_service.get_transfer_logs(
            from_block, to_block, polygon_token)
        direction = ('from' if polygon_bridge_interaction
                     is PolygonBridgeInteraction.FROM_ETHEREUM else 'to')
        reverse_direction = ('to' if polygon_bridge_interaction
                             is PolygonBridgeInteraction.FROM_ETHEREUM else
                             'from')
        matched_logs = []
        for transfer_log in transfer_logs:
            if (transfer_log['args']['value'] <= (amount * 1.01)
                    and transfer_log['args']['value'] >= (amount * 0.99)):
                if is_bridge_transaction:
                    if (transfer_log['args'][direction] == ADDRESS_ZERO
                            and transfer_log['args'][reverse_direction]
                            == sender_or_receiver):
                        matched_logs.append(transfer_log)
                else:
                    if (transfer_log['args'][direction] == sender_or_receiver
                            and transfer_log['args'][reverse_direction]
                            != ADDRESS_ZERO):
                        matched_logs.append(transfer_log)
        if len(matched_logs) != 1:
            raise MatchedLogsError('unexpected number of matched logs',
                                   matched_logs)
        return matched_logs[0]

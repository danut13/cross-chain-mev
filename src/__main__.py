"""Cross-chain MEV extraction entry point.

"""
import dataclasses
import datetime
import json
import logging
import os
import typing

from src.analysis.cross_chain_arbitrage import CrossChainArbitrage
from src.analysis.cross_chain_match import CrossChainMatch
from src.analysis.cross_chain_mev import CrossChainMev
from src.api_utilities.fetch import DataFetcher
from src.config import get_config
from src.database import initialize_database
from src.logging import initialize_logging

_logger = logging.getLogger(__name__)
"""Logger for this module."""


def initialize_application():
    """Initialize the application requirements.

    """
    initialize_database()
    initialize_logging()


def fetch_data(block_number_start: int, block_number_end: int):
    """Fetch the required data.

    """
    data_fetcher = DataFetcher()
    data_fetcher.fetch_block_data(block_number_start, block_number_end)
    data_fetcher.fetch_mev_block_data(block_number_start, block_number_end)
    data_fetcher.fetch_and_process_traces(block_number_start, block_number_end)


def analyze_data_batch(cross_chain_mev: CrossChainMev,
                       cross_chain_match: CrossChainMatch,
                       cross_chain_arbitrage: CrossChainArbitrage,
                       block_number_start: int,
                       block_number_end: int) -> typing.Tuple[int, int, int]:
    """Analyze the data batch.


    """
    block_to_cross_chain_mev_transactions = \
        cross_chain_mev.find_cross_chain_mev_candidates(
            block_number_start, block_number_end)
    total_len = 0
    for key in block_to_cross_chain_mev_transactions:
        total_len += len(block_to_cross_chain_mev_transactions[key])
    cross_chain_mev_extractions, cross_chain_mev_failed = \
        cross_chain_match.match_cross_chain_mev_transactions(
            block_to_cross_chain_mev_transactions)
    cross_chain_arbitrage.analayze_cross_chain_arbitrage(
        cross_chain_mev_extractions)

    with open("extractions_result.json", "a") as json_file:
        for extraction in cross_chain_mev_extractions:
            json_file.write(
                json.dumps(dataclasses.asdict(extraction), indent=4))
            json_file.write(',\n')

    with open("extractions_failed.json", "a") as json_file:
        for failed_extraction in cross_chain_mev_failed:
            json_file.write(
                json.dumps(dataclasses.asdict(failed_extraction), indent=4))
            json_file.write(',\n')

    return total_len, len(cross_chain_mev_extractions), len(
        cross_chain_mev_failed)


def analyze_data(block_number_start: int, block_number_end: int):
    """Analyze the data batch.

    """
    cross_chain_mev = CrossChainMev()
    cross_chain_match = CrossChainMatch()
    cross_chain_arbitrage = CrossChainArbitrage()
    batch_size = 1000
    number_of_cross_chain_mev_candidates = 0
    number_of_fulfilled_cross_chain_mev = 0
    number_of_bridged_back_cross_chain_mev = 0
    with open("extractions_result.json", "w") as json_file:
        json_file.write('[\n')
    with open("extractions_failed.json", "w") as json_file:
        json_file.write('[\n')
    while block_number_start + batch_size < block_number_end:
        (number_of_cross_chain_mev_candidates_,
         number_of_fulfilled_cross_chain_mev_,
         number_of_bridged_back_cross_chain_mev_) = analyze_data_batch(
             cross_chain_mev, cross_chain_match, cross_chain_arbitrage,
             block_number_start, block_number_start + batch_size)
        number_of_cross_chain_mev_candidates += \
            number_of_cross_chain_mev_candidates_
        number_of_fulfilled_cross_chain_mev += \
            number_of_fulfilled_cross_chain_mev_
        number_of_bridged_back_cross_chain_mev += \
            number_of_bridged_back_cross_chain_mev_
        block_number_start += batch_size

    (number_of_cross_chain_mev_candidates_,
     number_of_fulfilled_cross_chain_mev_,
     number_of_bridged_back_cross_chain_mev_) = analyze_data_batch(
         cross_chain_mev, cross_chain_match, cross_chain_arbitrage,
         block_number_start, block_number_end)
    number_of_cross_chain_mev_candidates += \
        number_of_cross_chain_mev_candidates_
    number_of_fulfilled_cross_chain_mev += \
        number_of_fulfilled_cross_chain_mev_
    number_of_bridged_back_cross_chain_mev += \
        number_of_bridged_back_cross_chain_mev_

    print('number of potential cross-chain MEV txs: '
          f'{number_of_cross_chain_mev_candidates}')
    print('number of fulfilled cross-chain MEV extractions: '
          f'{number_of_fulfilled_cross_chain_mev}')
    print('number of bridged back cross-chain MEV extractions: '
          f'{number_of_bridged_back_cross_chain_mev}')

    with open("extractions_result.json", "rb+") as json_file:
        if number_of_fulfilled_cross_chain_mev > 0:
            json_file.seek(-2, os.SEEK_END)
            json_file.truncate()
    with open("extractions_failed.json", "rb+") as json_file:
        if number_of_bridged_back_cross_chain_mev > 0:
            json_file.seek(-2, os.SEEK_END)
            json_file.truncate()

    with open("extractions_result.json", "a") as json_file:
        json_file.write(']')

    with open("extractions_failed.json", "a") as json_file:
        json_file.write(']')


if __name__ == "__main__":
    initialize_application()
    config = get_config()
    _logger.info(
        f'===========NEW RUN STARTED ON {datetime.datetime.now()}===========')
    block_number_start = int(config['Block_number']['start'])
    block_number_end = int(config['Block_number']['end'])
    try:
        fetch_data(block_number_start, block_number_end)
        analyze_data(block_number_start, block_number_end)
    except Exception:
        _logger.error('error', exc_info=True)
    _logger.info(
        f'===========RUN FINISHED ON {datetime.datetime.now()}===========')

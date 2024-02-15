"""Module for the CLI which manages the saved data.

"""
import argparse
import logging

from src.database import initialize_database
from src.database.access import delete_block_data
from src.database.access import get_all_blocks
from src.logging import initialize_logging

_logger = logging.getLogger(__name__)
"""Logger for this module."""


def view_data():
    """View which block data is saved.

    """
    blocks = sorted(get_all_blocks())
    blocks_len = len(blocks)
    if blocks_len == 0:
        print('No blocks are saved.')
        return
    print(f'There are {blocks_len} blocks saved')
    if len(blocks) == 1:
        print(f'{blocks[0]}')
        return
    block_number_start = blocks[0]
    block_number_end = blocks[0]
    for block_number in blocks[1:]:
        if block_number == block_number_end + 1:
            block_number_end = block_number
        else:
            _print_blocks(block_number_start, block_number_end)
            block_number_start = block_number_end = block_number
    _print_blocks(block_number_start, block_number_end)


def _print_blocks(block_number_start: int, block_number_end: int):
    if block_number_start == block_number_end:
        print(block_number_start)
    else:
        number_of_blocks = block_number_end - block_number_start + 1
        print(f'from {block_number_start} to {block_number_end} '
              f'{number_of_blocks} blocks')


def delete_data(block_number_start: int, block_number_end: int):
    """Delete block data from the start block number
    to the end block number.

    Parameters
    ----------
    block_number_start : int
        The number of the block to start the deletion from.
    block_number_end : int
        The number of the block to end the deletion at.

    """
    if block_number_start > block_number_end:
        print('block_number_start has to be lower or equal '
              'than block_number_end')
        return
    number_of_deleted_blocks = delete_block_data(block_number_start,
                                                 block_number_end)
    print(f'{number_of_deleted_blocks} blocks have been deleted.')


def main():
    parser = argparse.ArgumentParser(description="Database Management CLI")
    parser.add_argument('--blocks', action='store_true',
                        help='View which block data is saved')
    parser.add_argument("--delete", nargs=2, metavar=('start', 'end'),
                        type=int,
                        help='Delete the blocks saved from start to end')

    args = parser.parse_args()

    if args.blocks:
        view_data()
    elif args.delete:
        block_number_start, block_number_end = args.delete
        delete_data(block_number_start, block_number_end)
    else:
        print('No action specified. Use --blocks or --delete')


if __name__ == "__main__":
    initialize_database()
    initialize_logging()
    try:
        main()
    except Exception:
        _logger.error('error when using the CLI', exc_info=True)

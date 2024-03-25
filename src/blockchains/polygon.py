"""Module for interacting with Polygon.

"""
import web3.middleware
import web3.types

from src.blockchains.ethereum import TRANSFER_EVENT_ABI
from src.blockchains.ethereum import EthereumService


class PolygonService(EthereumService):
    """Polygon-specific blockchain service.

    """
    def __init__(self, rpc_url: str):
        """Initialze the instance.

        """
        super().__init__(rpc_url)
        self._w3.middleware_onion.inject(web3.middleware.geth_poa_middleware,
                                         layer=0)

    def get_transfer_logs(self, from_block: int, to_block: int,
                          token_address: str) -> list[web3.types.EventData]:
        """Get the transfer logs.

        Parameters
        ----------
        from_block : int
            The block number to query the logs from.
        to_block : int
            The block number to query the logs to.

        Returns
        -------
        list of web3.types.EventData
            The list of logs.

        """
        logs = []
        max_block_range = 600
        start_block = from_block
        while start_block + max_block_range < to_block:
            contract = self._w3.eth.contract(
                address=web3.Web3.to_checksum_address(token_address),
                abi=TRANSFER_EVENT_ABI)
            logs += contract.events.Transfer().get_logs(  # type: ignore
                fromBlock=start_block,
                toBlock=start_block + max_block_range - 1)
            start_block += max_block_range
        contract = self._w3.eth.contract(
            address=web3.Web3.to_checksum_address(token_address),
            abi=TRANSFER_EVENT_ABI)
        logs += contract.events.Transfer().get_logs(  # type: ignore
            fromBlock=start_block, toBlock=to_block)
        return logs

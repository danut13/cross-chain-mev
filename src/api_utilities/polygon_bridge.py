"""Module for interacting with the Polygon bridge API.

"""
import requests
import web3

from src.exceptions import BaseError

_TOKEN_MAPPING_API = ('https://api-polygon-tokens.polygon.technology/'
                      'tokenlists/mapped.tokenlist.json')


class PolygonBridgeInteractorError(BaseError):
    """Exception class for polygon bridge interactor errors.

    """


class PolygonBridgeInteractor:
    """Polygon bridge interactor.

    """
    def __init__(self):
        """Initialize and construct the instance.

        """
        response = requests.get(_TOKEN_MAPPING_API)
        self.__token_mapping = response.json()['tokens']

    def get_polygon_mapped_token(self, ethereum_token: str) -> str:
        """Get the child token mapped to the ethereum root token.

        Parameters
        ----------
        ethereum_token : str
            The address of the Ethereum token.

        """
        for token_ in self.__token_mapping:
            if token_['originTokenAddress'].lower() \
                    == ethereum_token.lower():
                for token__ in token_['wrappedTokens']:
                    if token__['chainId'] == 137:
                        return web3.Web3.to_checksum_address(
                            token__['wrappedTokenAddress'])
        raise PolygonBridgeInteractorError(
            f'no mapped token found for {ethereum_token}')

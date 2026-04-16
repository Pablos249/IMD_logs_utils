"""Module for HTTP login and REST API interaction"""

import requests


class APIClient:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()
        self.authenticated = False

    def login(self, username: str, password: str) -> bool:
        # TODO: implement login logic
        raise NotImplementedError

    def get_sessions(self, device_id: str):
        # TODO: fetch charging sessions for given device
        raise NotImplementedError

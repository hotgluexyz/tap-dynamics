#!/usr/bin/env python3

import sys
import json
from datetime import datetime, timedelta

import requests
import singer
from singer import metadata
from odata import ODataService

from tap_dynamics.discover import discover
from tap_dynamics.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "client_id",
    "client_secret",
    "redirect_uri",
    "refresh_token",
]


def do_discover(service):
    LOGGER.info("Testing authentication")
    try:
        pass  ## TODO: test authentication
    except:
        raise Exception("Error testing Dynamics authentication")

    LOGGER.info("Starting discover")
    catalog = discover(service)
    return catalog


class DynamicsAuth(requests.auth.AuthBase):
    def __init__(self, parsed_args):
        self.__config = parsed_args.config
        self.__config_path = parsed_args.config_path
        self.__resource = "https://{}.crm.dynamics.com".format(parsed_args.config["org"])
        self.__client_id = parsed_args.config["client_id"]
        self.__client_secret = parsed_args.config["client_secret"]
        self.__redirect_uri = parsed_args.config["redirect_uri"]
        self.__refresh_token = parsed_args.config["refresh_token"]

        self.__session = requests.Session()
        self.__access_token = None
        self.__expires_at = None

    def ensure_access_token(self):
        if self.__access_token is None or self.__expires_at <= datetime.utcnow():
            response = self.__session.post(
                "https://login.microsoftonline.com/common/oauth2/token",
                data={
                    "client_id": self.__client_id,
                    "client_secret": self.__client_secret,
                    "redirect_uri": self.__redirect_uri,
                    "refresh_token": self.__refresh_token,
                    "grant_type": "refresh_token",
                    "resource": self.__resource,
                },
            )

            if response.status_code != 200:
                raise Exception(response.text)

            data = response.json()

            self.__access_token = data["access_token"]
            self.__config["refresh_token"] = data["refresh_token"]
            self.__config["expires_in"] = data["expires_in"]
            self.__config["access_token"] = data["access_token"]

            with open(self.__config_path, "w") as outfile:
                json.dump(self.__config, outfile, indent=4)

            self.__expires_at = datetime.utcnow() + timedelta(
                seconds=int(data["expires_in"]) - 10
            )  # pad by 10 seconds for clock drift

    def __call__(self, r):
        self.ensure_access_token()
        r.headers["Authorization"] = "Bearer {}".format(self.__access_token)
        return r


@singer.utils.handle_top_exception(LOGGER)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    url = "https://{}.crm.dynamics.com/api/data/v9.0/".format(parsed_args.config["org"])
    auth = DynamicsAuth(parsed_args)
    service = ODataService(
        url, reflect_entities=True, auth=auth 
    )
    catalog = parsed_args.catalog or do_discover(service)
    if parsed_args.discover:
        json.dump(catalog.to_dict(), sys.stdout, indent=2)

    else:
        sync(
            service,
            catalog,
            parsed_args.state,
            parsed_args.config["start_date"],
        )


if __name__ == "__main__":
    main()
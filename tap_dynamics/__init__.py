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
    # "start_date",
    "client_id",
    "client_secret",
    # "redirect_uri",
    # "refresh_token",
]


def do_discover(service, get_lookup_tables):
    LOGGER.info("Testing authentication")
    try:
        pass  ## TODO: test authentication
    except:
        raise Exception("Error testing Dynamics authentication")

    LOGGER.info("Starting discover")
    catalog = discover(service, get_lookup_tables)
    return catalog


class DynamicsAuth(requests.auth.AuthBase):
    def __init__(self, parsed_args, url, auth_url):
        self.__config = parsed_args.config
        self.__config_path = parsed_args.config_path
        self.__resource = url
        self.__client_id = parsed_args.config["client_id"]
        self.__client_secret = parsed_args.config["client_secret"]

        self.__redirect_uri = parsed_args.config.get("redirect_uri", "https://hotglue.xyz/callback")

        self.__refresh_token = parsed_args.config.get("refresh_token", "")

        self.__auth_url = auth_url
        self.__grant_type = parsed_args.config.get("grant_type", "refresh_token")
            
        self.__username = parsed_args.config["username"]
        self.__password = parsed_args.config["password"]

        self.__session = requests.Session()
        self.__access_token = None
        self.__expires_at = None

    def ensure_access_token(self):
        if self.__access_token is None or self.__expires_at <= datetime.utcnow():

            data = {
                    "client_id": self.__client_id,
                    "client_secret": self.__client_secret,
                    "redirect_uri": self.__redirect_uri,
                    "grant_type": self.__grant_type,
                    "resource": self.__resource,
                }
            if self.__grant_type == "refresh_token":
                data["refresh_token"] = self.__refresh_token
            
            if self.__password and self.__username:
                data["password"] = self.__password
                data["username"] = self.__username

            response = self.__session.post(
                self.__auth_url,
                data=data,
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
   
    if parsed_args.config.get('full_url'):
        url = parsed_args.config['full_url']
    else:
        url = "https://{}.crm.dynamics.com".format(parsed_args.config["org"])

    if parsed_args.config.get('auth_url'):
        auth_url = parsed_args.config['auth_url']
    else: 
        auth_url = "https://login.microsoftonline.com/common/oauth2/token"


    auth = DynamicsAuth(parsed_args, url, auth_url)
    session = requests.Session()
    session.headers.update({"Prefer": 'odata.include-annotations="*"'})

    if  "/api/data/" not in url:
        service_url = url + "/api/data/v9.0/"
    else:
        service_url = url

    service = ODataService(
        service_url,
        reflect_entities=True,
        auth=auth,
        session=session,
       #  quiet_progress=True, # this is a python-odata only flag, not the library we use
    )
    get_lookup_tables = parsed_args.config.get("get_lookup_tables", False)
    catalog = parsed_args.catalog or do_discover(service, get_lookup_tables)
    if parsed_args.discover:
        json.dump(catalog.to_dict(), sys.stdout, indent=2)

    else:
        start_date = parsed_args.config.get("start_date", "2017-09-10")
        LOGGER.info(
            "Start date is: {}".format(start_date)
        )
        sync(
            service,
            catalog,
            parsed_args.state,
            start_date,
        )


if __name__ == "__main__":
    main()
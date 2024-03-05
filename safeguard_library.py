#!/bin/env python3

from pysafeguard import *
from urllib.parse import urlparse
import os
import json

from cs_logging import (logmsg, logerr)
class Safeguard:
    """
    This class constructor allows us to setup safeguard class variables.
    """

    def __init__(self, is_bde=False):
        """
        Construct a new Safeguard object
        """
        self.hostname = None
        self.ca_file = None
        self.user_cert_file = None
        self.user_key_file = None
        self.prod = True if os.getenv('CS_PROD') == "P" else False
        self.is_bde = is_bde
        self.connection = None
        self._connect()

    def _get_hostname(self):
        """
        parse the hostname out of the url environment variable
        """
        url = os.getenv('CS_SG_DEV')
        if self.prod:
            url = os.getenv('CS_SG')

        url_parts = urlparse(url)
        self.hostname = url_parts.netloc

    def _get_cert_file_details(self):
        """
        populate the cert variables based on if this is prod and if this for bde
        """
        self.ca_file = os.getenv('CS_CERT_LOC') + os.getenv('CS_CERT_CA')
        if self.prod and self.is_bde:
            self.user_cert_file = os.getenv('CS_CERT_LOC') + os.getenv('CS_CERT_BDH') + ".pem"
            self.user_key_file = os.getenv('CS_CERT_LOC') + os.getenv('CS_CERT_BDH') + ".key.pem"
        elif self.is_bde:
            self.user_cert_file = os.getenv('CS_CERT_LOC') + os.getenv('CS_CERT_BDH_DEV') + ".pem"
            self.user_key_file = os.getenv('CS_CERT_LOC') + os.getenv('CS_CERT_BDH_DEV') + ".key.pem"
        elif self.prod:
            self.user_cert_file = os.getenv('CS_CERT_LOC') + os.getenv('CS_CERT_DAI') + ".pem"
            self.user_key_file = os.getenv('CS_CERT_LOC') + os.getenv('CS_CERT_DAI') + ".key.pem"
        else:
            self.user_cert_file = os.getenv('CS_CERT_LOC') + os.getenv('CS_CERT_DAI_DEV') + ".pem"
            self.user_key_file = os.getenv('CS_CERT_LOC') + os.getenv('CS_CERT_DAI_DEV') + ".key.pem"

    def _connect(self):
        """
        Login to safeguard server
        """
        # populate fields needed to interact with the safeguard api
        self._get_hostname()
        self._get_cert_file_details()

        # connect to safeguard
        logmsg('Connecting to Safeguard {}'.format(self.hostname))
        self.connection = PySafeguardConnection(self.hostname, self.ca_file)

        # login to safeguard
        logmsg('Logging into safeguard')
        self.connection.connect_certificate(self.user_cert_file, self.user_key_file)

    def get_a2a_id(self):
        """
        Get the a2a_id
        """
        result = self.connection.invoke(HttpMethods.GET, Services.CORE, endpoint='A2ARegistrations', cert=(self.user_cert_file,self.user_key_file))
        result_json = result.json()
        if result_json:
            result_dict = result_json[0]
            return result_dict["Id"]
        else:
            logerr('Could not fetch A2A id.')
            return

    def get_api_key(self, a2a_id, username, system_name=None):
        """
        Get the api key for the account

        :param a2a_id: The a2a id of the account logging into safeguard
        :param username: The username/account password you need to fetch
        :param system_name: Optional input that contains the sytem name the accout is for ie. TSSIDM
        :return: returns the password fetched from safeguard
        """
        api_key = None
        accounts_list_result = self.connection.invoke(HttpMethods.GET, Services.CORE, endpoint='A2ARegistrations/{}/RetrievableAccounts'.format(a2a_id), cert=(self.user_cert_file,self.user_key_file))
        accounts_json = accounts_list_result.json()
        if accounts_json:
            # if the system name is empty check if there is more than one entry for the account name and log an error
            # and return if both conditions are met
            account_name_list = [acct_name["AccountName"].lower() for acct_name in accounts_json]
            if system_name == None and account_name_list.count(username.lower()) > 1:
                logerr('The account name {} appears in safeguard multiple times please specify a system name'.format(username))
                return

            # loop through the list of accounts to find the api key of the account you need the password for
            for account in accounts_json:
                if system_name:
                    if account["AssetName"].lower() == system_name.lower() and account["AccountName"].lower() == username.lower():
                        api_key = account["ApiKey"]
                        break
                else:
                    if account["AccountName"].lower() == username.lower():
                        api_key = account["ApiKey"]
                        break
        return api_key

    def get_account_id(self, username, system_name=None):
        """
        Get the unique id for the account

        :param username: The username/account password you need to fetch
        :param system_name: Optional input that contains the sytem name the accout is for ie. TSSIDM
        :return: returns the account id fetched from safeguard
        """
        account_id = None
        accounts_list_result = self.connection.invoke(HttpMethods.GET, Services.CORE, endpoint='AssetAccounts', cert=(self.user_cert_file,self.user_key_file))
        accounts_json = accounts_list_result.json()
        if accounts_json:
            # if the system name is empty check if there is more than one entry for the account name and log an error
            # and return if both conditions are met
            account_name_list = [acct_name["Name"].lower() for acct_name in accounts_json]
            if system_name == None and account_name_list.count(username.lower()) > 1:
                logerr('The account name {} appears in safeguard multiple times please specify a system name'.format(username))
                return

            # loop through the list of accounts to find the api key of the account you need the password for
            for account in accounts_json:
                if system_name:
                    if account["Asset"]["Name"].lower() == system_name.lower() and account["Name"].lower() == username.lower():
                        account_id = account["Id"]
                        break
                else:
                    if account["Name"].lower() == username.lower():
                        account_id = account["Id"]
                        break
        return account_id


    def get_password(self, username, system_name=None):
        """
        Fetches the password from safeguard

        :param username: The username/account password you need to fetch
        :param system_name: Optional input that contains the sytem name the accout is for ie. TSSIDM
        :return: returns the password fetched from safeguard
        """
        # get A2A id
        logmsg('Getting A2A id')
        a2a_id = self.get_a2a_id()
        if not a2a_id:
            return
        
        logmsg('Getting api key')
        api_key = self.get_api_key(a2a_id, username, system_name)

        logmsg('Getting Password')
        password = None
        # if we were able to get the api key from safeguard get the password otherwise throw an error.
        if api_key != None:
            password = self.connection.a2a_get_credential(self.hostname, api_key, self.user_cert_file, self.user_key_file, verify=self.ca_file)
            return password
        else:
            logerr('Api key required.  Could not fetch password')
            return


    def update_password(self, username, password, system_name=None):
        """
        Updates the password in safeguard for the specified user

        :param username: The username/account password you need to fetch
        :param password: The password to update in safeguard
        :param system_name: Optional input that contains the sytem name the accout is for ie. TSSIDM
        :return: returns True if the update was successful and False if the update fails
        """
        # get account id
        logmsg('Getting {} unique account id'.format(username))
        account_id = self.get_account_id(username, system_name)

        if not account_id:
            logmsg('Unable to update password')
            return False

        # update password
        results = self.connection.invoke(HttpMethods.PUT, Services.CORE, endpoint='AssetAccounts/{}/Password'.format(account_id), body=password, cert=(self.user_cert_file,self.user_key_file))

        if results.status_code == 204:
            logmsg('Successfully updated password')
            return True
        else:
            logmsg('Failed to update password: {}'.format(results.message))
            return False

#!/bin/env python3

import math
# Contains methods used to build and parse XML
import xml.etree.ElementTree as ET
import requests  # Contains methods used to make HTTP requests
import sys
import os
import pprint
import re
import json
import datetime
import time

# The following packages are used to build a multi-part/mixed request.
# They are contained in the 'requests' library.
from requests.packages.urllib3.fields import RequestField
from requests.packages.urllib3.filepost import encode_multipart_formdata
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

from cs_logging import logmsg
import cs_crypt
import cs_properties as prop
import cs_directory_services as dir_svcs
import cs_db
from cs_environment import current_user_is_production


api_version = os.environ['CS_TABLEAU_API_VER'] # Environment variable
api_version_number = api_version.split('/')[2]

class Tableau:
    """
    This class constructor allows us to setup tableau class variables.
    """

    def __init__(self):
        """
        Contruct a new Tableau object.
        """
        # The namespace for the REST API is 'http://tableausoftware.com/api' for Tableau Server 9.0 or 'http://tableau.com/api' for Tableau Server 9.1 or later
        self.xmlns = {'t': 'http://tableau.com/api'}
        self.server = ''
        self.server_name = None
        self.token = None
        self.user = ''
        self.passwd = ''
        self.site_id = ''
        self.site_name = None
        self.my_user_id = ''
        self.site_content_url = None
        self.session = requests.Session() # for connection pooling
        self.session.verify = False

    def __del__(self):
        """
        Sign out of Tableau and release any other allocated resources.
        """
        if self.token is not None:
            self.sign_out()


    @staticmethod
    def _get_group_server_list():
        server_list = ['https://tableauxta.dev.schwab.com', 'https://tableau.dev.schwab.com']
        if os.getenv('CS_PROD') == "P" and "svc" in os.getenv('LOGNAME')[:3]:
            server_list = server_list + ['https://tableauxta.schwab.com', 'https://tableau.schwab.com']
        if ('CS_PROD') == "T":
            server_list = server_list + ['https://tableau-pp.schwab.com']
        logmsg("List of possible servers:")
        for server_listing in server_list:
            logmsg(server_listing)
        return server_list


    @staticmethod
    def _get_server_sites():
        server_sites = {}
        server_sites['https://tableau.dev.schwab.com'] = [ "" ]
        server_sites['https://tableauxta.dev.schwab.com'] = ["BranchAnalytics"]
        server_sites['https://tableau-pp.schwab.com'] = [ "" ]
        if os.getenv('CS_PROD') == "P" and "svc" in os.getenv('LOGNAME')[:3]:
            server_sites['https://tableau.schwab.com'] = [ "" ]
            server_sites['https://tableauxta.schwab.com'] = ["BranchAnalytics"]
        return server_sites

    def _make_multipart(self, parts):
        """
        Creates one "chunk" for a multi-part upload.

        'parts' is a dictionary that provides key-value pairs of the format name: (filename, body, content_type).

        Returns the post body and the content type string.

        For more information, see this post:
        http://stackoverflow.com/questions/26299889/how-to-post-multipart-list-of-json-xml-files-using-python-requests
        """

        mime_multipart_parts = []

        for name, (filename, blob, content_type) in parts.items():
            multipart_part = RequestField(name=name, data=blob, filename=filename)
            multipart_part.make_multipart(content_type=content_type)
            mime_multipart_parts.append(multipart_part)

        post_body, content_type = encode_multipart_formdata(mime_multipart_parts)
        content_type = ''.join(('multipart/mixed',) + content_type.partition(';')[1:])
        return post_body, content_type


    def _handle_error(self, server_response):
        """
        Parses an error response for the error subcode and detail message
        and then displays them.

        Returns the error code and error message.
        """
        xml_response = ET.fromstring(server_response.text)
        error_code = xml_response.find('t:error', namespaces=self.xmlns).attrib.get('code')
        error_detail = xml_response.find('.//t:detail', namespaces=self.xmlns).text
        logmsg("ERROR: {0}: {1}".format(error_code, error_detail))
        return error_code, error_detail


    def sign_in_user(self, server, site=""):
        win_sec_filename=os.path.join(os.getenv("HOME") + '/.windows.sec')
        if ( not os.path.isfile(win_sec_filename) ):
            logmsg('ERROR: {} : No file found'.format(win_sec_filename))
            return
        username, win_pass_enc = cs_crypt.get_sec_file_user_pass(win_sec_filename)
        win_pass = cs_crypt.decrypt_AES('MISd1g1tal!', win_pass_enc)
        if (len(username) < 2) or (len(win_pass) < 2):
            logmsg('ERROR: {} : No username or password found'.format(win_sec_filename))
            return 
        return self.sign_in(server, username, win_pass, site)


    def sign_in_site_admin(self, server, site=""):
        sec_filename = "/NAS/mis/auth/dev/ad/svc.tableau.batch.dv.sec"
        if os.path.isfile("/NAS/mis/auth/prod/ad/svc.tableau.batch.sec") and not "dev" in server:
            sec_filename = "/NAS/mis/auth/prod/ad/svc.tableau.batch.sec"
        if server == 'https://tableau-pp.schwab.com': # CS_PROD == T
            sec_filename = "/NAS/mis/auth/prod/ad/svc.tableau.batch.sec"
        props = prop.ConfProps()
        prop_rc = props.load(sec_filename)
        username = props.get_property("username")
        enc_pass = props.get_property("password_aes")
        win_pass = cs_crypt.decrypt_AES("MISd1g1tal!", enc_pass)
        return self.sign_in(server, username, win_pass, site)


    def sign_in(self, server, name, password, site=""):
        """
        Signs in to the server specified server variable.

        'name'     is the name (not ID) of the user to sign in as.
                   Note that most of the functions in this example require that the user
                   have server administrator permissions.
        'password' is the password for the user.
        'site'     is the ID (as a string) of the site on the server to sign in to. The
                   default is "", which signs in to the default site.

        Returns the authentication token and the site ID.
        """
        self.server = server
        self.server_name = re.sub('^https?://', '', server)
        self.server_name = re.sub('/.*$', '', self.server_name)
        url = server + '/'.join(api_version.split('/')[:3]) + "/auth/signin"
        logmsg("Logging in to server: {0}".format(server))

        # Builds the request
        xml_payload_for_request = ET.Element('tsRequest')
        credentials_element = ET.SubElement(xml_payload_for_request, 'credentials', name=name, password=password)
        site_element = ET.SubElement(credentials_element, 'site', contentUrl=site)
        xml_payload_for_request = ET.tostring(xml_payload_for_request)

        # Makes the request to Tableau Server
        logmsg("Connecting to Tableau server {0}/{1} as {2}".format(server, site, name))
        try:
            server_response = self.session.post(url, data=xml_payload_for_request)
        except requests.exceptions.SSLError as err:
            logmsg("ERROR: SSLError: {0}".format(err))
            return False
        except:
            logmsg("ERROR: Unexpected error connecting to Tableau: {0}".format(sys.exc_info()[0]))
            return False

        if server_response.status_code != 200:
            if 400 <= server_response.status_code < 500:
                self._handle_error(server_response)
                return False
            logmsg("ERROR: " + server_response.text)
            return False
        # Reads and parses the response
        xml_response = ET.fromstring(server_response.text)

        # Gets the token and site ID
        self.token = xml_response.find('t:credentials', namespaces=self.xmlns).attrib.get('token')
        self.site_id = xml_response.find('.//t:site', namespaces=self.xmlns).attrib.get('id')
        self.site_content_url = xml_response.find('.//t:site', namespaces=self.xmlns).attrib.get('contentUrl')
        self.user_id = xml_response.find('.//t:user', namespaces=self.xmlns).attrib.get('id')
        
        # Query the site and get more details such as the name and site URL
        site_data = self.query_site(self.site_id)
        if site_data is not None:
            self.site_name = site_data.attrib.get('name')
        
        if self.site_name is not None:
            logmsg("  Site Name: " + self.site_name)
        return True


    def sign_out(self):
        """
        Destroys the active session
        """
        if self.token is not None:
            logmsg("Disconnecting from Tableau server " + self.server)
            url = self.server + '/'.join(api_version.split('/')[:3]) + "/auth/signout"
            try:
                server_response = self.session.post(url, headers={'x-tableau-auth': self.token})
            except:
                print("ERROR: Failed to sign out: " + str(sys.exc_info()[0]))
            self.token = None
        return
	def refresh_tableau_extract(self, workbook_id):
        """
        Process: Refreshes an extract

        :param workbook_id: ID of workbook
        :return: refresh_job_response: dictionary of response attributes
        """
        logmsg("Initiating refresh for workbook_id: {}".format(workbook_id))
        url = self.server + api_version + "{0}/workbooks/{1}/refresh".format(self.site_id, workbook_id)

        xml_payload_for_request = ET.Element('tsRequest')
        xml_payload_for_request = ET.tostring(xml_payload_for_request)
        
        # Loop while process is awaiting completion of existing extract
        max_retries = 10
        i = 0
        while i <= max_retries:
            if i == max_retries:
                logmsg("ERROR: max_retries of {} reached. Exiting loop...".format(max_retries))
                return None
            server_response = self.session.post(url, data=xml_payload_for_request, headers={'x-tableau-auth': self.token})
            if server_response.status_code == 202:
                break
            elif server_response.status_code in [403, 409]: # Existing extract already in progress
                logmsg("Refresh already in process. Retrying refresh up to {} times".format(max_retries))
                i += 1
                time.sleep(60)
                continue
            else:
                logmsg("ERROR:\nServer Response: {}".format(server_response.text))
                return None
        
        xml_response = ET.fromstring(server_response.text)
        response_data = {}
        for child in xml_response.iter():
            if child.tag == '{http://tableau.com/api}job':
                response_data['job_id'] = child.get('id')
        
        refresh_job_response = None
        max_retries = 600 # Allow for >= 5 hour runtime
        i = 1
        # Use refresh_job_response to check whether job is complete
        if server_response.status_code == 202:
            while i <= max_retries:
                if i == max_retries:
                    logmsg("ERROR: max_retries of {} reached. Exiting loop...".format(max_retries))
                    return None
                refresh_job_response = self.query_job(response_data['job_id'])
                if not refresh_job_response:
                    logmsg("ERROR: Unable to retrieve refresh job status for workbook id '{}'. Exiting...".format(workbook_id))
                    return None
                progress = refresh_job_response['progress']

                if refresh_job_response['finish_code'] == None:
                    job_status = 'In Progress'
                elif refresh_job_response['finish_code'] == '1':
                    job_status = 'Error'
                elif refresh_job_response['finish_code'] == '2':
                    job_status = 'Cancelled'
                elif refresh_job_response['finish_code'] == '0':
                    job_status = 'Complete'

                logmsg("Current progress: {}%, Job status: {}, Status check: {} of {}".format(progress if progress != None else 0, job_status, i, max_retries))
                if refresh_job_response['finish_code'] is not None: # successful := 0
                    break
                else:
                    time.sleep(30)
                i += 1
 
        return refresh_job_response
            
 
    def query_job(self, job_id):
        """
        Process: Returns status information about an asynchronous process that is tracked using a job

        :param job_id: ID of a job
        :return: response_data: dictionary of response attributes
        """
        # URI Format for Query Job: /api/api-version/sites/site-id/jobs/job-id
        url = self.server + api_version + "{0}/jobs/{1}".format(self.site_id, job_id)
        
        # Capture the refresh_job_id from the response of the refresh_tableau_extract(workbook_id) request
        xml_payload_for_request = ET.Element('tsRequest')
        xml_payload_for_request = ET.tostring(xml_payload_for_request)
        
        server_response = self.session.get(url, data=xml_payload_for_request, headers={'x-tableau-auth': self.token})
        
        # Fail out if server_response is something other than 200
        if server_response.status_code != 200:
            return None
        
        xml_response = ET.fromstring(server_response.text)
        response_data = {}

        for child in xml_response.iter():

            if child.tag == '{http://tableau.com/api}tsResponse':
                response_data['schema_location'] = child.get('{http://www.w3.org/2001/XMLSchema-instance}schemaLocation')
            if child.tag == '{http://tableau.com/api}job':
                response_data['id'] = child.get('id')
                response_data['mode'] = child.get('mode')
                response_data['type'] = child.get('type')
                response_data['progress'] = child.get('progress')
                response_data['created_at'] = child.get('createdAt')
                response_data['updated_at'] = child.get('updatedAt')
                response_data['completed_at'] = child.get('completedAt')
                response_data['finish_code'] = child.get('finishCode')
            if child.tag == '{http://tableau.com/api}workbook':
                response_data['workbook_id'] = child.get('id')
                response_data['name'] = child.get('name')
        
        return response_data

    def cancel_job(self, job_id):
        """
        Process: Cancels a job if it exists

        :param job_id: ID of a job
        :return: response_data: dictionary of response attributes
        """
        # URI Format for Query Job: /api/api-version/sites/site-id/jobs/job-id
        url = self.server + api_version + "{0}/jobs/{1}".format(self.site_id, job_id)

        # Capture the refresh_job_id from the response of the refresh_tableau_extract(workbook_id) request
        xml_payload_for_request = ET.Element('tsRequest')
        xml_payload_for_request = ET.tostring(xml_payload_for_request)

        server_response = self.session.put(url, data=xml_payload_for_request, headers={'x-tableau-auth': self.token})

        logmsg(str(server_response.status_code))
        logmsg(str(server_response.headers))
        logmsg(str(server_response.text))

        return
    
    ### Below 3 methods are to facilitate .tableau_extract, where a view is exported to a file of user-specified type (PNG, PDF, FULLPDF) ###
    # Endpoint 1: Query View Image
    def query_view_image(self, view_id):
        """
        Process: Extracts Tableau view to PNG

        :param view_id
        :return: server_response.content (PNG image of the provided view)
        """
        url = self.server + api_version + "{0}/views/{1}/image".format(self.site_id, view_id)
        logmsg("URI " + url)

        xml_payload_for_request = ET.Element('tsRequest')
        xml_payload_for_request = ET.tostring(xml_payload_for_request)
        
        server_response = self.session.get(url, data=xml_payload_for_request, headers={'x-tableau-auth': self.token})
        
        if server_response.status_code != 200:
            logmsg("ERROR:\nServer Response: {}".format(server_response.text))
            return None
        
        return server_response.content
        
        
    # Endpoint 2: Query View PDF
    #   - for option PDF, the user can specify: $page_orientation (portrait || landscape), $page_type (pageType in API docs), $width (vizWidth), $height (vizHeight)
    def query_view_pdf(self, view_id, page_orientation, page_type, width, height):
        """
        Process: Extracts Tableau view to PDF

        :param view_id, page_orientation, page_type, width, height. Latter 4 are set to defaults in directive if not otherwise specified
        :return: server_response.content (PDF of the provided view)
        """
        url = self.server + api_version + "{0}/views/{1}/pdf?orientation={2}&type={3}&vizWidth={4}&vizHeight={5}"\
            .format(self.site_id, view_id, page_orientation, page_type, width, height)
            
        xml_payload_for_request = ET.Element('tsRequest')
        xml_payload_for_request = ET.tostring(xml_payload_for_request)
        
        server_response = self.session.get(url, data=xml_payload_for_request, headers={'x-tableau-auth': self.token})
        
        if server_response.status_code != 200:
            logmsg("ERROR:\nServer Response: {}".format(server_response.text))
            return None
            
        return server_response.content
        
        
    # Endpoint 3: Download Workbook PDF
    def download_workbook_pdf(self, workbook_id, page_orientation, page_type):
        """
        Process: Extracts Tableau view to fullpdf

        :param workbook_id, page_orientation, page_type. Latter 2 are set to defaults in directive if not otherwise specified
        :return: server_response.content (PDF of the provided workbook)
        """
        url = self.server + api_version + "{0}/workbooks/{1}/pdf?orientation={2}&type={3}".format(self.site_id, workbook_id, page_orientation, page_type)
        
        xml_payload_for_request = ET.Element('tsRequest')
        xml_payload_for_request = ET.tostring(xml_payload_for_request)
        
        server_response = self.session.get(url, data=xml_payload_for_request, headers={'x-tableau-auth': self.token})
        
        if server_response.status_code != 200:
            logmsg("ERROR:\nServer Response: {}".format(server_response.text))
            return None
        
        return server_response.content
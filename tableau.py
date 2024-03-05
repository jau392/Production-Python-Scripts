###### Tableau API Methods, written to perform refreshes, extracts, and lookups via the API ######

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
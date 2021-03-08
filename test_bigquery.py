import unittest

import mock
import six
from bigquery import client
from bigquery.errors import (
    JobInsertException, JobExecutingException,
    BigQueryTimeoutException
)
from googleapiclient.errors import HttpError
from nose.tools import raises


class HttpResponse(object):
    def __init__(self, status, reason='There was an error'):
        """
        Args:
            :param int status: Integer HTTP response status
        """
        self.status = status
        self.reason = reason


class TestGetClient(unittest.TestCase):
    def setUp(self):
        client._bq_client = None

        self.mock_bq_service = mock.Mock()
        self.mock_job_collection = mock.Mock()

        self.mock_bq_service.jobs.return_value = self.mock_job_collection

        self.client = client.BigQueryClient(self.mock_bq_service, 'project')

    def test_no_credentials(self):
        """Ensure an Exception is raised when no credentials are provided."""

        self.assertRaises(AssertionError, client.get_client, 'foo')

    @mock.patch('bigquery.client._credentials')
    @mock.patch('bigquery.client.build')
    def test_initialize_readonly(self, mock_build, mock_return_cred):
        """Ensure that a BigQueryClient is initialized and returned with
        read-only permissions.
        """
        from bigquery.client import BIGQUERY_SCOPE_READ_ONLY

        mock_cred = mock.Mock()
        mock_http = mock.Mock()
        mock_service_url = mock.Mock()
        mock_cred.from_p12_keyfile_buffer.return_value.authorize.return_value = mock_http
        mock_bq = mock.Mock()
        mock_build.return_value = mock_bq
        key = 'key'
        service_account = 'account'
        project_id = 'project'
        mock_return_cred.return_value = mock_cred

        bq_client = client.get_client(
            project_id, service_url=mock_service_url,
            service_account=service_account, private_key=key,
            readonly=True)

        mock_return_cred.assert_called_once_with()
        mock_cred.from_p12_keyfile_buffer.assert_called_once_with(
            service_account, mock.ANY,
            scopes=BIGQUERY_SCOPE_READ_ONLY)
        self.assertTrue(
            mock_cred.from_p12_keyfile_buffer.return_value.authorize.called)
        mock_build.assert_called_once_with(
            'bigquery',
            'v2',
            http=mock_http,
            discoveryServiceUrl=mock_service_url,
            cache_discovery=False
        )
        self.assertEquals(mock_bq, bq_client.bigquery)
        self.assertEquals(project_id, bq_client.project_id)

    @mock.patch('bigquery.client._credentials')
    @mock.patch('bigquery.client.build')
    def test_initialize_read_write(self, mock_build, mock_return_cred):
        """Ensure that a BigQueryClient is initialized and returned with
        read/write permissions.
        """
        from bigquery.client import BIGQUERY_SCOPE

        mock_cred = mock.Mock()
        mock_http = mock.Mock()
        mock_service_url = mock.Mock()
        mock_cred.from_p12_keyfile_buffer.return_value.authorize.return_value = mock_http
        mock_bq = mock.Mock()
        mock_build.return_value = mock_bq
        key = 'key'
        service_account = 'account'
        project_id = 'project'
        mock_return_cred.return_value = mock_cred

        bq_client = client.get_client(
            project_id, service_url=mock_service_url,
            service_account=service_account, private_key=key,
            readonly=False)

        mock_return_cred.assert_called_once_with()
        mock_cred.from_p12_keyfile_buffer.assert_called_once_with(
            service_account, mock.ANY, scopes=BIGQUERY_SCOPE)
        self.assertTrue(
            mock_cred.from_p12_keyfile_buffer.return_value.authorize.called)
        mock_build.assert_called_once_with(
            'bigquery',
            'v2',
            http=mock_http,
            discoveryServiceUrl=mock_service_url,
            cache_discovery=False
        )
        self.assertEquals(mock_bq, bq_client.bigquery)
        self.assertEquals(project_id, bq_client.project_id)

    @mock.patch('bigquery.client._credentials')
    @mock.patch('bigquery.client.build')
    def test_initialize_key_file(self, mock_build, mock_return_cred):
        """Ensure that a BigQueryClient is initialized and returned with
        read/write permissions using a private key file.
        """
        from bigquery.client import BIGQUERY_SCOPE

        mock_cred = mock.Mock()
        mock_http = mock.Mock()
        mock_service_url = mock.Mock()
        mock_cred.from_p12_keyfile.return_value.authorize.return_value = mock_http
        mock_bq = mock.Mock()
        mock_build.return_value = mock_bq
        key_file = 'key.pem'
        service_account = 'account'
        project_id = 'project'
        mock_return_cred.return_value = mock_cred

        bq_client = client.get_client(
            project_id, service_url=mock_service_url,
            service_account=service_account,
            private_key_file=key_file, readonly=False)

        mock_return_cred.assert_called_once_with()
        mock_cred.from_p12_keyfile.assert_called_once_with(service_account,
                                                           key_file,
                                                           scopes=BIGQUERY_SCOPE)
        self.assertTrue(
            mock_cred.from_p12_keyfile.return_value.authorize.called)
        mock_build.assert_called_once_with(
            'bigquery',
            'v2',
            http=mock_http,
            discoveryServiceUrl=mock_service_url,
            cache_discovery=False
        )
        self.assertEquals(mock_bq, bq_client.bigquery)
        self.assertEquals(project_id, bq_client.project_id)

    @mock.patch('bigquery.client._credentials')
    @mock.patch('bigquery.client.build')
    @mock.patch('__builtin__.open' if six.PY2 else 'builtins.open')
    def test_initialize_json_key_file(self, mock_open, mock_build, mock_return_cred):
        """Ensure that a BigQueryClient is initialized and returned with
        read/write permissions using a JSON key file.
        """
        from bigquery.client import BIGQUERY_SCOPE
        import json

        mock_cred = mock.Mock()
        mock_http = mock.Mock()
        mock_service_url = mock.Mock()
        mock_cred.from_json_keyfile_dict.return_value.authorize.return_value = mock_http
        mock_bq = mock.Mock()
        mock_build.return_value = mock_bq
        json_key_file = 'key.json'
        json_key = {'client_email': 'mail', 'private_key': 'pkey'}
        mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(json_key)
        project_id = 'project'
        mock_return_cred.return_value = mock_cred

        bq_client = client.get_client(
            project_id, service_url=mock_service_url,
            json_key_file=json_key_file, readonly=False)

        mock_return_cred.assert_called_once_with()
        mock_cred.from_json_keyfile_dict.assert_called_once_with(json_key,
                                                                 scopes=BIGQUERY_SCOPE)
        self.assertTrue(
            mock_cred.from_json_keyfile_dict.return_value.authorize.called)
        mock_build.assert_called_once_with(
            'bigquery',
            'v2',
            http=mock_http,
            discoveryServiceUrl=mock_service_url,
            cache_discovery=False
        )
        self.assertEquals(mock_bq, bq_client.bigquery)
        self.assertEquals(project_id, bq_client.project_id)

    @mock.patch('bigquery.client._credentials')
    @mock.patch('bigquery.client.build')
    @mock.patch('__builtin__.open' if six.PY2 else 'builtins.open')
    def test_initialize_json_key_file_without_project_id(self, mock_open, mock_build,
                                                         mock_return_cred):
        """Ensure that a BigQueryClient is initialized and returned with
        read/write permissions using a JSON key file without project_id.
        """
        from bigquery.client import BIGQUERY_SCOPE
        import json

        mock_cred = mock.Mock()
        mock_http = mock.Mock()
        mock_service_url = mock.Mock()
        mock_cred.from_json_keyfile_dict.return_value.authorize.return_value = mock_http
        mock_bq = mock.Mock()
        mock_build.return_value = mock_bq
        json_key_file = 'key.json'
        json_key = {'client_email': 'mail', 'private_key': 'pkey', 'project_id': 'project'}
        mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(json_key)
        mock_return_cred.return_value = mock_cred

        bq_client = client.get_client(
            service_url=mock_service_url, json_key_file=json_key_file, readonly=False)

        mock_open.assert_called_once_with(json_key_file, 'r')
        mock_return_cred.assert_called_once_with()
        mock_cred.from_json_keyfile_dict.assert_called_once_with(json_key,
                                                                 scopes=BIGQUERY_SCOPE)
        self.assertTrue(
            mock_cred.from_json_keyfile_dict.return_value.authorize.called)
        mock_build.assert_called_once_with(
            'bigquery',
            'v2',
            http=mock_http,
            discoveryServiceUrl=mock_service_url,
            cache_discovery=False
        )
        self.assertEquals(mock_bq, bq_client.bigquery)
        self.assertEquals(json_key['project_id'], bq_client.project_id)


class TestGetProjectIds(unittest.TestCase):

    def test_get_project_ids(self):
        mock_bq_service = mock.Mock()
        mock_bq_service.projects().list().execute.return_value = {
            'kind': 'bigquery#projectList',
            'projects': [
                {
                    'friendlyName': 'Big Query Test',
                    'id': 'big-query-test',
                    'kind': 'bigquery#project',
                    'numericId': '1435372465',
                    'projectReference': {'projectId': 'big-query-test'}
                },
                {
                    'friendlyName': 'BQ Company project',
                    'id': 'bq-project',
                    'kind': 'bigquery#project',
                    'numericId': '4263574685796',
                    'projectReference': {'projectId': 'bq-project'}
                }
            ],
            'totalItems': 2
        }

        projects = client.get_projects(mock_bq_service)
        expected_projects_data = [
            {'id': 'big-query-test', 'name': 'Big Query Test'},
            {'id': 'bq-project', 'name': 'BQ Company project'}
        ]
        self.assertEqual(projects, expected_projects_data)


class TestQuery(unittest.TestCase):

    def setUp(self):
        client._bq_client = None

        self.mock_bq_service = mock.Mock()
        self.mock_job_collection = mock.Mock()

        self.mock_bq_service.jobs.return_value = self.mock_job_collection

        self.query = 'foo'
        self.project_id = 'project'
        self.external_udf_uris = ['gs://bucket/external_udf.js']
        self.client = client.BigQueryClient(self.mock_bq_service,
                                            self.project_id)


    def test_query(self):
        """Ensure that we retrieve the job id from the query."""

        mock_query_job = mock.Mock()
        expected_job_id = 'spiderman'
        expected_job_ref = {'jobId': expected_job_id}

        mock_query_job.execute.return_value = {
            'jobReference': expected_job_ref,
            'jobComplete': True,
            'cacheHit': False,
            'totalBytesProcessed': 0
        }

        self.mock_job_collection.query.return_value = mock_query_job

        job_id, results = self.client.query(self.query, external_udf_uris=self.external_udf_uris)

        self.mock_job_collection.query.assert_called_once_with(
            projectId=self.project_id,
            body={
                'query': self.query,
                'userDefinedFunctionResources': [ {'resourceUri': u} for u in self.external_udf_uris ],
                'timeoutMs': 0,
                'dryRun': False,
                'maxResults': None
            }
        )
        self.assertEquals(job_id, 'spiderman')
        self.assertEquals(results, [])

   
        """Ensure that we retrieve the job id from the query and the maxResults
        parameter is set.
        """

        mock_query_job = mock.Mock()
        expected_job_id = 'spiderman'
        expected_job_ref = {'jobId': expected_job_id}

        mock_query_job.execute.return_value = {
            'jobReference': expected_job_ref,
            'jobComplete': True,
            'cacheHit': False,
            'totalBytesProcessed': 0
        }

        self.mock_job_collection.query.return_value = mock_query_job
        max_results = 10

        job_id, results = self.client.query(self.query,
                                            max_results=max_results)

        self.mock_job_collection.query.assert_called_once_with(
            projectId=self.project_id,
            body={'query': self.query, 'timeoutMs': 0,
                  'maxResults': max_results, 'dryRun': False}
        )
        self.assertEquals(job_id, 'spiderman')
        self.assertEquals(results, [])

        """Ensure that None and a dict is returned from the query when dry_run
        is True and the query is invalid.
        """

        mock_query_job = mock.Mock()

        mock_query_job.execute.side_effect = HttpError(
            'crap', '{"message": "Bad query"}'.encode('utf8'))

        self.mock_job_collection.query.return_value = mock_query_job

        job_id, results = self.client.query('%s blah' % self.query,
                                            dry_run=True)

        self.mock_job_collection.query.assert_called_once_with(
            projectId=self.project_id,
            body={'query': '%s blah' % self.query, 'timeoutMs': 0,
                  'maxResults': None,
                  'dryRun': True}
        )
        self.assertIsNone(job_id)
        self.assertEqual({'message': 'Bad query'}, results)

    def test_query_with_results(self):
        """Ensure that we retrieve the job id from the query and results if
        they are available.
        """

        mock_query_job = mock.Mock()
        expected_job_id = 'spiderman'
        expected_job_ref = {'jobId': expected_job_id}

        mock_query_job.execute.return_value = {
            'jobReference': expected_job_ref,
            'schema': {'fields': [{'name': 'foo', 'type': 'INTEGER'}]},
            'rows': [{'f': [{'v': 10}]}],
            'jobComplete': True,
            'cacheHit': False,
            'totalBytesProcessed': 0
        }

        self.mock_job_collection.query.return_value = mock_query_job

        job_id, results = self.client.query(self.query)

        self.mock_job_collection.query.assert_called_once_with(
            projectId=self.project_id,
            body={'query': self.query, 'timeoutMs': 0, 'dryRun': False,
                  'maxResults': None}
        )
        self.assertEquals(job_id, 'spiderman')
        self.assertEquals(results, [{'foo': 10}])

        """Ensure that use_legacy_sql bool gets used"""

        mock_query_job = mock.Mock()
        expected_job_id = 'spiderman'
        expected_job_ref = {'jobId': expected_job_id}

        mock_query_job.execute.return_value = {
            'jobReference': expected_job_ref,
            'jobComplete': True,
            'cacheHit': False,
            'totalBytesProcessed': 0
        }

        self.mock_job_collection.query.return_value = mock_query_job

        job_id, results = self.client.query(self.query, use_legacy_sql=False)

        self.mock_job_collection.query.assert_called_once_with(
            projectId=self.project_id,
            body={'query': self.query, 'timeoutMs': 0, 'dryRun': False,
                  'maxResults': None, 'useLegacySql': False}
        )
        self.assertEquals(job_id, 'spiderman')
        self.assertEquals(results, [])

        return_values = [{'status': {'state': u'RUNNING'},
                          'jobReference': {'jobId': "testJob"}},
                         {'status': {'state': u'DONE'},
                          'jobReference': {'jobId': "testJob"}}]

        def side_effect(*args, **kwargs):
            return return_values.pop(0)

        self.api_mock.jobs().get().execute.side_effect = side_effect

        job_resource = self.client.wait_for_job(1234567,
                                                interval=.01,
                                                timeout=600)

        self.assertEqual(self.api_mock.jobs().get().execute.call_count, 2)
        self.assertIsInstance(job_resource, dict)


class TestImportDataFromURIs(unittest.TestCase):

    def setUp(self):
        client._bq_client = None
        self.mock_api = mock.Mock()

        self.query = 'foo'
        self.project_id = 'project'
        self.dataset_id = 'dataset'
        self.table_id = 'table'
        self.client = client.BigQueryClient(self.mock_api,
                                            self.project_id)

    def test_csv_job_body_constructed_correctly(self):
        expected_result = {
            'status': {'state': u'RUNNING'},
        }

        body = {
            "jobReference": {
                "projectId": self.project_id,
                "jobId": "job"
            },
            "configuration": {
                "load": {
                    "sourceUris": ["sourceuri"],
                    "schema": {"fields": ["schema"]},
                    "destinationTable": {
                        "projectId": self.project_id,
                        "datasetId": self.dataset_id,
                        "tableId": self.table_id
                    },
                    "createDisposition": "a",
                    "writeDisposition": "b",
                    "fieldDelimiter": "c",
                    "skipLeadingRows": "d",
                    "encoding": "e",
                    "quote": "f",
                    "maxBadRecords": "g",
                    "allowQuotedNewlines": "h",
                    "sourceFormat": "CSV",
                    "allowJaggedRows": "j",
                    "ignoreUnknownValues": "k"
                }
            }
        }

        self.mock_api.jobs().insert().execute.return_value = expected_result
        result = self.client.import_data_from_uris(["sourceuri"],
                                                   self.dataset_id,
                                                   self.table_id,
                                                   ["schema"],
                                                   job="job",
                                                   create_disposition="a",
                                                   write_disposition="b",
                                                   field_delimiter="c",
                                                   skip_leading_rows="d",
                                                   encoding="e",
                                                   quote="f",
                                                   max_bad_records="g",
                                                   allow_quoted_newlines="h",
                                                   source_format="CSV",
                                                   allow_jagged_rows="j",
                                                   ignore_unknown_values="k")

        self.mock_api.jobs().insert.assert_called_with(
            projectId=self.project_id,
            body=body
        )

        self.assertEqual(result, expected_result)

    def test_json_job_body_constructed_correctly(self):
        expected_result = {
            'status': {'state': u'RUNNING'},
        }

        body = {
            "jobReference": {
                "projectId": self.project_id,
                "jobId": "job",
            },
            "configuration": {
                "load": {
                    "sourceUris": ["sourceuri"],
                    "schema": {"fields": ["schema"]},
                    "destinationTable": {
                        "projectId": self.project_id,
                        "datasetId": self.dataset_id,
                        "tableId": self.table_id
                    },
                    "sourceFormat": "JSON"
                }
            }
        }

        self.mock_api.jobs().insert().execute.return_value = expected_result
        result = self.client.import_data_from_uris(["sourceuri"],
                                                   self.dataset_id,
                                                   self.table_id,
                                                   ["schema"],
                                                   job="job",
                                                   source_format="JSON")

        self.mock_api.jobs().insert.assert_called_with(
            projectId=self.project_id,
            body=body
        )

        self.assertEqual(result, expected_result)

    @raises(Exception)
    def test_field_delimiter_exception_if_not_csv(self):
        """Raise exception if csv-only parameter is set inappropriately"""
        self.client.import_data_from_uris(["sourceuri"],
                                          self.dataset_id,
                                          self.table_id,
                                          ["schema"],
                                          job="job",
                                          source_format="JSON",
                                          field_delimiter=",")

    @raises(Exception)
    def test_allow_jagged_rows_exception_if_not_csv(self):
        """Raise exception if csv-only parameter is set inappropriately"""
        self.client.import_data_from_uris(["sourceuri"],
                                          self.dataset_id,
                                          self.table_id,
                                          ["schema"],
                                          job="job",
                                          source_format="JSON",
                                          allow_jagged_rows=True)

    @raises(Exception)
    def test_allow_quoted_newlines_exception_if_not_csv(self):
        """Raise exception if csv-only parameter is set inappropriately"""
        self.client.import_data_from_uris(["sourceuri"],
                                          self.dataset_id,
                                          self.table_id,
                                          ["schema"],
                                          job="job",
                                          source_format="JSON",
                                          allow_quoted_newlines=True)

    @raises(Exception)
    def test_quote_exception_if_not_csv(self):
        """Raise exception if csv-only parameter is set inappropriately"""
        self.client.import_data_from_uris(["sourceuri"],
                                          self.dataset_id,
                                          self.table_id,
                                          ["schema"],
                                          job="job",
                                          source_format="JSON",
                                          quote="'")

    @raises(Exception)
    def test_skip_leading_rows_exception_if_not_csv(self):
        """Raise exception if csv-only parameter is set inappropriately"""
        self.client.import_data_from_uris(["sourceuri"],
                                          self.dataset_id,
                                          self.table_id,
                                          ["schema"],
                                          "job",
                                          source_format="JSON",
                                          skip_leading_rows=10)

    def test_accepts_single_source_uri(self):
        """Ensure that a source_uri accepts a non-list"""
        expected_result = {
            'status': {'state': u'RUNNING'},
        }

        body = {
            "jobReference": {
                "projectId": self.project_id,
                "jobId": "job"
            },
            "configuration": {
                "load": {
                    "sourceUris": ["sourceuri"],
                    "schema": {"fields": ["schema"]},
                    "destinationTable": {
                        "projectId": self.project_id,
                        "datasetId": self.dataset_id,
                        "tableId": self.table_id
                    }
                }
            }
        }

        self.mock_api.jobs().insert().execute.return_value = expected_result
        result = self.client.import_data_from_uris("sourceuri",  # not a list!
                                                   self.dataset_id,
                                                   self.table_id,
                                                   schema=["schema"],
                                                   job="job")

        self.mock_api.jobs().insert.assert_called_with(
            projectId=self.project_id,
            body=body
        )

        self.assertEqual(result, expected_result)

    def test_import_http_error(self):
        """ Test import with http error"""
        expected_result = {
            "error": {
                "errors": [{
                    "domain": "global",
                    "reason": "required",
                    "message": "Required parameter is missing"
                }],
                "code": 400,
                "message": "Required parameter is missing"
            }
        }

        self.mock_api.jobs().insert().execute.return_value = expected_result
        self.assertRaises(JobInsertException,
                          self.client.import_data_from_uris,
                          ["sourceuri"],
                          self.dataset_id,
                          self.table_id)

    def test_import_error_result(self):
        """ Test import with error result"""
        expected_result = {
            "status": {
                "state": "DONE",
                "errorResult": {
                    "reason": "invalidQuery",
                    "location": "query",
                    "message": "Your Error Message Here "
                },
            },
        }

        self.mock_api.jobs().insert().execute.return_value = expected_result
        self.assertRaises(JobInsertException,
                          self.client.import_data_from_uris,
                          ["sourceuri"],
                          self.dataset_id,
                          self.table_id)


class TestExportDataToURIs(unittest.TestCase):

    def setUp(self):
        client._bq_client = None
        self.mock_api = mock.Mock()

        self.project_id = 'project'
        self.dataset_id = 'dataset'
        self.table_id = 'table'
        self.destination_format = "CSV"
        self.print_header = False
        self.client = client.BigQueryClient(self.mock_api,
                                            self.project_id)

    @mock.patch('bigquery.client.BigQueryClient._generate_hex_for_uris')
    def test_export(self, mock_generate_hex):
        """ Ensure that export is working in normal circumstances """
        expected_result = {
            'status': {'state': u'RUNNING'},
        }

        body = {
            "jobReference": {
                "projectId": self.project_id,
                "jobId": "%s-%s-destinationuri" % (self.dataset_id,
                                                   self.table_id)
            },
            "configuration": {
                "extract": {
                    "destinationUris": ["destinationuri"],
                    "sourceTable": {
                        "projectId": self.project_id,
                        "datasetId": self.dataset_id,
                        "tableId": self.table_id
                    },
                    "destinationFormat": self.destination_format,
                    "printHeader": self.print_header,
                }
            }
        }

        self.mock_api.jobs().insert().execute.return_value = expected_result
        mock_generate_hex.return_value = "destinationuri"
        result = self.client.export_data_to_uris(
            ["destinationuri"], self.dataset_id, self.table_id,
            destination_format=self.destination_format,
            print_header=self.print_header
        )

        self.mock_api.jobs().insert.assert_called_with(
            projectId=self.project_id,
            body=body
        )

        self.assertEqual(result, expected_result)

  
NEXT_TABLE_LIST_RESPONSE = {
    "kind": "bigquery#tableList",
    "etag": "\"t_UlB9a9mrx5sjQInRGzeDrLrS0/TsIP_i4gAeLegj84WzkPzBPIkjo\"",
    "nextPageToken": "2013_05_appspot_1",
    "tables": [
        {
            "kind": "bigquery#table",
            "id": "project:dataset.2013_06_appspot_10",
            "tableReference": {
                "projectId": "project",
                "datasetId": "dataset",
                "tableId": "2013_06_appspot_10"
            }
        },
        {
            "kind": "bigquery#table",
            "id": "project:dataset.2013_06_appspot_11",
            "tableReference": {
                "projectId": "project",
                "datasetId": "dataset",
                "tableId": "2013_06_appspot_11"
            }
        },
        {
            "kind": "bigquery#table",
            "id": "project:dataset.2013_06_appspot_12",
            "tableReference": {
                "projectId": "project",
                "datasetId": "dataset",
                "tableId": "2013_06_appspot_12"
            }
        },
    ],
    "totalItems": 3
}

FULL_TABLE_LIST_RESPONSE = {
    "kind": "bigquery#tableList",
    "etag": "\"GSclnjk0zID1ucM3F-xYinOm1oE/cn58Rpu8v8pB4eoJQaiTe11lPQc\"",
    "tables": [
        {
            "kind": "bigquery#table",
            "id": "project:dataset.notanappspottable_20130515_0261",
            "tableReference": {
                "projectId": "project",
                "datasetId": "dataset",
                "tableId": "notanappspottable_20130515_0261"
            }
        },
        {
            "kind": "bigquery#table",
            "id": "project:dataset.2013_05_appspot_1",
            "tableReference": {
                "projectId": "project",
                "datasetId": "dataset",
                "tableId": "2013_05_appspot"
            }
        },
        {
            "kind": "bigquery#table",
            "id": "project:dataset.2013_06_appspot_1",
            "tableReference": {
                "projectId": "project",
                "datasetId": "dataset",
                "tableId": "2013_06_appspot_1"
            }
        },
        {
            "kind": "bigquery#table",
            "id": "project:dataset.2013_06_appspot_2",
            "tableReference": {
                "projectId": "project",
                "datasetId": "dataset",
                "tableId": "2013_06_appspot_2"
            }
        },
        {
            "kind": "bigquery#table",
            "id": "project:dataset.2013_06_appspot_3",
            "tableReference": {
                "projectId": "project",
                "datasetId": "dataset",
                "tableId": "2013_06_appspot_3"
            }
        },
        {
            "kind": "bigquery#table",
            "id": "project:dataset.2013_06_appspot_4",
            "tableReference": {
                "projectId": "project",
                "datasetId": "dataset",
                "tableId": "2013_06_appspot_4"
            }
        },
        {
            "kind": "bigquery#table",
            "id": "project:dataset.2013_06_appspot_5",
            "tableReference": {
                "projectId": "project",
                "datasetId": "dataset",
                "tableId": "2013_06_appspot_5"
            }
        },
        {
            "kind": "bigquery#table",
            "id": "project:dataset.appspot_6_2013_06",
            "tableReference": {
                "projectId": "project",
                "datasetId": "dataset",
                "tableId": "appspot_6_2013_06"
            }
        },
        {
            "kind": "bigquery#table",
            "id": "project:dataset.table_not_matching_naming",
            "tableReference": {
                "projectId": "project",
                "datasetId": "dataset",
                "tableId": "table_not_matching_naming"
            }
        },
        {
            "kind": "bigquery#table",
            "id": "bad table data"
        },
    ],
    "totalItems": 9
}


@mock.patch('bigquery.client.BigQueryClient.get_query_results')
class TestGetQueryRows(unittest.TestCase):

    def test_query_complete(self, get_query_mock):
        """Ensure that get_query_rows works when a query is complete."""
        from bigquery.client import BigQueryClient

        bq = BigQueryClient(mock.Mock(), 'project')

        get_query_mock.return_value = {
            'jobComplete': True,
            'rows': [
                {'f': [{'v': 'bar'}, {'v': 'man'}]},
                {'f': [{'v': 'abc'}, {'v': 'xyz'}]}
            ],
            'schema': {
                'fields': [
                    {'name': 'foo', 'type': 'STRING'},
                    {'name': 'spider', 'type': 'STRING'}
                ]
            },
            'totalRows': 2
        }

        result_rows = bq.get_query_rows(job_id=123, offset=0, limit=0)

        expected_rows = [{'foo': 'bar', 'spider': 'man'},
                         {'foo': 'abc', 'spider': 'xyz'}]
        self.assertEquals(result_rows, expected_rows)

    def test_query_complete_with_page_token(self, get_query_mock):
        """Ensure that get_query_rows works with page token."""
        from bigquery.client import BigQueryClient

        page_one_resp = {
            "jobComplete": True,
            "kind": "bigquery#getQueryResultsResponse",
            "pageToken": "TOKEN_TO_PAGE_2",
            "schema": {
                "fields": [{
                    "name": "first_name",
                    "type": "STRING",
                }, {
                    "name": "last_name",
                    "type": "STRING",
                }]
            },
            "rows": [{
                "f": [{
                    "v": "foo",
                }, {
                    "v": "bar"
                }]
            }, {
                "f": [{
                    "v": "abc",
                }, {
                    "v": "xyz"
                }]
            }],
            "totalRows": "4"
        }

        page_two_resp = {
            "jobComplete": True,
            "kind": "bigquery#getQueryResultsResponse",
            "schema": {
                "fields": [{
                    "name": "first_name",
                    "type": "STRING",
                }, {
                    "name": "last_name",
                    "type": "STRING",
                }]
            },
            "rows": [{
                "f": [{
                    "v": "the",
                }, {
                    "v": "beatles"
                }]
            }, {
                "f": [{
                    "v": "monty",
                }, {
                    "v": "python"
                }]
            }],
            "totalRows": "4"
        }

        bq = BigQueryClient(mock.Mock(), 'project')
        get_query_mock.side_effect = [page_one_resp, page_two_resp]
        result_rows = bq.get_query_rows(job_id=123, offset=0, limit=0)

        expected_rows = [{'first_name': 'foo', 'last_name': 'bar'},
                         {'first_name': 'abc', 'last_name': 'xyz'},
                         {'first_name': 'the', 'last_name': 'beatles'},
                         {'first_name': 'monty', 'last_name': 'python'}]
        self.assertEquals(result_rows, expected_rows)

    def test_query_incomplete(self, get_query_mock):
        """Ensure that get_query_rows handles scenarios where the query is not
        finished.
        """
        from bigquery.client import BigQueryClient

        bq = BigQueryClient(mock.Mock(), 'project')

        get_query_mock.return_value = {
            'jobComplete': False,
            'rows': [
                {'f': [{'v': 'bar'}, {'v': 'man'}]},
                {'f': [{'v': 'abc'}, {'v': 'xyz'}]}
            ],
            'schema': {
                'fields': [
                    {'name': 'foo', 'type': 'STRING'},
                    {'name': 'spider', 'type': 'STRING'}
                ]
            },
            'totalRows': 2
        }

        self.assertRaises(client.UnfinishedQueryException, bq.get_query_rows,
                          job_id=123, offset=0, limit=0)


class TestCheckTable(unittest.TestCase):

    def setUp(self):
        self.mock_bq_service = mock.Mock()
        self.mock_tables = mock.Mock()
        self.mock_bq_service.tables.return_value = self.mock_tables
        self.table = 'table'
        self.project = 'project'
        self.dataset = 'dataset'
        self.client = client.BigQueryClient(self.mock_bq_service, self.project)

    def test_table_does_not_exist(self):
        """Ensure that if the table does not exist, False is returned."""

        self.mock_tables.get.return_value.execute.side_effect = (
            HttpError(HttpResponse(404), 'There was an error'.encode('utf8')))

        actual = self.client.check_table(self.dataset, self.table)

        self.assertFalse(actual)

        self.mock_tables.get.assert_called_once_with(
            projectId=self.project, datasetId=self.dataset, tableId=self.table)

        self.mock_tables.get.return_value.execute. \
            assert_called_once_with(num_retries=0)

    def test_table_does_exist(self):
        """Ensure that if the table does exist, True is returned."""

        self.mock_tables.get.return_value.execute.side_effect = {
            'status': 'foo'}

        actual = self.client.check_table(self.dataset, self.table)

        self.assertTrue(actual)

        self.mock_tables.get.assert_called_once_with(
            projectId=self.project, datasetId=self.dataset, tableId=self.table)

        self.mock_tables.get.return_value.execute. \
            assert_called_once_with(num_retries=0)


class TestCreateTable(unittest.TestCase):

    def setUp(self):
        self.mock_bq_service = mock.Mock()
        self.mock_tables = mock.Mock()
        self.mock_bq_service.tables.return_value = self.mock_tables
        self.table = 'table'
        self.schema = [
            {'name': 'foo', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'bar', 'type': 'FLOAT', 'mode': 'nullable'}
        ]
        self.project = 'project'
        self.dataset = 'dataset'
        self.client = client.BigQueryClient(self.mock_bq_service, self.project)
        self.body = {
            'schema': {'fields': self.schema},
            'tableReference': {
                'tableId': self.table, 'projectId': self.project,
                'datasetId': self.dataset}
        }
        self.expiration_time = 1437513693000
        self.time_partitioning = True

    def test_table_create_failed(self):
        """Ensure that if creating the table fails, False is returned,
        or if swallow_results is False an empty dict is returned."""

        self.mock_tables.insert.return_value.execute.side_effect = (
            HttpError(HttpResponse(404), 'There was an error'.encode('utf8')))

        actual = self.client.create_table(self.dataset, self.table,
                                          self.schema)

        self.assertFalse(actual)

        self.client.swallow_results = False

        actual = self.client.create_table(self.dataset, self.table,
                                          self.schema)

        self.assertEqual(actual, {})

        self.client.swallow_results = True

        self.mock_tables.insert.assert_called_with(
            projectId=self.project, datasetId=self.dataset, body=self.body)

        self.mock_tables.insert.return_value.execute. \
            assert_called_with(num_retries=0)

    def test_table_create_success(self):
        """Ensure that if creating the table succeeds, True is returned,
        or if swallow_results is False the actual response is returned."""

        self.mock_tables.insert.return_value.execute.side_effect = [{
            'status': 'foo'}, {'status': 'bar'}]

        actual = self.client.create_table(self.dataset, self.table,
                                          self.schema)

        self.assertTrue(actual)

        self.client.swallow_results = False

        actual = self.client.create_table(self.dataset, self.table,
                                          self.schema)

        self.assertEqual(actual, {'status': 'bar'})

        self.client.swallow_results = True

        self.mock_tables.insert.assert_called_with(
            projectId=self.project, datasetId=self.dataset, body=self.body)

        self.mock_tables.insert.return_value.execute. \
            assert_called_with(num_retries=0)

    def test_table_create_body_with_expiration_time(self):
        """Ensure that if expiration_time has specified,
        it passed to the body."""

        self.mock_tables.insert.return_value.execute.side_effect = [{
            'status': 'foo'}, {'status': 'bar'}]

        self.client.create_table(self.dataset, self.table,
                                 self.schema, self.expiration_time)

        body = self.body.copy()
        body.update({
            'expirationTime': self.expiration_time
        })

        self.mock_tables.insert.assert_called_with(
            projectId=self.project, datasetId=self.dataset, body=body)

        self.mock_tables.insert.return_value.execute. \
            assert_called_with(num_retries=0)

    def test_table_create_body_with_time_partitioning(self):
        """Ensure that if time_partitioning has specified,
        it passed to the body."""

        self.mock_tables.insert.return_value.execute.side_effect = [{
            'status': 'foo'}, {'status': 'bar'}]

        self.client.create_table(self.dataset, self.table,
                                 self.schema,
                                 time_partitioning=self.time_partitioning)

        body = self.body.copy()
        body.update({
            'timePartitioning': {'type': 'DAY'}
        })

        self.mock_tables.insert.assert_called_with(
            projectId=self.project, datasetId=self.dataset, body=body)

        self.mock_tables.insert.return_value.execute. \
            assert_called_with(num_retries=0)


class TestUpdateTable(unittest.TestCase):

    def setUp(self):
        self.mock_bq_service = mock.Mock()
        self.mock_tables = mock.Mock()
        self.mock_bq_service.tables.return_value = self.mock_tables
        self.table = 'table'
        self.schema = [
            {'name': 'foo', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'bar', 'type': 'FLOAT', 'mode': 'nullable'}
        ]
        self.project = 'project'
        self.dataset = 'dataset'
        self.client = client.BigQueryClient(self.mock_bq_service, self.project)
        self.body = {
            'schema': {'fields': self.schema},
            'tableReference': {
                'tableId': self.table, 'projectId': self.project,
                'datasetId': self.dataset}
        }
        self.expiration_time = 1437513693000

    def test_table_update_failed(self):
        """Ensure that if updating the table fails, False is returned,
        or if swallow_results is False an empty dict is returned."""

        self.mock_tables.update.return_value.execute.side_effect = (
            HttpError(HttpResponse(404), 'There was an error'.encode('utf8')))

        actual = self.client.update_table(self.dataset, self.table,
                                          self.schema)

        self.assertFalse(actual)

        self.client.swallow_results = False

        actual = self.client.update_table(self.dataset, self.table,
                                          self.schema)

        self.assertEqual(actual, {})

        self.client.swallow_results = True

        self.mock_tables.update.assert_called_with(
            projectId=self.project, tableId=self.table, datasetId=self.dataset,
            body=self.body)

        self.mock_tables.update.return_value.execute. \
            assert_called_with(num_retries=0)

    def test_table_update_success(self):
        """Ensure that if updating the table succeeds, True is returned,
        or if swallow_results is False the actual response is returned."""

        self.mock_tables.update.return_value.execute.side_effect = [{
            'status': 'foo'}, {'status': 'bar'}]

        actual = self.client.update_table(self.dataset, self.table,
                                          self.schema)

        self.assertTrue(actual)

        self.client.swallow_results = False

        actual = self.client.update_table(self.dataset, self.table,
                                          self.schema)

        self.assertEqual(actual, {'status': 'bar'})

        self.client.swallow_results = True

        self.mock_tables.update.assert_called_with(
            projectId=self.project, tableId=self.table, datasetId=self.dataset,
            body=self.body)

        self.mock_tables.update.return_value.execute. \
            assert_called_with(num_retries=0)


#
# Dataset tests
#
class TestCreateDataset(unittest.TestCase):

    def setUp(self):
        self.mock_bq_service = mock.Mock()
        self.mock_datasets = mock.Mock()
        self.mock_bq_service.datasets.return_value = self.mock_datasets
        self.dataset = 'dataset'
        self.project = 'project'
        self.client = client.BigQueryClient(self.mock_bq_service, self.project)
        self.friendly_name = "friendly name"
        self.description = "description"
        self.access = [{'userByEmail': "bob@gmail.com"}]
        self.body = {
            'datasetReference': {
                'datasetId': self.dataset,
                'projectId': self.project},
            'friendlyName': self.friendly_name,
            'description': self.description,
            'access': self.access
        }

    def test_dataset_create_failed(self):
        """Ensure that if creating the table fails, False is returned."""

        self.mock_datasets.insert.return_value.execute.side_effect = \
            HttpError(HttpResponse(404), 'There was an error'.encode('utf8'))

        actual = self.client.create_dataset(self.dataset,
                                            friendly_name=self.friendly_name,
                                            description=self.description,
                                            access=self.access)
        self.assertFalse(actual)

        self.client.swallow_results = False

        actual = self.client.create_dataset(self.dataset,
                                            friendly_name=self.friendly_name,
                                            description=self.description,
                                            access=self.access)

        self.assertEqual(actual, {})

        self.client.swallow_results = True

        self.mock_datasets.insert.assert_called_with(
            projectId=self.project, body=self.body)

        self.mock_datasets.insert.return_value.execute. \
            assert_called_with(num_retries=0)

    def test_dataset_create_success(self):
        """Ensure that if creating the table fails, False is returned."""

        self.mock_datasets.insert.return_value.execute.side_effect = [{
            'status': 'foo'}, {'status': 'bar'}]

        actual = self.client.create_dataset(self.dataset,
                                            self.friendly_name,
                                            self.description,
                                            self.access)
        self.assertTrue(actual)

        self.client.swallow_results = False

        actual = self.client.create_dataset(self.dataset,
                                            self.friendly_name,
                                            self.description,
                                            self.access)

        self.assertEqual(actual, {'status': 'bar'})

        self.client.swallow_results = True

        self.mock_datasets.insert.assert_called_with(
            projectId=self.project, body=self.body)

        self.mock_datasets.insert.return_value.execute. \
            assert_called_with(num_retries=0)


class TestDeleteDataset(unittest.TestCase):

    def setUp(self):
        self.mock_bq_service = mock.Mock()
        self.mock_datasets = mock.Mock()
        self.mock_bq_service.datasets.return_value = self.mock_datasets
        self.project = 'project'
        self.dataset = 'dataset'
        self.client = client.BigQueryClient(self.mock_bq_service, self.project)

    def test_delete_datasets_fail(self):
        """Ensure that if deleting table fails, False is returned."""

        self.mock_datasets.delete.return_value.execute.side_effect = \
            HttpError(HttpResponse(404), 'There was an error'.encode('utf8'))

        actual = self.client.delete_dataset(self.dataset)

        self.assertFalse(actual)

        self.mock_datasets.delete.assert_called_once_with(
            projectId=self.project, datasetId=self.dataset,
            deleteContents=False)

        self.client.swallow_results = False

        actual = self.client.delete_dataset(self.dataset)

        self.assertEqual(actual, {})

        self.client.swallow_results = True

        self.mock_datasets.delete.return_value.execute. \
            assert_called_with(num_retries=0)

    def test_delete_datasets_success(self):
        """Ensure that if deleting table succeeds, True is returned."""

        self.mock_datasets.delete.return_value.execute.side_effect = [{
            'status': 'foo'}, {'status': 'bar'}]

        actual = self.client.delete_dataset(self.dataset)

        self.assertTrue(actual)

        self.client.swallow_results = False

        actual = self.client.delete_dataset(self.dataset)

        self.assertEqual(actual, {'status': 'bar'})

        self.client.swallow_results = True

        self.mock_datasets.delete.assert_called_with(
            projectId=self.project, datasetId=self.dataset,
            deleteContents=False)

        self.mock_datasets.delete.return_value.execute. \
            assert_called_with(num_retries=0)

    def test_delete_datasets_delete_contents_success(self):
        """Ensure that if deleting table succeeds, True is returned."""

        self.mock_datasets.delete.return_value.execute.side_effect = [{
            'status': 'foo'}, {'status': 'bar'}]

        actual = self.client.delete_dataset(self.dataset, True)

        self.assertTrue(actual)

        self.client.swallow_results = False

        actual = self.client.delete_dataset(self.dataset, True)

        self.assertEqual(actual, {'status': 'bar'})

        self.client.swallow_results = True

        self.mock_datasets.delete.assert_called_with(
            projectId=self.project, datasetId=self.dataset,
            deleteContents=True)

        self.mock_datasets.delete.return_value.execute. \
            assert_called_with(num_retries=0)


FULL_DATASET_LIST_RESPONSE = {
    "kind": "bigquery#dataseteList",
    "etag": "\"GSclnjk0zID1ucM3F-xYinOm1oE/cn58Rpu8v8pB4eoJQaiTe11lPQc\"",
    "datasets": [
        {
            "kind": "bigquery#dataset",
            "id": "project:dataset1",
            "datasetReference": {
                "projectId": "project",
                "datasetId": "dataset1"
            }
        },
        {
            "kind": "bigquery#dataset",
            "id": "project:dataset2",
            "datasetReference": {
                "projectId": "project",
                "datasetId": "dataset2",
            }
        },
        {
            "kind": "bigquery#dataset",
            "id": "project:dataset3",
            "datasetReference": {
                "projectId": "project",
                "datasetId": "dataset3"
            }
        },
        {
            "kind": "bigquery#dataset",
            "id": "project:dataset4",
            "datasetReference": {
                "projectId": "project",
                "datasetId": "dataset4"
            }
        },
        {
            "kind": "bigquery#dataset",
            "id": "project:dataset5",
            "datasetReference": {
                "projectId": "project",
                "datasetId": "dataset5"
            }
        },
        {
            "kind": "bigquery#dataset",
            "id": "project:dataset6",
            "datasetReference": {
                "projectId": "project",
                "datasetId": "dataset6"
            }
        },
        {
            "kind": "bigquery#dataset",
            "id": "project:dataset7",
            "datasetReference": {
                "projectId": "project",
                "datasetId": "dataset7"
            }
        },
        {
            "kind": "bigquery#dataset",
            "id": "bad dataset data"
        }
    ],
    "totalItems": 8
}


class TestGetDatasets(unittest.TestCase):

    def test_get_datasets(self):
        """Ensure datasets are returned."""

        mock_execute = mock.Mock()
        mock_execute.execute.return_value = FULL_DATASET_LIST_RESPONSE

        mock_datasets = mock.Mock()
        mock_datasets.list.return_value = mock_execute

        mock_bq_service = mock.Mock()
        mock_bq_service.datasets.return_value = mock_datasets

        bq = client.BigQueryClient(mock_bq_service, 'project')

        datasets = bq.get_datasets()
        six.assertCountEqual(self, datasets,
                             FULL_DATASET_LIST_RESPONSE['datasets'])

    def test_get_datasets_returns_no_list(self):
        """Ensure we handle the no datasets case"""
        mock_execute = mock.Mock()
        mock_execute.execute.return_value = {
            "kind": "bigquery#dataseteList",
            "etag": "\"GSclnjk0zID1ucM3F-xYinOm1oE/cn58Rpu8v8pB4eoJQaiTe11lP\""
        }

        mock_datasets = mock.Mock()
        mock_datasets.list.return_value = mock_execute

        mock_bq_service = mock.Mock()
        mock_bq_service.datasets.return_value = mock_datasets

        bq = client.BigQueryClient(mock_bq_service, 'project')

        datasets = bq.get_datasets()
        six.assertCountEqual(self, datasets, [])


class TestUpdateDataset(unittest.TestCase):

    def setUp(self):
        self.mock_bq_service = mock.Mock()
        self.mock_datasets = mock.Mock()
        self.mock_bq_service.datasets.return_value = self.mock_datasets
        self.dataset = 'dataset'
        self.project = 'project'
        self.client = client.BigQueryClient(self.mock_bq_service, self.project)
        self.friendly_name = "friendly name"
        self.description = "description"
        self.access = [{'userByEmail': "bob@gmail.com"}]
        self.body = {
            'datasetReference': {
                'datasetId': self.dataset,
                'projectId': self.project},
            'friendlyName': self.friendly_name,
            'description': self.description,
            'access': self.access
        }

    def test_dataset_update_failed(self):
        """Ensure that if creating the table fails, False is returned."""

        self.mock_datasets.update.return_value.execute.side_effect = \
            HttpError(HttpResponse(404), 'There was an error'.encode('utf8'))

        actual = self.client.update_dataset(self.dataset,
                                            friendly_name=self.friendly_name,
                                            description=self.description,
                                            access=self.access)
        self.assertFalse(actual)

        self.client.swallow_results = False

        actual = self.client.update_dataset(self.dataset,
                                            friendly_name=self.friendly_name,
                                            description=self.description,
                                            access=self.access)

        self.assertEqual(actual, {})

        self.client.swallow_results = True

        self.mock_datasets.update.assert_called_with(
            projectId=self.project, datasetId=self.dataset, body=self.body)

        self.mock_datasets.update.return_value.execute. \
            assert_called_with(num_retries=0)

    def test_dataset_update_success(self):
        """Ensure that if creating the table fails, False is returned."""

        self.mock_datasets.update.return_value.execute.side_effect = [{
            'status': 'foo'}, {'status': 'bar'}]

        actual = self.client.update_dataset(self.dataset,                                           
                                            friendly_name=self.friendly_name,
                                            description=self.description,
                                            access=self.access)
        self.assertTrue(actual)

        self.client.swallow_results = False

        actual = self.client.update_dataset(self.dataset,                                            
                                            friendly_name=self.friendly_name,
                                            description=self.description,
                                            access=self.access)

        self.assertEqual(actual, {'status': 'bar'})

        self.client.swallow_results = True

        self.mock_datasets.update.assert_called_with(
            projectId=self.project, datasetId=self.dataset, body=self.body)

        self.mock_datasets.update.return_value.execute. \
            assert_called_with(num_retries=0)



    def setUp(self):
        client._bq_client = None

        self.mock_bq_service = mock.Mock()
        self.mock_tables = mock.Mock()
        self.mock_job_collection = mock.Mock()
        self.mock_datasets = mock.Mock()
        self.mock_table_data = mock.Mock()
        self.mock_bq_service.tables.return_value = self.mock_tables
        self.mock_bq_service.jobs.return_value = self.mock_job_collection
        self.mock_bq_service.datasets.return_value = self.mock_datasets
        self.mock_bq_service.tabledata.return_value = self.mock_table_data

        self.project_id = 'project'
        self.num_retries = 5
        self.client = client.BigQueryClient(self.mock_bq_service,
                                            self.project_id,
                                            num_retries=self.num_retries)
        self.dataset = 'dataset'
        self.project = 'project'
        self.table = 'table'
        self.schema = [
            {'name': 'foo', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'bar', 'type': 'FLOAT', 'mode': 'nullable'}
        ]
        self.friendly_name = "friendly name"
        self.description = "description"
        self.access = [{'userByEmail': "bob@gmail.com"}]
        self.query = 'SELECT "bar" foo, "foo" bar'
        self.rows = [
            {'one': 'uno', 'two': 'dos'}, {'one': 'ein', 'two': 'zwei'},
            {'two': 'kiwi'}]
        self.data = {
            "kind": "bigquery#tableDataInsertAllRequest",
            "rows": [{'insertId': "uno", 'json': {'one': 'uno', 'two': 'dos'}},
                     {'insertId': "ein", 'json':
                         {'one': 'ein', 'two': 'zwei'}},
                     {'json': {'two': 'kiwi'}}]
        }

    def test_get_response(self):
        job_id = 'bar'

        mock_query_job = mock.Mock()
        mock_query_reply = mock.Mock()
        mock_query_job.execute.return_value = mock_query_reply
        self.mock_job_collection.getQueryResults.return_value = mock_query_job

        offset = 5
        limit = 10
        page_token = "token"
        timeout = 1

        self.client.get_query_results(job_id, offset, limit,
                                      page_token, timeout)

        mock_query_job.execute. \
            assert_called_once_with(num_retries=self.num_retries)

    def test_table_exists(self):
        expected = [
            {'type': 'FLOAT', 'name': 'foo', 'mode': 'NULLABLE'},
            {'type': 'INTEGER', 'name': 'bar', 'mode': 'NULLABLE'},
            {'type': 'INTEGER', 'name': 'baz', 'mode': 'NULLABLE'},
        ]

        self.mock_tables.get.return_value.execute.return_value = \
            {'schema': {'fields': expected}}

        self.client.get_table_schema(self.dataset, self.table)
        self.mock_tables.get.return_value.execute. \
            assert_called_once_with(num_retries=self.num_retries)

    def test_table_create(self):
        self.mock_tables.insert.return_value.execute.side_effect = [{
            'status': 'foo'}, {'status': 'bar'}]

        self.client.create_table(self.dataset, self.table,
                                 self.schema)

        self.mock_tables.insert.return_value.execute. \
            assert_called_with(num_retries=self.num_retries)

    def test_table_update(self):
        self.mock_tables.update.return_value.execute.side_effect = [{
            'status': 'foo'}, {'status': 'bar'}]

        self.client.update_table(self.dataset, self.table,
                                 self.schema)

        self.mock_tables.update.return_value.execute. \
            assert_called_with(num_retries=self.num_retries)

    def test_table_patch(self):
        self.mock_tables.patch.return_value.execute.side_effect = [{
            'status': 'foo'}, {'status': 'bar'}]

        self.client.patch_table(self.dataset, self.table,
                                self.schema)

        self.mock_tables.patch.return_value.execute. \
            assert_called_with(num_retries=self.num_retries)

    def test_view_create(self):
        body = {
            'view': {'query': self.query},
            'tableReference': {
                'tableId': self.table, 'projectId': self.project,
                'datasetId': self.dataset
            }
        }

        self.mock_tables.insert.return_value.execute.side_effect = [{
            'status': 'foo'}, {'status': 'bar'}]

        actual = self.client.create_view(self.dataset, self.table,
                                         self.query)

        self.assertTrue(actual)

        self.mock_tables.insert.assert_called_with(
            projectId=self.project, datasetId=self.dataset, body=body)

        self.mock_tables.insert.return_value.execute. \
            assert_called_with(num_retries=self.num_retries)

    def test_delete_table(self):
        self.mock_tables.delete.return_value.execute.side_effect = [{
            'status': 'foo'}, {'status': 'bar'}]

        actual = self.client.delete_table(self.dataset, self.table)

        self.assertTrue(actual)

        self.mock_tables.delete.assert_called_with(
            projectId=self.project, datasetId=self.dataset, tableId=self.table)

        self.mock_tables.delete.return_value.execute. \
            assert_called_with(num_retries=self.num_retries)

    def test_push(self):
        self.mock_table_data.insertAll.return_value.execute.return_value = {
            'status': 'foo'}

        actual = self.client.push_rows(self.dataset, self.table, self.rows,
                                       'one')

        self.assertTrue(actual)

        self.mock_bq_service.tabledata.assert_called_with()

        self.mock_table_data.insertAll.assert_called_with(
            projectId=self.project, datasetId=self.dataset, tableId=self.table,
            body=self.data)

        execute_calls = [mock.call(num_retries=self.num_retries)]
        self.mock_table_data.insertAll.return_value.execute.assert_has_calls(
            execute_calls)

    def test_dataset_create(self):
        body = {
            'datasetReference': {
                'datasetId': self.dataset,
                'projectId': self.project},
            'friendlyName': self.friendly_name,
            'description': self.description,
            'access': self.access
        }

        self.mock_datasets.insert.return_value.execute.side_effect = [{
            'status': 'foo'}, {'status': 'bar'}]

        actual = self.client.create_dataset(self.dataset,
                                            self.friendly_name,
                                            self.description,
                                            self.access)
        self.assertTrue(actual)

        self.mock_datasets.insert.assert_called_with(
            projectId=self.project, body=body)

        self.mock_datasets.insert.return_value.execute. \
            assert_called_with(num_retries=self.num_retries)

    def test_delete_datasets(self):
        self.mock_datasets.delete.return_value.execute.side_effect = [{
            'status': 'foo'}, {'status': 'bar'}]

        actual = self.client.delete_dataset(self.dataset)

        self.assertTrue(actual)

        self.mock_datasets.delete.assert_called_with(
            projectId=self.project, datasetId=self.dataset,
            deleteContents=False)

        self.mock_datasets.delete.return_value.execute. \
            assert_called_with(num_retries=self.num_retries)

    def test_dataset_update(self):
        self.mock_datasets.update.return_value.execute.side_effect = [{
            'status': 'foo'}, {'status': 'bar'}]

        actual = self.client.update_dataset(self.dataset,
                                            self.friendly_name,
                                            self.description,
                                            self.access)
        self.assertTrue(actual)

        self.mock_datasets.update.return_value.execute. \
            assert_called_with(num_retries=self.num_retries)

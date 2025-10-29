""" Test cases for opencost-parquet-exporter."""
import unittest
from unittest.mock import patch, MagicMock, mock_open
import json
import os
import requests
from azure.identity import CredentialUnavailableError
from freezegun import freeze_time
from opencost_parquet_exporter import get_config, request_data, load_config_file
from storage.azure_storage import AzureStorage


class TestGetConfig(unittest.TestCase):
    """Test cases for get_config method"""

    def test_get_aws_config_with_env_vars(self):
        """Test get_config returns correct configurations based on environment variables."""
        with patch.dict(os.environ, {
                'OPENCOST_PARQUET_SVC_HOSTNAME': 'testhost',
                'OPENCOST_PARQUET_SVC_PORT': '8080',
                'OPENCOST_PARQUET_WINDOW_START': '2020-01-01T00:00:00Z',
                'OPENCOST_PARQUET_WINDOW_END': '2020-01-01T23:59:59Z',
                'OPENCOST_PARQUET_S3_BUCKET': 's3://test-bucket',
                'OPENCOST_PARQUET_FILE_KEY_PREFIX': 'test-prefix/',
                'OPENCOST_PARQUET_AGGREGATE': 'namespace',
                'OPENCOST_PARQUET_STEP': '1m',
                'OPENCOST_PARQUET_STORAGE_BACKEND': 'aws'}, clear=True):
            config = get_config()

            self.assertEqual(
                config['url'], 'http://testhost:8080/allocation/compute')
            self.assertEqual(config['params'][0][1],
                             '2020-01-01T00:00:00Z,2020-01-01T23:59:59Z')
            self.assertEqual(config['s3_bucket'], 's3://test-bucket')
            self.assertEqual(config['storage_backend'], 'aws')
            self.assertEqual(config['params'][1][1], 'false')
            self.assertEqual(config['params'][2][1], 'false')

    def test_get_azure_config_with_env_vars(self):
        """Test get_config returns correct configurations based on environment variables."""
        with patch.dict(os.environ, {
                'OPENCOST_PARQUET_SVC_HOSTNAME': 'testhost',
                'OPENCOST_PARQUET_SVC_PORT': '8080',
                'OPENCOST_PARQUET_WINDOW_START': '2020-01-01T00:00:00Z',
                'OPENCOST_PARQUET_WINDOW_END': '2020-01-01T23:59:59Z',
                'OPENCOST_PARQUET_S3_BUCKET': 's3://test-bucket',
                'OPENCOST_PARQUET_FILE_KEY_PREFIX': 'test-prefix/',
                'OPENCOST_PARQUET_AGGREGATE': 'namespace',
                'OPENCOST_PARQUET_STEP': '1m',
                'OPENCOST_PARQUET_STORAGE_BACKEND': 'azure',
                'OPENCOST_PARQUET_AZURE_STORAGE_ACCOUNT_NAME': 'testaccount',
                'OPENCOST_PARQUET_AZURE_CONTAINER_NAME': 'testcontainer',
                'OPENCOST_PARQUET_AZURE_TENANT': 'testtenant',
                'OPENCOST_PARQUET_AZURE_APPLICATION_ID': 'testid',
                'OPENCOST_PARQUET_AZURE_APPLICATION_SECRET': 'testsecret',
                'OPENCOST_PARQUET_IDLE_BY_NODE': 'true',
                'OPENCOST_PARQUET_INCLUDE_IDLE': 'true'}, clear=True):
            config = get_config()

            self.assertEqual(
                config['url'], 'http://testhost:8080/allocation/compute')
            self.assertEqual(config['params'][0][1],
                             '2020-01-01T00:00:00Z,2020-01-01T23:59:59Z')
            self.assertEqual(config['storage_backend'], 'azure')
            self.assertEqual(
                config['azure_storage_account_name'], 'testaccount')
            self.assertEqual(config['azure_container_name'], 'testcontainer')
            self.assertEqual(config['azure_tenant'], 'testtenant')
            self.assertEqual(config['azure_application_id'], 'testid')
            self.assertEqual(config['azure_application_secret'], 'testsecret')
            self.assertEqual(config['azure_auth_mode'], 'auto')
            self.assertEqual(config['params'][1][1], 'true')
            self.assertEqual(config['params'][2][1], 'true')

    def test_get_azure_config_workload_identity_mode(self):
        """Test get_config populates workload identity defaults when requested."""
        with patch.dict(os.environ, {
                'OPENCOST_PARQUET_STORAGE_BACKEND': 'azure',
                'OPENCOST_PARQUET_AZURE_STORAGE_ACCOUNT_NAME': 'testaccount',
                'OPENCOST_PARQUET_AZURE_CONTAINER_NAME': 'testcontainer',
                'OPENCOST_PARQUET_AZURE_AUTH_MODE': 'workload-identity',
                'AZURE_CLIENT_ID': 'identity-client-id',
                'AZURE_TENANT_ID': 'identity-tenant-id',
                'AZURE_FEDERATED_TOKEN_FILE': '/var/run/secrets/azure/tokens/azure-identity-token',
                'OPENCOST_PARQUET_FILE_KEY_PREFIX': 'prefix/',
                'OPENCOST_PARQUET_WINDOW_START': '2020-01-01T00:00:00Z',
                'OPENCOST_PARQUET_WINDOW_END': '2020-01-01T23:59:59Z'}, clear=True):
            config = get_config()

            self.assertEqual(config['azure_auth_mode'], 'workload-identity')
            self.assertEqual(config['azure_storage_account_name'], 'testaccount')
            self.assertEqual(config['azure_container_name'], 'testcontainer')
            self.assertEqual(config['azure_application_id'], 'identity-client-id')
            self.assertEqual(config['azure_tenant'], 'identity-tenant-id')
            self.assertEqual(config['azure_federated_token_file'], '/var/run/secrets/azure/tokens/azure-identity-token')

    def test_get_azure_config_with_opencost_token_file_precedence(self):
        """Test OPENCOST_PARQUET_AZURE_FEDERATED_TOKEN_FILE takes precedence over AZURE_FEDERATED_TOKEN_FILE."""
        with patch.dict(os.environ, {
                'OPENCOST_PARQUET_STORAGE_BACKEND': 'azure',
                'OPENCOST_PARQUET_AZURE_STORAGE_ACCOUNT_NAME': 'testaccount',
                'OPENCOST_PARQUET_AZURE_CONTAINER_NAME': 'testcontainer',
                'OPENCOST_PARQUET_AZURE_AUTH_MODE': 'workload-identity',
                'AZURE_CLIENT_ID': 'identity-client-id',
                'AZURE_TENANT_ID': 'identity-tenant-id',
                'OPENCOST_PARQUET_AZURE_FEDERATED_TOKEN_FILE': '/custom/token/path',
                'AZURE_FEDERATED_TOKEN_FILE': '/var/run/secrets/azure/tokens/azure-identity-token',
                'OPENCOST_PARQUET_FILE_KEY_PREFIX': 'prefix/',
                'OPENCOST_PARQUET_WINDOW_START': '2020-01-01T00:00:00Z',
                'OPENCOST_PARQUET_WINDOW_END': '2020-01-01T23:59:59Z'}, clear=True):
            config = get_config()

            self.assertEqual(config['azure_federated_token_file'], '/custom/token/path')

    def test_get_gcp_config_with_env_vars(self):
        """Test get_config returns correct configurations based on environment variables."""
        with patch.dict(os.environ, {
            'OPENCOST_PARQUET_SVC_HOSTNAME': 'testhost',
            'OPENCOST_PARQUET_SVC_PORT': '8080',
            'OPENCOST_PARQUET_WINDOW_START': '2020-01-01T00:00:00Z',
            'OPENCOST_PARQUET_WINDOW_END': '2020-01-01T23:59:59Z',
            'OPENCOST_PARQUET_S3_BUCKET': 's3://test-bucket',
            'OPENCOST_PARQUET_FILE_KEY_PREFIX': 'test-prefix/',
            'OPENCOST_PARQUET_AGGREGATE': 'namespace',
            'OPENCOST_PARQUET_STEP': '1m',
            'OPENCOST_PARQUET_STORAGE_BACKEND': 'gcp',
            'OPENCOST_PARQUET_GCP_BUCKET_NAME': 'testbucket',
            'OPENCOST_PARQUET_GCP_CREDENTIALS_JSON': '{"type": "service_account"}',
            'OPENCOST_PARQUET_IDLE_BY_NODE': 'true',
                'OPENCOST_PARQUET_INCLUDE_IDLE': 'true'}, clear=True):
            config = get_config()

            self.assertEqual(
                config['url'], 'http://testhost:8080/allocation/compute')
            self.assertEqual(config['params'][0][1],
                             '2020-01-01T00:00:00Z,2020-01-01T23:59:59Z')
            self.assertEqual(config['storage_backend'], 'gcp')
            self.assertEqual(
                config['gcp_bucket_name'], 'testbucket')
            self.assertEqual(config['gcp_credentials'], {
                             'type': 'service_account'})
            self.assertEqual(config['params'][1][1], 'true')
            self.assertEqual(config['params'][2][1], 'true')

    @freeze_time("2024-01-31")
    def test_get_config_defaults_last_day_of_month(self):
        """Test get_config returns correct defaults when no env vars are set."""
        with patch.dict(os.environ, {}, clear=True):
            yesterday = '2024-01-30'
            window_start = yesterday+'T00:00:00Z'
            window_end = yesterday+'T23:59:59Z'
            window = f"{window_start},{window_end}"

            config = get_config()
            self.assertEqual(
                config['url'], 'http://localhost:9003/allocation/compute')
            self.assertTrue(config['file_key_prefix'], '/tmp/')
            self.assertNotIn('s3_bucket', config)
            self.assertEqual(config['params'][0][1], window)

    @freeze_time("2024-02-01")
    def test_get_config_defaults_first_day_of_month(self):
        """Test get_config returns correct defaults when no env vars are set."""
        with patch.dict(os.environ, {}, clear=True):
            yesterday = '2024-01-31'
            window_start = yesterday+'T00:00:00Z'
            window_end = yesterday+'T23:59:59Z'
            window = f"{window_start},{window_end}"

            config = get_config()
            self.assertEqual(
                config['url'], 'http://localhost:9003/allocation/compute')
            self.assertTrue(config['file_key_prefix'], '/tmp/')
            self.assertNotIn('s3_bucket', config)
            self.assertEqual(config['params'][0][1], window)

    @freeze_time("2024-01-11")
    def test_get_config_no_window_start(self):
        """Test get_config returns correct defaults when window start
        is not set. It should set window to yesterday"""
        with patch.dict(os.environ, {
                'OPENCOST_PARQUET_WINDOW_END': '2020-01-01T23:59:59Z'
        }, clear=True):
            yesterday = '2024-01-10'
            window_start = yesterday+'T00:00:00Z'
            window_end = yesterday+'T23:59:59Z'
            window = f"{window_start},{window_end}"

            config = get_config()
            self.assertEqual(
                config['url'], 'http://localhost:9003/allocation/compute')
            self.assertTrue(config['file_key_prefix'], '/tmp/')
            self.assertNotIn('s3_bucket', config)
            self.assertEqual(config['params'][0][1], window)

    @freeze_time('2024-12-20')
    def test_get_config_no_window_end(self):
        """Test get_config returns correct defaults when window end
        is not set. It should set window to yesterday"""
        with patch.dict(os.environ, {
                'OPENCOST_PARQUET_WINDOW_START': '2020-01-01T00:00:00Z'
        }, clear=True):
            yesterday = '2024-12-19'
            window_start = yesterday+'T00:00:00Z'
            window_end = yesterday+'T23:59:59Z'
            window = f"{window_start},{window_end}"

            config = get_config()
            self.assertEqual(
                config['url'], 'http://localhost:9003/allocation/compute')
            self.assertTrue(config['file_key_prefix'], '/tmp/')
            self.assertNotIn('s3_bucket', config)
            self.assertEqual(config['params'][0][1], window)


class TestRequestData(unittest.TestCase):
    """ Test request_data method """
    @patch('opencost_parquet_exporter.requests.get')
    def test_request_data_success(self, mock_get):
        """Test request_data successfully retrieves data when response is OK."""
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.headers = {'content-type': 'application/json'}
        mock_response.json.return_value = {'data': [{'key': 'value'}]}
        mock_get.return_value = mock_response

        config = {
            'url': 'http://testurl',
            'params': (
                ('sample_param',  'value')
            )
        }
        data = request_data(config)
        self.assertEqual(data, [{'key': 'value'}])

    @patch('opencost_parquet_exporter.requests.get')
    def test_request_data_wrong_content_type(self, mock_get):
        """Test request_data successfully retrieves data when response is OK."""
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.json.return_value = {'data': ['something']}
        mock_get.return_value = mock_response

        config = {
            'url': 'http://testurl',
            'params': (
                ('sample_param',  'value')
            )
        }
        data = request_data(config)
        self.assertEqual(data, None)

    @patch('opencost_parquet_exporter.requests.get')
    def test_request_data_failure(self, mock_get):
        """Test request_data returns None when there is a RequestException."""
        mock_get.side_effect = requests.RequestException

        config = {
            'url': 'http://testurl',
            'params': (
                ('sample_param',  'value')
            )
        }
        data = request_data(config)
        self.assertIsNone(data)


class TestLoadConfigMaps(unittest.TestCase):
    """Test cases for load_config_file method"""

    def setUp(self):
        # Setup a temporary directory and example config data for testing
        self.test_dir = 'test_configs'
        os.makedirs(self.test_dir, exist_ok=True)
        self.valid_json_path = os.path.join(self.test_dir, 'valid_config.json')
        self.invalid_json_path = os.path.join(
            self.test_dir, 'invalid_config.json')
        self.empty_json_path = os.path.join(self.test_dir, 'empty.json')

        # Create a valid JSON file
        with open(self.valid_json_path, 'w', encoding='utf-8') as f:
            json.dump({"key": "value"}, f)

        # Create an invalid JSON file
        with open(self.invalid_json_path, 'w', encoding='utf-8') as f:
            f.write('{"key": "value",}')

        # Create an empty JSON file
        with open(self.empty_json_path, 'w', encoding='utf-8') as f:
            pass

    def tearDown(self):
        # Remove the directory after tests
        for file in os.listdir(self.test_dir):
            os.remove(os.path.join(self.test_dir, file))
        os.rmdir(self.test_dir)

    def test_successful_load(self):
        """ Test loading a valid JSON file """
        result = load_config_file(self.valid_json_path)
        self.assertEqual(result, {"key": "value"})

    def test_file_not_found(self):
        """ Test the response when the JSON file does not exist """
        with self.assertRaises(FileNotFoundError):
            load_config_file('nonexistent.json')

    def test_permission_error(self):
        """ Test the response to inadequate file permissions """
        # Simulate permission error by patching os.open
        with patch('builtins.open', mock_open()) as mocked_file:
            mocked_file.side_effect = PermissionError("Permission denied")
            with self.assertRaises(PermissionError):
                load_config_file(self.valid_json_path)

    def test_invalid_json_format(self):
        """ Test how it handles a file with invalid JSON """
        with self.assertRaises(json.JSONDecodeError):
            load_config_file(self.invalid_json_path)

    def test_empty_file(self):
        """ Test the function's response to an empty JSON file """
        with self.assertRaises(json.JSONDecodeError):
            load_config_file(self.empty_json_path)


class TestAzureStorageCredentialSelection(unittest.TestCase):
    """Test credential selection logic for Azure storage backend."""

    def setUp(self):
        self.azure_storage = AzureStorage()

    @patch('storage.azure_storage.WorkloadIdentityCredential')
    @patch('storage.azure_storage.ClientSecretCredential')
    def test_auto_prefers_client_secret_when_secret_present(
            self, mock_client_secret, mock_workload):
        """Ensure client secret path is used when credentials provided."""
        config = {
            'azure_auth_mode': 'auto',
            'azure_tenant': 'tenant',
            'azure_application_id': 'app-id',
            'azure_application_secret': 'secret',
        }
        credential = self.azure_storage._build_credentials(config)

        mock_client_secret.assert_called_once_with('tenant', 'app-id', 'secret')
        mock_workload.assert_not_called()
        self.assertEqual(credential, mock_client_secret.return_value)

    @patch('storage.azure_storage.WorkloadIdentityCredential')
    def test_workload_identity_mode_uses_env_values(self, mock_workload):
        """Ensure workload identity credential is built from environment."""
        config = {
            'azure_auth_mode': 'workload-identity',
            'azure_application_secret': None,
            'azure_tenant': None,
            'azure_application_id': None,
            'azure_federated_token_file': None,
        }
        with patch.dict(os.environ, {
                'AZURE_CLIENT_ID': 'workload-client-id',
                'AZURE_TENANT_ID': 'workload-tenant',
                'AZURE_FEDERATED_TOKEN_FILE': '/tmp/token'}, clear=True):
            credential = self.azure_storage._build_credentials(config)

        mock_workload.assert_called_once_with(
            client_id='workload-client-id',
            tenant_id='workload-tenant',
            token_file_path='/tmp/token'
        )
        self.assertEqual(credential, mock_workload.return_value)

    @patch('storage.azure_storage.WorkloadIdentityCredential')
    def test_workload_identity_mode_uses_config_values(self, mock_workload):
        """Ensure workload identity credential is built from config dict."""
        config = {
            'azure_auth_mode': 'workload-identity',
            'azure_application_secret': None,
            'azure_tenant': 'config-tenant',
            'azure_application_id': 'config-client-id',
            'azure_federated_token_file': '/config/token',
        }
        with patch.dict(os.environ, {}, clear=True):
            credential = self.azure_storage._build_credentials(config)

        mock_workload.assert_called_once_with(
            client_id='config-client-id',
            tenant_id='config-tenant',
            token_file_path='/config/token'
        )
        self.assertEqual(credential, mock_workload.return_value)

    @patch('storage.azure_storage.DefaultAzureCredential')
    @patch('storage.azure_storage.WorkloadIdentityCredential')
    def test_auto_falls_back_to_default_when_workload_unavailable(
            self, mock_workload, mock_default):
        """Ensure auto mode falls back when workload identity env is missing."""
        config = {
            'azure_auth_mode': 'auto',
            'azure_application_secret': None,
            'azure_tenant': None,
            'azure_application_id': None,
        }
        with patch.dict(os.environ, {}, clear=True):
            credential = self.azure_storage._build_credentials(config)

        mock_workload.assert_not_called()
        mock_default.assert_called_once_with(
            exclude_interactive_browser_credential=True)
        self.assertEqual(credential, mock_default.return_value)

    def test_client_secret_mode_missing_values_raises(self):
        """Ensure missing client-secret config raises an error."""
        config = {
            'azure_auth_mode': 'client-secret',
            'azure_application_secret': None,
            'azure_tenant': None,
            'azure_application_id': None,
        }
        with self.assertRaises(ValueError):
            self.azure_storage._build_credentials(config)


if __name__ == '__main__':
    unittest.main()

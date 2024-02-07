""" Test cases for opencost-parquet-exporter."""
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
import os
import requests
from opencost_parquet_exporter import get_config, request_data

class TestGetConfig(unittest.TestCase):
    """Test cases for get_config method"""
    def test_get_config_with_env_vars(self):
        """Test get_config returns correct configurations based on environment variables."""
        with patch.dict(os.environ, {
                'OPENCOST_PARQUET_SVC_HOSTNAME': 'testhost',
                'OPENCOST_PARQUET_SVC_PORT': '8080',
                'OPENCOST_PARQUET_WINDOW_START': '2020-01-01T00:00:00Z',
                'OPENCOST_PARQUET_WINDOW_END': '2020-01-01T23:59:59Z',
                'OPENCOST_PARQUET_S3_BUCKET': 's3://test-bucket',
                'OPENCOST_PARQUET_FILE_KEY_PREFIX': 'test-prefix/',
                'OPENCOST_PARQUET_AGGREGATE': 'namespace',
                'OPENCOST_PARQUET_STEP': '1m'}, clear=True):
            config = get_config()
            print(config)
            self.assertEqual(config['url'], 'http://testhost:8080/allocation/compute')
            self.assertEqual(config['params'][0][1], '2020-01-01T00:00:00Z,2020-01-01T23:59:59Z')
            self.assertEqual(config['s3_bucket'], 's3://test-bucket')

    def test_get_config_defaults(self):
        """Test get_config returns correct defaults when no env vars are set."""
        with patch.dict(os.environ, {}, clear=True):
            yesterday = datetime.strftime(
                datetime.now() - timedelta(1), '%Y-%m-%d')
            window_start = yesterday+'T00:00:00Z'
            window_end = yesterday+'T23:59:59Z'
            window = f"{window_start},{window_end}"

            config = get_config()
            self.assertEqual(config['url'], 'http://localhost:9003/allocation/compute')
            self.assertTrue(config['file_key_prefix'], '/tmp/')
            self.assertNotIn('s3_bucket', config)
            self.assertEqual(config['params'][0][1], window)

    def test_get_config_no_window_start(self):
        """Test get_config returns correct defaults when window start
        is not set. It should set window to yesterday"""
        with patch.dict(os.environ, {
                'OPENCOST_PARQUET_WINDOW_END': '2020-01-01T23:59:59Z'
        }, clear=True):
            yesterday = datetime.strftime(
                datetime.now() - timedelta(1), '%Y-%m-%d')
            window_start = yesterday+'T00:00:00Z'
            window_end = yesterday+'T23:59:59Z'
            window = f"{window_start},{window_end}"

            config = get_config()
            self.assertEqual(config['url'], 'http://localhost:9003/allocation/compute')
            self.assertTrue(config['file_key_prefix'], '/tmp/')
            self.assertNotIn('s3_bucket', config)
            self.assertEqual(config['params'][0][1], window)
    def test_get_config_no_window_end(self):
        """Test get_config returns correct defaults when window end
        is not set. It should set window to yesterday"""
        with patch.dict(os.environ, {
                'OPENCOST_PARQUET_WINDOW_START': '2020-01-01T00:00:00Z'
        }, clear=True):
            yesterday = datetime.strftime(
                datetime.now() - timedelta(1), '%Y-%m-%d')
            window_start = yesterday+'T00:00:00Z'
            window_end = yesterday+'T23:59:59Z'
            window = f"{window_start},{window_end}"

            config = get_config()
            self.assertEqual(config['url'], 'http://localhost:9003/allocation/compute')
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


if __name__ == '__main__':
    unittest.main()

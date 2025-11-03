import unittest
from unittest.mock import patch, mock_open, MagicMock
import csv_load

class TestCsvLoad(unittest.TestCase):
    def test_open_file_success(self):
        m = mock_open(read_data='test content')
        with patch('builtins.open', m):
            result = csv_load.open_file('dummy.txt')
            self.assertEqual(result, 'test content')

    def test_open_file_failure(self):
        with patch('builtins.open', side_effect=FileNotFoundError):
            with self.assertRaises(FileNotFoundError):
                csv_load.open_file('missing.txt')

    @patch('csv_load.open_file', return_value='db_path: test_db')
    @patch('csv_load.yaml.safe_load', return_value={'test': {'DATABASE_PATH': 'db', 'SCHEMA': 's', 'FILE_PATH': 'f', 'TABLE_NAME': 't'}})
    def test_load_config_success(self, mock_yaml, mock_open_file):
        config = csv_load.load_config('dummy.yaml')
        self.assertIn('test', config)

    @patch('csv_load.duckdb.connect')
    def test_load_csv_to_duckdb(self, mock_connect):
        mock_con = MagicMock()
        mock_connect.return_value = mock_con
        mock_con.execute.side_effect = [None, MagicMock(fetchone=MagicMock(return_value=[0])), None]
        # Should not raise
        csv_load.load_csv_to_duckdb('db', 'schema', 'csv', 'table')
        self.assertTrue(mock_con.execute.called)

    @patch('csv_load.load_csv_to_duckdb')
    def test_process_config(self, mock_load_csv):
        config = {'test': {'DATABASE_PATH': 'db', 'SCHEMA': 's', 'FILE_PATH': 'f', 'TABLE_NAME': 't'}}
        csv_load.process_config(config)
        mock_load_csv.assert_called_once_with('db', 's', 'f', 't')

if __name__ == '__main__':
    unittest.main()

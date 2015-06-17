import os
import unittest
from datetime import datetime

import itab

TEST_SCHEMA = {'fields': {
    'integer': {'reader': 'int(x)', 'writer': 'str(int(x))', 'validator': 'x > 10'},
    'date':	{'reader': "date(x, '%Y-%m-%d %H:%M:%S')", 'writer': "x.strftime(format='%Y-%m-%d %H:%M:%S')", 'validator': 'x.month > 1'}
    }
}

TEST_HEADERS = ['integer', 'date']
TEST_FILE_01 = 'test_writer_1.tsv.gz'
TEST_FILE_02 = 'test_writer_2.tsv.gz'

rows = [
    [11, datetime(2015, 6, 17, 11, 14, 4)],
    [12, datetime(2015, 6, 7, 6, 14, 4)]
]

class WriteTest(unittest.TestCase):

    def test_writer(self):

        with itab.writer(TEST_FILE_01, headers=TEST_HEADERS, schema=TEST_SCHEMA) as writer:
            for row in rows:
                errors = writer.writerow(row)
                self.assertEqual(len(errors), 0)

        self.assertFile(TEST_FILE_01)

    def test_dictwriter(self):

        with itab.DictWriter(TEST_FILE_02, headers=TEST_HEADERS, schema=TEST_SCHEMA) as writer:
            for row in rows:
                # Convert to a dict
                row_dict = {h: row[i] for i, h in enumerate(TEST_HEADERS)}
                errors = writer.writerow(row_dict)
                self.assertEqual(len(errors), 0)
        self.assertFile(TEST_FILE_02)

    def assertFile(self, file):
        read_rows = []
        with itab.reader(file, schema=TEST_SCHEMA) as reader:
            for row, errors in reader:
                read_rows.append(row)
                self.assertEqual(len(errors), 0)

        self.assertListEqual(rows, read_rows)

    def tearDown(self):
        if os.path.exists(TEST_FILE_01):
            os.remove(TEST_FILE_01)
        if os.path.exists(TEST_FILE_02):
            os.remove(TEST_FILE_02)


if __name__ == '__main__':
    unittest.main()
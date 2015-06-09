import csv
import logging
import tempfile
from urllib.request import urlretrieve
from itab.files import open_file
from functools import lru_cache

# Parser and validator imports
import re
match = re.match

from datetime import datetime
date = datetime.strptime

DEFAULT_ITAB_CACHE_FOLDER = '~/.itab'
DEFAULT_SCHEMA_DELIMITER = '\t'

@lru_cache(maxsize=40)
def _temp_schema_file(schema_url):
    schema_tmp_file = tempfile.mkstemp(prefix="itab-", suffix='-schema.tsv')[1]
    urlretrieve(schema_url, schema_tmp_file)
    return schema_tmp_file

class CSVSchema(object):

    def __init__(self, values, headers, **kwargs):

        # Load schema
        if 'schema' in kwargs and kwargs['schema'] is not None:
            schema_file = kwargs['schema']
        else:
            schema_file = values['schema']

        if schema_file.startswith("http"):
            #TODO Use a custom cache folder
            schema_file = _temp_schema_file(schema_file)

        sd = open_file(schema_file)
        self.schema = {r['HEADER']: self._init_schema(r) for r in csv.DictReader(sd, delimiter=DEFAULT_SCHEMA_DELIMITER)}

        # Check headers
        self.headers = headers
        for h in self.headers:
            if h not in self.schema:
                logging.warning("Unknown header '{}'".format(h))

    def parse_row(self, r, line_num):
        result = []
        errors = []
        for ix, x in enumerate(r):
            val, err = self._parse_cell(x, r, line_num, ix)
            result.append(val)
            if err is not None:
                errors.append(err)
        return result, errors

    def _parse_cell(self, value, row, line_num, col_num):

        err = None
        if self.headers[col_num] not in self.schema:
            return value, err
        field_schema = self.schema[self.headers[col_num]]

        # Validate the nullability of the cell
        if field_schema['NULLABLE'] is not None:
            try:
                null = True
                if (value is None):
                    null = field_schema['_nullable'](value, row)
            except:
                null = False
            finally:
                if not null:
                    err = "Nullability error at line {} column {}: {}. [value:'{}' nullability:'{}']".format(
                        line_num, col_num, field_schema['HEADER'], value, field_schema['NULLABLE']
                    )
                    return None, err

        # Parse the value
        if field_schema['PARSER'] is not None:
            try:
                value_parsed = field_schema['_parser'](value, row)
            except:
                err = "Parsing error at line {} column {}: {}. [value:'{}' parser:'{}']".format(
                    line_num, col_num+1, field_schema['HEADER'], value, field_schema['PARSER']
                )
                return None, err

        # Validate the value
        if field_schema['VALIDATOR'] is not None:
            try:
                valid = field_schema['_validator'](value_parsed, row)
            except:
                valid = False
            finally:
                if not valid:
                    err = "Validation error at line {} column {}: {}. [value:'{}' validator:'{}']".format(
                        line_num, col_num, field_schema['HEADER'], value, field_schema['VALIDATOR']
                    )

        return value_parsed, err

    @staticmethod
    def _init_schema(s):
        try:
            if 'NULLABLE' in s:
                s['_nullable'] = eval("lambda x, r: bool({})".format(s['NULLABLE']))
            else:
                s['_nullable'] = lambda x, r: True
        except:
            logging.error("Bad validator cell '" + s['VALIDATOR'] + "'" )
            raise

        try:
            if 'PARSER' in s:
                s['_parser'] = eval("lambda x, r: {}".format(s['PARSER']))
            else:
                s['_parser'] = lambda x, r: x
        except:
            logging.error("Bad parser cell  '" + s['PARSER'] + "'" )
            raise

        try:
            if 'VALIDATOR' in s:
                s['_validator'] = eval("lambda x, r: bool({})".format(s['VALIDATOR']))
            else:
                s['_validator'] = lambda x, r: True
        except:
            logging.error("Bad validator cell '" + s['VALIDATOR'] + "'" )
            raise

        return s


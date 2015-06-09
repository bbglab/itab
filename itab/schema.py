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

        self.headers = headers

        # Load schema
        if 'schema' in kwargs and kwargs['schema'] is not None:
            schema_value = kwargs['schema']

            if type(schema_value) == dict:
                self.schema = schema_value
                self.schema['fields'] = {k: self._init_schema(k, v) for k, v in schema_value['fields'].items()}
                return
            else:
                schema_file = schema_value

        elif 'schema' in values:
            schema_file = values['schema']
        else:
            self.schema = {'fields': {}}
            return

        if schema_file.startswith("http"):
            #TODO Use a custom cache folder
            schema_file = _temp_schema_file(schema_file)

        sd = open_file(schema_file)
        self.schema = {
            'fields': {r['HEADER']: self._init_schema(r['HEADER'], r) for r in csv.DictReader(sd, delimiter=DEFAULT_SCHEMA_DELIMITER)}
        }

        # Check headers
        for h in self.headers:
            if h not in self.schema['fields']:
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

    def _header_id(self, col_num):
        if col_num >= len(self.headers):
            return "{}".format(col_num)
        else:
            return self.headers[col_num]

    def _field_schema(self, col_num):
        return self.schema['fields'].get(self._header_id(col_num), None)

    def _parse_cell(self, value, row, line_num, col_num):

        err = None
        field_schema = self._field_schema(col_num)

        # If we allow columns without schema
        if field_schema is None:
            return value, err

        # Validate the nullability of the cell
        try:
            null = True
            if (value is None):
                null = field_schema['_nullable'](value, row)
        except:
            null = False
        finally:
            if not null:
                err = "Nullability error at line {} column {}: {}. [value:'{}' nullability:'{}']".format(
                    line_num, col_num+1, field_schema.get('HEADER', None), value, field_schema.get('NULLABLE', None)
                )
                return None, err

        # Parse the value
        try:
            value_parsed = field_schema['_parser'](value, row)
        except:
            err = "Parsing error at line {} column {}: {}. [value:'{}' parser:'{}']".format(
                line_num, col_num+1, field_schema.get('HEADER', None), value, field_schema.get('PARSER', None)
            )
            return None, err

        # Validate the value
        try:
            valid = field_schema['_validator'](value_parsed, row)
        except:
            valid = False
        finally:
            if not valid:
                err = "Validation error at line {} column {}: {}. [value:'{}' validator:'{}']".format(
                    line_num, col_num+1, field_schema.get('HEADER', None), value, field_schema.get('VALIDATOR', None)
                )

        return value_parsed, err

    @staticmethod
    def _init_schema(k, s):
        try:
            if '_nullable' not in s:
                if 'NULLABLE' in s:
                    s['_nullable'] = eval("lambda x, r: bool({})".format(s['NULLABLE']))
                else:
                    s['_nullable'] = lambda x, r: True
        except:
            logging.error("Bad nullable cell '" + s.get('VALIDATOR', None) + "'" )
            raise

        try:
            if '_parser' not in s:
                if 'PARSER' in s:
                    s['_parser'] = eval("lambda x, r: {}".format(s['PARSER']))
                else:
                    s['_parser'] = lambda x, r: x
        except:
            logging.error("Bad parser cell  '" + s.get('PARSER', None) + "'" )
            raise

        try:
            if '_validator' not in s:
                if 'VALIDATOR' in s:
                    s['_validator'] = eval("lambda x, r: bool({})".format(s['VALIDATOR']))
                else:
                    s['_validator'] = lambda x, r: True
        except:
            logging.error("Bad validator cell '" + s.get('VALIDATOR', None) + "'" )
            raise

        if 'HEADER' not in s:
            s['HEADER'] = k

        return s
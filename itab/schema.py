import csv
import logging
from itab.files import open_file

from datetime import datetime
date = datetime.strptime

DEFAULT_SCHEMA_DELIMITER = '\t'

class CSVSchema(object):

    def __init__(self, values, headers):

        # Load schema
        schema_file = values['SCHEMA']
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
        field_schema = self.schema[self.headers[col_num]]

        # Parse the value
        try:
            value_parsed = field_schema['_parser'](value, row)
        except:
            err = "Parsing error at line {} column {}. [value:'{}' parser:'{}']".format(
                line_num, col_num+1, value, field_schema['PARSER']
            )
            return None, err

        # Validate the value
        try:
            valid = field_schema['_validator'](value_parsed, row)
        except:
            valid = False
        finally:
            if not valid:
                err = "Validation error at line {} column {}. [value:'{}' validator:'{}']".format(
                    line_num, col_num, value, field_schema['VALIDATOR']
                )

        return value_parsed, err

    @staticmethod
    def _init_schema(s):
        if 'PARSER' in s:
            s['_parser'] = eval("lambda x, r: {}".format(s['PARSER']))
        else:
            s['_parser'] = lambda x, r: x

        if 'VALIDATOR' in s:
            s['_validator'] = eval("lambda x, r: {}".format(s['VALIDATOR']))
        else:
            s['_validator'] = lambda x, r: True
        return s


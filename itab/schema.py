import csv
import logging
import os
import tempfile
from urllib.request import urlretrieve
from itab.files import open_file
from functools import lru_cache

# Parser and validator imports
import re
match = re.match

from datetime import datetime
date = datetime.strptime

# Schema headers
SCHEMA_HEADER = 'header'
SCHEMA_READER = 'reader'
SCHEMA_READER_EVAL = '_reader'
SCHEMA_WRITER = 'writer'
SCHEMA_WRITER_EVAL = '_writer'
SCHEMA_VALIDATOR = 'validator'
SCHEMA_VALIDATOR_EVAL = '_validator'
SCHEMA_NULLABLE = 'nullable'
SCHEMA_NULLABLE_EVAL = '_nullable'
SCHEMA_HELP = 'help'

# Default behaviours
DEFAULT_NULLABLE = lambda x, r: True
DEFAULT_READER = lambda x, r: x
DEFAULT_WRITER = lambda x, r: "{}".format(x)
DEFAULT_VALIDATOR = lambda x, r: True

DEFAULT_ITAB_CACHE_FOLDER = '~/.itab'

DEFAULT_NULL_TOKEN = ''
DEFAULT_DELIMITER = '\t'
DEFAULT_SCHEMA_DELIMITER = '\t'

@lru_cache(maxsize=40)
def _temp_schema_file(schema_url):
    schema_tmp_file = tempfile.mkstemp(prefix="itab-", suffix='-schema.tsv')[1]
    urlretrieve(schema_url, schema_tmp_file)
    return schema_tmp_file


class Schema(object):

    def __init__(self, schema, headers=None, basedir=None, ignore_unknown_headers=True):

        if basedir is not None \
                and type(schema) == str \
                and not schema.startswith("http") \
                and not schema.startswith("/"):
            schema = os.path.join(basedir, schema)

        self.headers = headers
        self.schema_not_found = True
        self.schema_url = None
        _schema_headers = []

        # Load schema
        if schema is not None:

            if type(schema) == dict:
                self.schema = schema
                self.schema['fields'] = {k: self._init_schema_field(k, v) for k, v in schema['fields'].items()}
                _schema_headers = list(self.schema['fields'].keys())
            else:
                self.schema_url = schema
        else:
            self.schema = {'fields': {}}

        if self.schema_url is not None:
            if self.schema_url.startswith("http"):
                #TODO Use a custom cache folder
                schema_file = _temp_schema_file(self.schema_url)
            else:
                schema_file = self.schema_url

            if os.path.exists(schema_file):
                sd = open_file(schema_file)
                self.schema = {'fields': {}}
                _schema_headers = []
                for r in csv.DictReader(sd, delimiter=DEFAULT_SCHEMA_DELIMITER):
                    self.schema['fields'][r[SCHEMA_HEADER]] = self._init_schema_field(r[SCHEMA_HEADER], r)
                    _schema_headers.append(r[SCHEMA_HEADER])

                self.schema_not_found = False
            else:
                self.schema = {'fields': {}}

        # Check headers
        if self.headers is not None:
            for h in self.headers:
                if h not in self.schema['fields'] and not ignore_unknown_headers:
                    logging.warning("Unknown header '{}'".format(h))
        else:
            self.headers = _schema_headers

    def _header_id(self, col_num):
        if col_num >= len(self.headers):
            return "{}".format(col_num)
        else:
            return self.headers[col_num]

    def _field_schema(self, col_num):
        return self.schema['fields'].get(self._header_id(col_num), None)

    def format_cell(self, value, row, line_num, col_num, parser='reader'):

        err = None
        field_schema = self._field_schema(col_num)

        # If we allow columns without schema
        if field_schema is None:
            return value, err

        # Parse nulls if we are reading
        if parser == 'reader' and value == DEFAULT_NULL_TOKEN:
            value = None

        # Validate the nullability of the cell
        try:
            null = True
            if value is None:
                null = field_schema[SCHEMA_NULLABLE_EVAL](value, row)
        except:
            null = False
        finally:
            if not null:
                err = "Nullability error at line {} column {}: {}. [value:'{}' nullability:'{}']".format(
                    line_num, col_num, field_schema.get(SCHEMA_HEADER, None), value, field_schema.get(SCHEMA_NULLABLE, None)
                )
                return None, err

        if value is None:
            if parser == 'reader':
                return None, err
            else:
                DEFAULT_NULL_TOKEN, err

        if parser == 'reader':
            # Read the value
            try:
                value_parsed = field_schema[SCHEMA_READER_EVAL](value, row)
            except:
                err = "Reading error at line {} column {}: {}. [value:'{}' reader:'{}']".format(
                    line_num, col_num+1, field_schema.get(SCHEMA_HEADER, None), value, field_schema.get(SCHEMA_READER, None)
                )
                return None, err
        else:
            value_parsed = value

        # Validate the value
        try:
            valid = field_schema[SCHEMA_VALIDATOR_EVAL](value_parsed, row)
        except:
            valid = False
        finally:
            if not valid:
                err = "Validation error at line {} column {}: {}. [value:'{}' validator:'{}']".format(
                    line_num, col_num, field_schema.get(SCHEMA_HEADER, None), value, field_schema.get(SCHEMA_VALIDATOR, None)
                )

        if parser == 'writer':
            # Write the value
            try:
                value_parsed = field_schema[SCHEMA_WRITER_EVAL](value_parsed, row)
            except:
                err = "Writing error at line {} column {}: {}. [value:'{}' writer:'{}']".format(
                    line_num, col_num+1, field_schema.get(SCHEMA_HEADER, None), value, field_schema.get(SCHEMA_READER, None)
                )
                return None, err

        return value_parsed, err

    @staticmethod
    def _init_schema_field(k, s):

        # Check that the header id is defined
        if SCHEMA_HEADER not in s:
            s[SCHEMA_HEADER] = k

        # Initialize nullable
        try:
            if SCHEMA_NULLABLE_EVAL not in s:
                if SCHEMA_NULLABLE in s:
                    if callable(s[SCHEMA_NULLABLE]):
                        s[SCHEMA_NULLABLE_EVAL] = s[SCHEMA_NULLABLE]
                    else:
                        s[SCHEMA_NULLABLE_EVAL] = eval("lambda x, r: bool({})".format(s[SCHEMA_NULLABLE]))
                else:
                    s[SCHEMA_NULLABLE_EVAL] = DEFAULT_NULLABLE
        except:
            logging.error("Bad schema nullable cell '" + s.get(SCHEMA_NULLABLE, None) + "'" )
            raise

        # Initialize reader
        try:
            if SCHEMA_READER_EVAL not in s:
                if SCHEMA_READER in s:
                    if callable(s[SCHEMA_READER]):
                        s[SCHEMA_READER_EVAL] = s[SCHEMA_READER]
                    else:
                        if s.get(SCHEMA_READER, None) is None:
                            s[SCHEMA_READER_EVAL] = DEFAULT_READER
                        else:
                            s[SCHEMA_READER_EVAL] = eval("lambda x, r: {}".format(s[SCHEMA_READER]))
                else:
                    s[SCHEMA_READER_EVAL] = DEFAULT_READER
        except:
            logging.error("Bad schema reader cell  '" + s.get(SCHEMA_READER, None) + "'" )
            raise

        # Initialize writer
        try:
            if SCHEMA_WRITER_EVAL not in s:
                if SCHEMA_WRITER in s:
                    if callable(s[SCHEMA_WRITER]):
                        s[SCHEMA_WRITER_EVAL] = s[SCHEMA_WRITER]
                    else:
                        if s.get(SCHEMA_WRITER, None) is None:
                            s[SCHEMA_WRITER_EVAL] = DEFAULT_WRITER
                        else:
                            s[SCHEMA_WRITER_EVAL] = eval("lambda x, r: {}".format(s[SCHEMA_WRITER]))

                else:
                    s[SCHEMA_WRITER_EVAL] = DEFAULT_WRITER
        except:
            logging.error("Bad schema writer cell '" + s.get(SCHEMA_WRITER, None) + "'")
            raise

        # Initialize validator
        try:
            if SCHEMA_VALIDATOR_EVAL not in s:
                if s.get(SCHEMA_VALIDATOR, None) is not None:
                    if callable(s[SCHEMA_VALIDATOR]):
                        s[SCHEMA_VALIDATOR_EVAL] = s[SCHEMA_VALIDATOR]
                    else:
                        s[SCHEMA_VALIDATOR_EVAL] = eval("lambda x, r: bool({})".format(s[SCHEMA_VALIDATOR]))
                else:
                    s[SCHEMA_VALIDATOR_EVAL] = DEFAULT_VALIDATOR
        except:
            logging.error("Bad schema validator cell '" + s.get(SCHEMA_VALIDATOR, None) + "'")
            raise

        return s

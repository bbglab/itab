import csv
import logging
import argparse
import traceback

from datetime import datetime
from bgcore.files import open_file

DEFAULT_DELIMITER = '\t'

date = datetime.strptime

def cellcheck(value, row, schema, line_num, col_num):

    # Parse the value
    try:
        value_parsed = schema['_parser'](value, row)
    except:
        logging.error("Parsing error at line {} column {}. [value:'{}' parser:'{}']".format(
                line_num, col_num, value, schema['PARSER']
            ))
        return None
        #traceback.print_exc()

    # Validate the value
    try:
        valid = schema['_validator'](value_parsed, row)
    except:
        valid = False
        traceback.print_exc()
    finally:
        if not valid:
            logging.error("Validation error at line {} column {}. [value:'{}' validator:'{}']".format(
                line_num, col_num, value, schema['VALIDATOR']
            ))

    return value_parsed

def init_schema(s):
    if 'PARSER' in s:
        s['_parser'] = eval("lambda x, r: {}".format(s['PARSER']))
    else:
        s['_parser'] = lambda x, r: x

    if 'VALIDATOR' in s:
        s['_validator'] = eval("lambda x, r: {}".format(s['VALIDATOR']))
    else:
        s['_validator'] = lambda x, r: True
    return s

def tsvcheck(file):

    # Open a anntated and commented file iterator
    fd = open_file(file)

    # Create a CSV parser
    f_reader = csv.reader(fd, delimiter=DEFAULT_DELIMITER)

    # Load headers
    headers = next(f_reader)

    # Load TSV schema
    schema_file = fd.get_values()['SCHEMA']
    sd = open_file(schema_file)
    schema = { r['HEADER']: init_schema(r) for r in csv.DictReader(sd, delimiter=DEFAULT_DELIMITER) }

    # Check headers
    for h in headers:
        if h not in schema:
            logging.warn("Unknown header '{}'".format(h))

    # Check the file
    for l_ix, row in enumerate(f_reader, start=len(fd.get_comments())+len(fd.get_values())+2):
        for c_ix, value in enumerate(row):
            cellcheck(value, row, schema[headers[c_ix]], l_ix, c_ix+1)

if __name__ == "__main__":

    # Configure the logging
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', datefmt='%H:%M:%S')

    # Parse the arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("file")
    args = parser.parse_args()

    tsvcheck(args.file)
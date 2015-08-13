import argparse

import itab
from itab.schema import SCHEMA_HELP, SCHEMA_HEADER


def itab_help(file, schema=None):

    # Create an ITab reader
    reader = itab.reader(file, schema=schema)
    schema = reader.schema.schema

    for key, field in schema['fields'].items():
        print("{}: {}".format(field.get(SCHEMA_HEADER, key), field.get(SCHEMA_HELP, "Unknown")))


def cmdline():
    parser = argparse.ArgumentParser()
    parser.add_argument("file")
    parser.add_argument("--schema", dest="schema", default=None)
    args = parser.parse_args()

    # Show schema help
    itab_help(args.file, schema=args.schema)

if __name__ == "__main__":
    cmdline()
import argparse

import itab


def check(file, schema=None):

    # Create an ITab reader
    reader = itab.reader(file, schema=schema)

    all_errors = []
    for row, errors in reader:

        if len(errors) > 0:
            print("\n[line {}] ERRORS:".format(reader.line_num))
            for e in errors:
                print('\t', e)
            all_errors += errors

    return all_errors


def cmdline():

    # Parse the arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("file")
    parser.add_argument("--schema", dest="schema", default=None)
    args = parser.parse_args()

    errors = check(args.file, schema=args.schema)

    # Return a system error if there is any error
    ret_code = -1 if len(errors) > 0 else 0
    exit(ret_code)

if __name__ == "__main__":
    cmdline()
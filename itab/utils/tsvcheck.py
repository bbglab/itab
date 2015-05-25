import logging
import argparse

import itab

def tsvcheck(file):

    # Create an ITab reader
    reader = itab.open(file)

    for row, errors in reader:

        if len(errors) > 0:
            print("\n[line {}] ERRORS:".format(reader.line_num))
            for e in errors:
                print('\t', e)
            continue


def cmdline():

    # Parse the arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("file")
    args = parser.parse_args()

    tsvcheck(args.file)


if __name__ == "__main__":
    cmdline()
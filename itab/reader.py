import csv
import six
from itab.files import open_file
from itab.schema import CSVSchema, _temp_schema_file
import os

DEFAULT_DELIMITER = '\t'


def has_schema(fileName):

    fd = open_file(fileName)

    # Create a CSV parser
    reader = csv.reader(fd, delimiter=DEFAULT_DELIMITER)

    # Load headers
    headers = next(reader)
    metadata = fd.metadata

    if 'schema' in metadata and metadata['schema'] is not None:
        schema_file = metadata['schema']
    else:
        return False

    if schema_file.startswith("http"):
        #TODO Use a custom cache folder
        schema_file = _temp_schema_file(schema_file)

    try:
        os.stat(schema_file)
    except:
        return False

    return True


class TabReader(six.Iterator):

    def __init__(self, f, header=None, **kwargs):

        # Open an annotated and commented file iterator
        self.fd = open_file(f)

        # Create a CSV parser
        self.reader = csv.reader(self.fd, delimiter=DEFAULT_DELIMITER)

        # Load headers
        if header is None:
            self.headers = next(self.reader)
        else:
            self.headers = header

        # Load schema
        self.schema = CSVSchema(self.metadata, self.headers, **kwargs)

        # Total number of lines before first data line
        self._line_offset = len(self.comments) + len(self.metadata)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.fd.close()

    def __iter__(self):
        return self

    def __next__(self):
        return self.schema.parse_row(next(self.reader), self.line_num)

    @property
    def dialect(self):
        return self.reader.dialect

    @property
    def line_num(self):
        return self.reader.line_num + self._line_offset

    @property
    def comments(self):
        return self.fd.get_comments()

    @property
    def metadata(self):
        return self.fd.get_metadata()


class TabDictReader(TabReader):
    def __init__(self, f, restkey=None, restval=None, **kwargs):
        super().__init__(f, **kwargs)

        self.restkey = restkey          # key to catch long rows
        self.restval = restval          # default value for short rows

    def __iter__(self):
        return self

    def __next__(self):

        row, errors = super().__next__()

        # unlike the basic reader, we prefer not to return blanks,
        # because we will typically wind up with a dict full of None
        # values
        while row == []:
            row, errors = super().__next__()

        d = dict(zip(self.headers, row))
        lf = len(self.headers)
        lr = len(row)
        if lf < lr:
            d[self.restkey] = row[lf:]
        elif lf > lr:
            for key in self.headers[lr:]:
                d[key] = self.restval

        return d, errors
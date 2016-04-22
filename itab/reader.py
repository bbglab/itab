import csv
import six
from itab.files import open_file
from itab.schema import Schema, DEFAULT_DELIMITER, DEFAULT_NULL_TOKEN
import os


class TabReader(six.Iterator):

    def __init__(self, f, header=None, commentchar='#', delimiter=DEFAULT_DELIMITER, **kwargs):

        # Open an annotated and commented file iterator
        self.fd = open_file(f, commentchar=commentchar)

        # Create a CSV parser
        self.reader = csv.reader(self.fd, delimiter=delimiter)

        # Load headers
        if header is None or len(header) == 0:
            self.headers = next(self.reader)
        else:
            self.headers = header

        # Load schema
        schema_url = kwargs.get('schema', None)
        if schema_url is None:
            schema_url = self.fd.metadata.get('schema', None)

        if header is not None and len(header)==0: #Just for testing the schema inside the file
            self.schema_url = schema_url
            return

        self.schema = Schema(schema_url, headers=self.headers, basedir=os.path.dirname(f))

        # Total number of lines before first data line
        self._line_offset = len(self.comments) + len(self.metadata)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.fd.close()

    def __iter__(self):
        return self

    def __next__(self):
        row = next(self.reader)

        # Skip empty lines
        if self.schema.schema.get('skip_empty', False) and len(row) == 0:
            return self.__next__()

        l_row = len(row)
        l_headers = len(self.headers)

        if l_row < l_headers:
            row += [DEFAULT_NULL_TOKEN]*(l_headers - l_row)

        if l_row > l_headers:
            row = row[:l_headers]
            # TODO Check a configurable error

        result = []
        errors = []
        for ix, x in enumerate(row):
            val, err = self.schema.format_cell(x, row, self.line_num, ix, parser='reader')
            result.append(val)
            if err is not None:
                errors.append(err)
        return result, errors

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
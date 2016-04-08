import csv
import os
from itab.files import open_file
from itab.schema import DEFAULT_DELIMITER, Schema


class TabWriter(object):

    def __init__(self, f, schema=None, headers=None, comments=None, write_headers=False, delimiter=DEFAULT_DELIMITER):

        # Load schema
        self.schema = Schema(schema, headers=headers, basedir=os.path.dirname(f))

        # Check if the schema is a URL and save it as a comment
        if type(schema) == str:
            metadata = {'schema': schema}
        else:
            metadata = None

        # Open an annotated and commented file iterator
        self.fd = open_file(f, metadata=metadata, mode="w", comments=comments)

        # Use default python writer
        self.writer = csv.writer(self.fd, delimiter=delimiter)

        # Write header
        if write_headers:
            self.writer.writerow(self.schema.headers)
        self.line_num = 0

    def writerow(self, row):
        result = []
        errors = []
        for ix, x in enumerate(row):
            val, err = self.schema.format_cell(x, row, self.line_num, ix, parser='writer')
            result.append(val)
            if err is not None:
                errors.append(err)

        self.writer.writerow(result)
        self.line_num += 1
        return errors

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.fd.close()


class TabDictWriter(TabWriter):

    def __init__(self, file, schema=None, headers=None, extrasaction='ignore', write_headers=True):
        """
        :param file: File path
        :param schema: A file, url or python dictionary with the tab schema
        :param extrasaction: If it's equal to 'ignore', the values with a key not defined as a schema field will be
        ignored. If it's equal to 'append' the values will be added at the end of the line, but without a header.
        """
        TabWriter.__init__(self, file, schema=schema, headers=headers, write_headers=write_headers)
        self.extrasaction = extrasaction

    def writerow(self, row_dict):
        """

        :param row_dict: A dictionary with the values and the field names as keys.
        :return: A list with the writing or validation errors. An empty list if there is no error.
        """

        # Check if there is a value without a defined field
        errors = []
        for k in row_dict.keys():
            if k not in self.schema.headers:
                if self.extrasaction == 'append':
                    self.schema.headers.append(k)
                    err_msg = "You will have some extra values without header."
                else:
                    err_msg = "This values are ignored."
                errors += "The key '{}' is not a valid schema field. {}".format(k, err_msg)

        row_list = [row_dict.get(h, None) for h in self.schema.headers]

        errors += TabWriter.writerow(self, row_list)
        return errors

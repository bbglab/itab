import bz2
import gzip
import lzma
import io
import datetime


def open_file(file_name, mode=None, metadata=None, comments=None, commentchar="#", writedate=False, encoding=None, errors=None, newline=None, compresslevel=9):
    """
        It opens a file descriptor. If the file extension ends with '.gz' it will
        use the 'gzip.open' function. Otherwise it will use the builtin 'open'.

        If you pass values and/or comments it will write them as a commented header.
        If you are reading a file with a commented header it will parse them

    :param file_name: File path
    :param mode: Open mode
    :return: A file descriptor
    """

    # If we have values or comments we open the file on write mode,
    # otherwise as read mode
    if mode is None:
        mode = "r" if metadata is None and comments is None else "w"

    if file_name.endswith(".gz"):
        gz_mode = mode.replace("t", "")
        fd = gzip.GzipFile(file_name, gz_mode, compresslevel)
    elif file_name.endswith(".bz2"):
        bz_mode = mode.replace("t", "")
        fd = bz2.BZ2File(file_name, bz_mode, compresslevel=compresslevel)
    elif file_name.endswith(".xz"):
        lz_mode = mode.replace("t", "")
        fd = lzma.LZMAFile(file_name, lz_mode, format=None, check=-1, preset=None, filters=None)
    else:
        fd = io.FileIO(file_name, mode)

    return AnnotatedFile(fd,
                         metadata=metadata,
                         comments=comments,
                         commentchar=commentchar,
                         writedate=writedate,
                         encoding=encoding,
                         errors=errors,
                         newline=newline)


class AnnotatedFile(io.TextIOWrapper):
    def __init__(self, fd, metadata=None, comments=None, commentchar="#", writedate=False,
                 encoding=None, errors=None, newline=None):
        super().__init__(fd, encoding, errors, newline)

        self.metadata = metadata
        self.comments = comments

        self._commentchar = commentchar

        if writedate:
            if comments is None:
                comments = []

            comments.append(str(datetime.datetime.today()))

        if self.comments is None:
            self.comments = []

        if self.metadata is None:
            self.metadata = {}

        self.writedHeader = False

    def read(self, *args, **kwargs):
        line = super().readline()
        while line.startswith(self._commentchar):
            if line.startswith(self._commentchar + self._commentchar):
                if self.metadata is None:
                    self.metadata = {}
                try:
                    k, v = line.replace(self._commentchar + self._commentchar + " ", "").split("=")
                    self.metadata[k.strip()] = v.strip()
                except:
                    pass #It is not metadata, but it is not an error
            else:

                self.comments.append(line.replace(self._commentchar, "").strip())
            line = super().readline()

        return line

    def readline(self):
        return self.read()

    def write(self, *args, **kwargs):

        # First write the header comments
        if not self.writedHeader:

            if len(self.comments) > 0:
                super().write("\n".join([self._commentchar + " " + comment for comment in self.comments]))
                super().write("\n")

            if len(self.metadata) > 0:
                super().write("\n".join([self._commentchar + self._commentchar + " " + str(k) + "=" + str(v) for k, v in self.metadata.items()]))
                super().write("\n")

            self.writedHeader = True

        super().write(*args, **kwargs)

    def __iter__(self):
        return self

    def get_comments(self):
        return self.comments

    def get_metadata(self):
        return self.metadata



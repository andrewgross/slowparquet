from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import operator
import os
import re
import struct

import thriftpy


class ParquetFile(object):

    def __init__(self, file_location):
        self.file_location = file_location

    def _get_metadata_location(self, file_location):
        metadata_location = os.sep.join([file_location, '_metadata'])
        if not os.path.exists(metadata_location):
            metadata_location = file_location
        return file_location

    def read_metadata(self):
        metadata_location = self._get_metadata_location(self.file_location)
        with open(metadata_location, 'rb') as f:
            self._check_header_magic_number(f)
            self._check_footer_magic_number(f)
            footer_length = self._get_footer_length(f)
            metadata = self._read_footer(f, footer_length)

    def _check_header_magic_number(self, _file):
        _file.seek(0)
        self._check_magic_number(_file)

    def _check_footer_magic_number(self, _file):
        _file.seek(-4, os.SEEK_END)
        self._check_magic_number(_file)

    def _check_magic_number(self, _file):
        if _file.read(4) != b'PAR1':
            raise Exception('Missing Magic Number for {} at offset {}'.format(_file.name, _file.tell()))

    def _get_footer_length(self, _file):
        _file.seek(-8, os.SEEK_END)
        # Read Little Endian packed integer, 4 bytes wide
        return struct.unpack('<i', _file.read(4))[0]

    def _read_footer(self, _file, footer_length):
        # Account for magic number and footer length field
        footer_start = footer_length + 8
        _file.seek(-footer_start, os.SEEK_END)
        return _parse_footer(_file)

    def _parse_footer(_file):
        pass

    def __str__(self):
        return "<Parquet File: %s>" % self.info

    __repr__ = __str__

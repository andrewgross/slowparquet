from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import operator
import os
import re
import struct

from .thrift import read_metadata, read_page_header, PARQUET_THRIFT
from .schema import SchemaHelper

class ParquetFile(object):

    def __init__(self, file_location):
        self.file_location = file_location
        self.metadata = self._read_metadata()

    def _get_metadata_location(self, file_location):
        metadata_location = os.sep.join([file_location, '_metadata'])
        if not os.path.exists(metadata_location):
            metadata_location = file_location
        return file_location

    def _read_metadata(self):
        metadata_location = self._get_metadata_location(self.file_location)
        with open(metadata_location, 'rb') as f:
            self._check_header_magic_number(f)
            self._check_footer_magic_number(f)
            footer_length = self._get_footer_length(f)
            return self._read_footer(f, footer_length)

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
        return read_metadata(_file)

    def read(self, columns=None):
        """
        Read a parquet file into Python native data structures
        """
        if columns is None:
            columns = self._get_all_columns(self.metadata)
        schema = SchemaHelper(self.metadata.schema)
        with open(self.file_location, 'rb') as f:
            for row_group in self.metadata.row_groups:
                for column in row_group.columns:
                    if self._build_column_name(column) not in columns:
                        continue
                    self._move_to_page_header(f, column.meta_data)
                    page_header = read_page_header(f)
                    if page_header.type == PARQUET_THRIFT.PageType.DATA_PAGE:
                        return f, column.meta_data, page_header, schema
                        self._read_data_page(f, column.metadata, page_header, schema)
                    elif page_header.type == PARQUET_THRIFT.PageType.DATA_PAGE_V2:
                        pass
                    elif page_header.type == PARQUET_THRIFT.PageType.INDEX_PAGE:
                        pass
                    elif page_header.type == PARQUET_THRIFT.PageType.DICTIONARY_PAGE:
                        pass
                    else:
                        pass

    def _read_data_page(self, _file, column_metadata, page_header, schema):
        data_page_header = page_header.data_page_header
        # Check if the field is required
        max_repetition_level = schema.max_repetition_level(column_metadata.path_in_schema)
        max_definition_level = schema.max_definition_level(column_metadata.path_in_schema)

        is_required = schema.is_required(data_page_header.path_in_schema[-1])
        if not is_required:
            definition_levels = None
        else:
            definition_levels = get_definition

    def _get_repetition_levels(self, _file, column_metadata, page_header, schema):


    def _move_to_page_header(self, _file, metadata):
        """
        Move the file to the offset of the column page header
        """
        offset = self._get_offset(metadata)
        _file.seek(offset, os.SEEK_SET)

    def _get_offset(self, metadata):
        dict_offset = metadata.dictionary_page_offset
        data_offset = metadata.data_page_offset
        if dict_offset is None or data_offset < dict_offset:
            return data_offset
        return dict_offset

    def _build_column_name(self, column):
        return ".".join(column.meta_data.path_in_schema)

    def _get_all_columns(self, metadata):
        return [s.name for s in metadata.schema if s.type != None]

    def __str__(self):
        return "<Parquet File: %s>" % self.file_location

    __repr__ = __str__


"""thrift_filetransport.py - read thrift encoded data from a file object."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os

import thriftpy

from thriftpy.protocol.compact import TCompactProtocolFactory
from thriftpy.transport import TTransportBase, TTransportException


THRIFT_FILE = os.path.join(os.path.dirname(__file__), "parquet.thrift")
PARQUET_THRIFT = thriftpy.load(THRIFT_FILE, module_name=str("parquet_thrift"))


class TFileTransport(TTransportBase):
    """TTransportBase implementation for decoding data from a file object."""

    def __init__(self, fo):
        """Initialize with `fo`, the file object to read from."""
        self._fo = fo
        self._pos = fo.tell()

    def _read(self, sz):
        """Read data `sz` bytes."""
        return self._fo.read(sz)

    def _write(self, sz):
        return self._fo.write(sz)

    write = _write


def read_metadata(_file):
    """
    Read the file metadata, given a file at the correct offset
    """
    try:
        return read_thrift(_file, PARQUET_THRIFT.FileMetaData)
    except thriftpy.transport.TTransportException:
        raise Exception('Unable to parse metadata for {} at offset {}'.format(_file.name, _file.tell()))


def read_page_header(_file):
    """
    Read the page header, given a file ata the correct offset
    """
    try:
        return read_thrift(_file, PARQUET_THRIFT.PageHeader)
    except thriftpy.transport.TTransportException:
        raise Exception('Unable to parse metadata for {} at offset {}'.format(_file.name, _file.tell()))


def read_thrift(file_obj, ttype):
    """Read a thrift structure from the given file object"""
    tin = TFileTransport(file_obj)
    pin = TCompactProtocolFactory().get_protocol(tin)
    page_header = ttype()
    page_header.read(pin)
    return page_header

Slowparquet - A Mostly python parquet reader/wire
=========================================================================

**WARNING: This library is still in beta. It can do anything up to and
including eating your laundry.**

Slowparquet is handler for the Parquet file format. It's goal is to provide a simple
library for working with Parquet files.  It does not use Pandas to manage
data, despite that being superior for most use cases.  This is due to issues with
handling Null values in integer based columns, a requirement for work we are doing
with Parquet.  If that is not a factor, it is recommended you use **fastparquet**
which is designed to work with Pandas and take advantage of it's speedups.

Installation
------------

**Slowparquet** is available from `PyPI <https://pypi.python.org/>`__ and can
be installed in all the usual ways. To install via *CLI*:

.. code:: bash

    $ pip install slowparquet

Or just add it to your ``requirements.txt``.


Credits
-------

This library is based heavily off of **fastparquet** and it's predecessor **python-parquet**.
Credit goes to those authors who blazed the trail and created those implementations.
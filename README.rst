===========================
PandABlocks IOC Ring Buffer
===========================

.. image:: https://github.com/dmgav/PandABlocks-ioc-ringbuf/actions/workflows/testing.yml/badge.svg
   :target: https://github.com/dmgav/PandABlocks-ioc-ringbuf/actions/workflows/testing.yml


.. image:: https://img.shields.io/pypi/v/PandABlocks-ioc-ringbuf.svg
        :target: https://pypi.python.org/pypi/PandABlocks-ioc-ringbuf


Ring Buffer for PandABlocks IOC

Starting the IOC
================

.. code: bash

  pandablocks-ioc-ringbuf softioc <panda-host-name> <IOC-prefix>
  pandablocks-ioc-ringbuf softioc <panda-host-name> <IOC-prefix> --buffer-max-size=<max-buffer-size>

Maximum buffer size is the size of internal ring buffer and determines how many recent data points
are kept in the memory. The number of captured (saved) frames can be equal or less the the maximum size.
The default buffer size is 100000, which is sufficient for most applications.
"""Distributed build and test executor."""

import logging

# Silent until a process calls dpes.log.setup() (plan/10, D10.7).
logging.getLogger(__name__).addHandler(logging.NullHandler())

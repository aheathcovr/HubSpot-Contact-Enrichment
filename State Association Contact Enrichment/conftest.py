"""
Pytest session configuration for the State Association Contact Enrichment suite.

Importing pandas here at session scope ensures numpy is loaded once before any
test module imports state_association_matcher (which also imports pandas). Without
this, pytest's module isolation can trigger a "cannot load module more than once
per process" numpy error when test_state_assoc.py collects.
"""

import pandas  # noqa: F401  — must be first to claim numpy's import slot

# Testing

This directory contains scripts and files needed to test the module's code.

All tests are located in the `enabled` directory. Tests for one method should be confined to a subdirectory named after that method. This helps keep test files small. Many tests are still in the `methods` directory, but tests for newly added methods are in their own subdirectories.

Module level setup and teardown are implemented in the `enabled` directories `__init__.py`. At present the module is responsible for the database startup and population, and subsequent depopulation.

All tests use mock data stored in a local mongodb (not a separate container - this could be fixed some day). The source for the data is `db_files`.

Note that a live connection to CI or another KBase environment is still required as the KBase SDK test process will attempt to validate the test login token and obtain a username.

The entire test suite is run with the usual `kb-sdk test` (after ensuring that kb_sdk is available and in the path).
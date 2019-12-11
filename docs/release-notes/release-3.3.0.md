# kb_Metrics Change Summary

## Overview

added new methods to support compatibility with job browser bff, which in turn aims to exploit the new or changed capabilities and limitations of ee2, so this effort is to somewhat equalize support from kb_Metrics to ee2. To whit:

- paging via offset and limit
- sort, search, filter
- no decoration of returned job information

Some of the new methods were developed in order to enhance the job browser when used directly with this module (get_jobs, get_job), but with the advent of ee2 and the job browser bff, a second set of methods (query_jobs, query_jobs_admin, is_admin) was added. They may seem duplicative, and they are, but the use cases are somewhat different, and the two sets of methods need to exist at the same time, so I felt it was better to avoid changing the methods for direct ui access while adding support for ee2 compatibility.

## Changes

### enhanced methods

The `get_app_metrics` method was updated to add sorting and paging support before the ee2 effort was joined. This was an effort to improve the job browser in general before ee2 entered the scene. These new features have no effect if not used, and have been in place in CI (and production) for weeks now without ill effect.

### new methods

New methods were introduced to support ee2 compatibility.

These methods were added to support the new job browser before the EE2 effort. They provide improved support for browsing, adding especially paging support. Further development was halted (to move additional functionality from the front end to back end) when the EE2 and Job Browser BFF were begun.

  - get_jobs
  - get_job
  - is_admin

All job browsing needs for EE2 compatibility are met by the two query_jobs methods. They support the usual time range constraint, as well as sorting, filtering, and a limited form of free text search.

  - query_jobs
  - query_jobs_admin
  
### new types:

  - to support the new methods, new types were added to the module spec

### testing restructured

The testing mechanism was restructured to allow tests to be separated into multiple test files. This involved moving the test database population into module-level setup and teardown functions. Tests themselves are separated by directory into major function - e.g. each method has it's own directory and test file. The exception is the `methods` test directory which contains most tests prior to creating this new test organization.

In addition, when splitting tests into multiple files, the class level test support was moved into a superclass `Test.py`.

### new tests and test data

  - test files were added for all new methods
  - new tests are primarily happy-path and incomplete - the sample data is quite complex and difficult to trace, with no tools to internally validate or query them; if this were to be maintained for longer it would be worth revisiting the testing to find a better way of generating the multiplicity of job states for different classes of users.
  - also, populating the test data into an external mongodb container would aid test development, because the developer could query the test databases to confirm or generate test conditions.

### developer support

  - local developer image to allow running the service locally
  - in the end not useful, because too many databases would need to be run locally; if the service were to be relied upon for job queries in the future, it would be worth completing, but since ee2 is taking over this functionality soon, not worth finishing.
  - however, by utilizing the test database (especially if containerized), the developer image could be used for local development.

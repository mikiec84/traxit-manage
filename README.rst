===============================
TraxAir Corpus Management
===============================

Organize your audio files in a specific structure. Use scripts or a CLI to run tests.

Environment variables
---------------------

* QUERY_N_JOBS: Number of jobs used to query keys in get_matches.

Install
-------

* `pip install traxit_manage` will install the distributed computing API but the worker won't be usable.
* `pip install traxit_manage[celery]` will install traxit_celery as well.
* `pip install traxit_manage[algorithm]` will install traxit_algorithm as well.
* `pip install traxit_manage[all]` will install traxit_algorithm and traxit_celery.

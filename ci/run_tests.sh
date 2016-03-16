#!/bin/bash -eux

ci/mypy-run

make virtualenv_coverage

#! /bin/bash

uv export --no-annotate --no-header --no-hashes --no-dev --frozen > requirements.txt
uv export --no-annotate --no-header --no-hashes --dev --frozen > requirements_test.txt

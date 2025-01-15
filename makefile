#!make
include .env
.PHONY: all install format strip-notebook precommit test
SHELL = /bin/bash

# Default behavior
all: install format strip-notebook precommit test

install: 
	pixi install

format:
	pixi run isort bps_to_omop tests
	pixi run black bps_to_omop tests

strip-notebook:
	pixi run git config filter.strip-notebook-output.clean 'jupyter nbconvert --ClearOutputPreprocessor.enabled=True --ClearMetadataPreprocessor.enabled=True --to=notebook --stdin --stdout --log-level=INFO'

precommit:
	pixi run pre-commit uninstall
	pixi run pre-commit install
	pixi run pre-commit run --all-files

test:
	pixi run pytest tests

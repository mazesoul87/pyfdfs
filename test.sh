#!/usr/bin/env bash
cd $(dirname $0)
export NOSE_INCLUDE_EXE=1
export NOSE_WITH_COVERAGE=1
export NOSE_COVER_PACKAGE=pyfdfs
nosetests -s --tc-file test_cfg.ini
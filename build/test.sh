#! /usr/bin/env bash

function main {
  local args_pytest=(
    --cov 'aioextensions'
    --cov-branch
    --cov-report 'term'
    --cov-report "html:${PWD}/coverage/"
    --cov-report "xml:${PWD}/coverage.xml"
    --disable-pytest-warnings
    --exitfirst
    --no-cov-on-fail
    --verbose
  )

      echo '[INFO] Running tests' \
  &&  poetry run pytest "${args_pytest[@]}" \

}

main

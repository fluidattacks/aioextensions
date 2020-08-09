#! /usr/bin/env bash

function main {
  local args_mypy=(
    --ignore-missing-imports
    --strict
  )
  local args_prospector=(
    --doc-warnings
    --full-pep8
    --strictness veryhigh
    --test-warnings
  )

      echo '[INFO] Checking static typing' \
  &&  poetry run mypy "${args_mypy[@]}" src/ \
  &&  echo "[INFO] Linting" \
  &&  poetry run prospector "${args_prospector[@]}" src/ \

}

main

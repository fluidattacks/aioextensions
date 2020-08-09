#! /usr/bin/env bash

function main {
  local args_mypy=(
    --config-file 'settings.cfg'
  )
  local args_prospector=(
    # --doc-warnings
    --full-pep8
    --strictness veryhigh
    --test-warnings
  )

      echo '[INFO] Checking static typing' \
  &&  poetry run mypy "${args_mypy[@]}" src/ \
  &&  poetry run mypy "${args_mypy[@]}" test/ \
  &&  echo "[INFO] Linting" \
  &&  poetry run prospector "${args_prospector[@]}" src/ \
  &&  poetry run prospector "${args_prospector[@]}" test/ \

}

main

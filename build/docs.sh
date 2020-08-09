#! /usr/bin/env bash

function main {
  local args_sphinx_build=(
    docs/
    docs/_build/
  )

      echo '[INFO] Building' \
  &&  poetry run sphinx-build "${args_sphinx_build[@]}" \

}

main

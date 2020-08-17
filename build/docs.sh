#! /usr/bin/env bash

function main {
      echo '[INFO] Building' \
  &&  poetry run pdoc \
        --config 'git_link_template="https://github.com/kamadorueda/aioextensions/blob/latest/{path}#L{start_line}-L{end_line}"' \
        --config 'sort_identifiers=False' \
        --force \
        --html \
        --output-dir docs/ \
        aioextensions \
  &&  mv docs/aioextensions/* docs/ \

}

main

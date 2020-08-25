#! /usr/bin/env bash

function main {
      echo '[INFO] Exporting dependencies' \
  &&  poetry \
        export \
        --format requirements.txt \
        --without-hashes \
        --output requirements.txt \

}

main

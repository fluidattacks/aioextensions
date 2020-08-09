#! /usr/bin/env bash

source ./build/common.sh

function main {
  export PYPI_PASSWORD
  export PYPI_USERNAME
  local version

  function restore_version {
    sed --in-place 's|^version.*$|version = "1.0.0"|g' "pyproject.toml"
  }

      version=$(helper_compute_version) \
  &&  echo "[INFO] Version: ${version}" \
  &&  trap 'restore_version' EXIT \
  &&  sed --in-place \
        "s|^version = .*$|version = \"${version}\"|g" \
        'pyproject.toml' \
  &&  poetry publish \
        --build \
        --password "${PYPI_PASSWORD}" \
        --username "${PYPI_USERNAME}" \

}

main

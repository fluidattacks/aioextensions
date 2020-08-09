#! /usr/bin/env bash

function helper_compute_version {
  poetry run python -c 'if True:
    import time
    now=time.gmtime()
    minutes_month=(
      (now.tm_mday - 1) * 86400
      + now.tm_hour * 3600
      + now.tm_min * 60
      + now.tm_sec
    )
    print(time.strftime(f"%y.%m.{minutes_month}"))
  '
}

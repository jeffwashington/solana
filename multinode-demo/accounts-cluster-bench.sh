#!/usr/bin/env bash
set -e

here=$(dirname "$0")
# shellcheck source=multinode-demo/common.sh
source "$here"/common.sh

usage() {
  if [[ -n $1 ]]; then
    echo "$*"
    echo
  fi
  echo "usage: $0 [extra args]"
  echo
  echo " Run bench-tps "
  echo
  echo "   extra args: additional arguments are passed along to solana-bench-tps"
  echo
  exit 1
}

if [[ -z $1 ]]; then # default behavior
  $solana_accounts_cluster_bench \
    --entrypoint 127.0.0.1:8001 \
    --faucet 127.0.0.1:9900 \
    --identity key.json \
    --space 10000000 \
    --lamports 190560924 \
    --iterations 10000 \
    --batch-size 100 \


else
  $solana_accounts_cluster_bench "$@"
fi

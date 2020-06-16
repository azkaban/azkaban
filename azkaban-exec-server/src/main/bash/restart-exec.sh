#!/usr/bin/env bash

script_dir=$(dirname $0)
${script_dir}/shutdown-exec.sh
${script_dir}/start-exec.sh

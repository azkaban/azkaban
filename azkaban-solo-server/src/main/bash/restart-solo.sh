#!/usr/bin/env bash

script_dir=$(dirname $0)

${script_dir}/shutdown-solo.sh
${script_dir}/start-solo.sh

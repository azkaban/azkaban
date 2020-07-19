#!/usr/bin/env bash

script_dir=$(dirname $0)
${script_dir}/shutdown-web.sh
${script_dir}/start-web.sh

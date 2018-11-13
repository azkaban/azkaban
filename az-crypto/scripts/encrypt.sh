#!/bin/bash

if [[ $# -ne 2 ]]; then
    echo "Usage: encrypt.sh [pass phrase] [plain text]"
    exit 128
fi

#Use version fit for your service.
version=1.1
class_path=../../jars/*:../lib/*

command="-cp $class_path azkaban.crypto.EncryptionCLI -k $1 -p $2 -v $version"
#echo "Executing: java $command"

java $command
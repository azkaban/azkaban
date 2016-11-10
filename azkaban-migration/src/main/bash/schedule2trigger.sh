#!/bin/bash

java -cp "lib/*:extlib/*" azkaban.migration.schedule2trigger.Schedule2Trigger conf/azkaban.properties


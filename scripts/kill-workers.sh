#!usr/bin/env sh
pkill -9 beam.smp && epmd -kill && epmd -daemon

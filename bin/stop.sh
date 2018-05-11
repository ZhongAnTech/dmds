#!/bin/bash

CURR_DIR=`pwd`
cd `dirname "$0"`/..
DMDS_HOME=`pwd`
cd $CURR_DIR
if [ -z "$DMDS_HOME" ] ; then
    echo
    echo "Error: DMDS_HOME environment variable is not defined correctly."
    echo
    exit 1
fi
JAR_NAME=dmds-server
stop() {
	PROG_PID=$( ps aux | grep -v grep | grep ${JAR_NAME} | grep "DMDS_HOME="${DMDS_HOME} | awk '{print $2}' )
	if [ "${PROG_PID}" != "" ] ; then
        echo "stop running ${JAR_NAME} $PROG_PID"
        kill -9 $PROG_PID
	fi
	PROG_PID=$( ps aux | grep -v grep | grep ${JAR_NAME} | grep "DMDS_HOME="${DMDS_HOME} | awk '{print $2}' )
	if [ "${PROG_PID}" == "" ] ; then
        echo "stop running ${JAR_NAME} success"
	else
		    echo "stop running ${JAR_NAME} failed" >&2
	fi
}

stop
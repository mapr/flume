#!/bin/bash


FLUME_HOME=/opt/mapr/flume/flume-1.5.0/
FLUME_LIB=$FLUME_HOME/lib

#
# single flume jar for post-yarn releases
#
if [ -f /opt/mapr/MapRBuildVersion ]; then
    #
    # if hbase has been installed, set it version
    #
    if [ -f "/opt/mapr/hbase/hbaseversion" ]; then
        HBASE_VERSION=`cat /opt/mapr/hbase/hbaseversion`
    else
        HBASE_VERSION="none"
    fi

    #
    # if mapr-core release >=4.0 (yarn beta) returns boolean 1, else returns boolean 0
    #
    MAPR_VERSION=`cat /opt/mapr/MapRBuildVersion | awk -F "." '{print $1"."$2}'`
    POST_YARN=`echo | awk -v cur=$MAPR_VERSION -v min=4.0 '{if (cur >= min) printf("1"); else printf ("0");}'`

    #
    # not yarn
    #
    if [ "$POST_YARN" = "0" ]; then
        echo "POST_YARN=$POST_YARN, HBASE_VERSION=$HBASE_VERSION: removing yarn jars"
        find $FLUME_LIB/ \
            -iname "flume*-hbase.94-h1.jar" \
            -exec bash -c 'mv "{}" $(dirname "{}")/$(basename "{}" "-hbase.94-h1.jar").jar' \;

    #
    # yarn
    #
    else
        case $HBASE_VERSION in
            "0.94.21"|"0.94.17")
                echo "POST_YARN=$POST_YARN, HBASE_VERSION=$HBASE_VERSION: installing flume*-hbase.94-h2 jars"
                find $FLUME_LIB/ \
                    -iname "flume*-hbase.94-h2.jar" \
                    -exec bash -c 'mv "{}" $(dirname "{}")/$(basename "{}" "-hbase.94-h2.jar").jar' \;
                ;;

            "0.98.4")
                echo "POST_YARN=$POST_YARN, HBASE_VERSION=$HBASE_VERSION: installing flume*-hbase.98-h2 jars"
                find $FLUME_LIB/ \
                    -iname "flume*-hbase.98-h2.jar" \
                    -exec bash -c 'mv "{}" $(dirname "{}")/$(basename "{}" "-hbase.98-h2.jar").jar' \;
                ;;

            *)
                echo "POST_YARN=$POST_YARN, HBASE_VERSION=$HBASE_VERSION: installing default flume*-hbase.98-h2 jars"
                find $FLUME_LIB/ \
                    -iname "flume*-hbase.98-h2.jar" \
                    -exec bash -c 'mv "{}" $(dirname "{}")/$(basename "{}" "-hbase.98-h2.jar").jar' \;
                ;;
        esac
    fi
fi

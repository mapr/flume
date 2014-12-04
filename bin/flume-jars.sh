#!/bin/bash


FLUME_HOME=/opt/mapr/flume/flume-1.5.0/
FLUME_LIB=$FLUME_HOME/lib

if [ -f /opt/mapr/MapRBuildVersion ]; then
    MAPR_VERSION=`cat /opt/mapr/MapRBuildVersion | awk -F "." '{print $1"."$2}'`

    #if mapr-core release >=4.0 (yarn beta) returns boolean 1, else returns boolean 0
    POST_YARN=`echo | awk -v cur=$MAPR_VERSION -v min=4.0 '{if (cur >= min) printf("1"); else printf ("0");}'`

    HBASE_VER="not_installed"

    if [ -f /opt/mapr/hbase/hbaseversion ]; then
        HBASE_VER=`cat /opt/mapr/hbase/hbaseversion`
    fi

    if [ "$POST_YARN" = "0" ]; then
        echo POST_YARN=$POST_YARN, HBASE_VER=$HBASE_VER coping flume*-h1.jar.tmp to flume*.jar
        cd $FLUME_LIB
        for filename in `ls flume*-h1.jar.tmp`
        do
            newfilename=`echo $filename | sed 's/-h1.jar.tmp/.jar/'`
            cp $filename $newfilename
        done

    else

        case $HBASE_VER in
            "0.94.21"|"0.94.17")
            echo POST_YARN=$POST_YARN, HBASE_VER=$HBASE_VER coping flume*-hbase-94-h2.jar.tmp to flume*.jar

            cd $FLUME_LIB
            for filename in `ls flume*-hbase-94-h2.jar.tmp`
            do
                newfilename=`echo $filename | sed 's/-hbase-94-h2.jar.tmp/.jar/'`
                cp $filename $newfilename
            done
            ;;
            "0.98.4")
            echo POST_YARN=$POST_YARN, HBASE_VER=$HBASE_VER coping flume*-hbase-98-h2.jar.tmp to flume*.jar
            cd $FLUME_LIB
            for filename in `ls flume*-hbase-98-h2.jar.tmp`
            do
                newfilename=`echo $filename | sed 's/-hbase-98-h2.jar.tmp/.jar/'`
                cp $filename $newfilename
            done
            ;;
            "not_installed")
            echo POST_YARN=$POST_YARN, Hbase is not installed. Usung hbase-98 dependency as default. Coping flume*-hbase-98-h2.jar.tmp to flume*.jar
            cd $FLUME_LIB
            for filename in `ls flume*-hbase-98-h2.jar.tmp`
            do
                newfilename=`echo $filename | sed 's/-hbase-98-h2.jar.tmp/.jar/'`
                cp $filename $newfilename
            done
            ;;
        esac
    fi
fi

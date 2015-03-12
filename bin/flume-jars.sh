#!/bin/bash

#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


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
        HBASE_VERSION=$(cat /opt/mapr/hbase/hbaseversion)
    else
        HBASE_VERSION="none"
    fi

    #
    # if mapr-core release >=4.0 (yarn beta) returns boolean 1, else returns boolean 0
    #
    MAPR_VERSION=$(cat /opt/mapr/MapRBuildVersion | awk -F "." '{print $1"."$2}')
    POST_YARN=$(echo | awk -v cur="$MAPR_VERSION" -v min=4.0 '{if (cur >= min) printf("1"); else printf ("0");}')

    #
    # not yarn
    #
    if [ "$POST_YARN" == "0" ]; then
        echo "POST_YARN=$POST_YARN, HBASE_VERSION=$HBASE_VERSION: removing yarn jars"
        find $FLUME_LIB/ \
            -iname "flume*-hbase.94-h1.jar.tmp" \
            -exec bash -c 'cp "{}" $(dirname "{}")/$(basename "{}" "-hbase.94-h1.jar.tmp").jar' \;

    #
    # yarn
    #
    else
        if [ "$HBASE_VERSION" = "none" ]; then
            #
            # No hbase installed. Use 0.98 jars by default
            #
            echo "POST_YARN=$POST_YARN, HBASE_VERSION=$HBASE_VERSION: installing default flume*-hbase.98-h2 jars"
            find $FLUME_LIB/ \
                -iname "flume*-hbase.98-h2.jar.tmp" \
                -exec bash -c 'cp "{}" $(dirname "{}")/$(basename "{}" "-hbase.98-h2.jar.tmp").jar' \;
        else
            #
            # If hbase version less or equal 0.94X returns boolean 1, else returns boolean 0
            #
            HBASE_VERSION_AS_FLOAT=$(echo $HBASE_VERSION | awk -F "." '{print $1"."$2}')
            IS_HBASE_VER_LESS_OR_EQUAL_094=$(echo | awk -v cur="$HBASE_VERSION_AS_FLOAT" -v min=0.94 '{if (cur <= min) printf("1"); else printf ("0");}')
    
            if [ "$IS_HBASE_VER_LESS_OR_EQUAL_094" = "1" ]; then
                #
                # hbase version is less or equal 0.94X
                #
                echo "POST_YARN=$POST_YARN, HBASE_VERSION=$HBASE_VERSION: installing flume*-hbase.94-h2 jars"
                find $FLUME_LIB/ \
                    -iname "flume*-hbase.94-h2.jar.tmp" \
                    -exec bash -c 'cp "{}" $(dirname "{}")/$(basename "{}" "-hbase.94-h2.jar.tmp").jar' \;
            else
                #
                # hbase version is greater 0.94X
                #
                echo "POST_YARN=$POST_YARN, HBASE_VERSION=$HBASE_VERSION: installing flume*-hbase.98-h2 jars"
                find $FLUME_LIB/ \
                    -iname "flume*-hbase.98-h2.jar.tmp" \
                    -exec bash -c 'cp "{}" $(dirname "{}")/$(basename "{}" "-hbase.98-h2.jar.tmp").jar' \;
            fi
        fi
    fi
fi

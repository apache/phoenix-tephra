#!/bin/sh

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Common environment settings for Tephra.
# Uncomment the lines below, where needed, and modify to adapt to your environment.
#

# A string representing this instance of the Tephra server. $USER by default.
export IDENT_STRING=$USER

# Where log files are stored.  /var/log by default.
export TEPHRA_LOG_DIR=/tmp/tephra-$IDENT_STRING

# The directory where pid files are stored. /var/run by default.
export TEPHRA_PID_DIR=/tmp

# Add any extra classes to the classpath
# export TEPHRA_EXTRA_CLASSPATH

# Set the JVM heap size
# export TEPHRA_JAVA_HEAPMAX=-Xmx2048m

# Additional runtime options
#
# GC logging options.
# Uncomment the following two lines, making any desired changes, to enable GC logging output
# export TEPHRA_GC_LOG_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:server-gc.log -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=2 -XX:GCLogFileSize=50M"
# export TEPHRA_OPTS="$TEPHRA_OPTS $TEPHRA_GC_LOG_OPTS"
#
# JMX options.
# Uncomment the following two lines, making any desired changes, to enable remote JMX connectivity
# export TEPHRA_JMX_OPTS="-Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=13001"
# export TEPHRA_OPTS="$TEPHRA_OPTS $TEPHRA_JMX_OPTS"

# Extra Java runtime options.
# Below are what we set by default.  May only work with SUN JVM.
# For more on why as well as other possible settings,
# see http://wiki.apache.org/hadoop/PerformanceTuning
export TEPHRA_OPTS="$TEPHRA_OPTS -XX:+UseConcMarkSweepGC"

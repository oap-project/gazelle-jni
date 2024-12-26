#! /bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -x

SCRIPT_LOCATION=$(dirname $0)
PAUS=$HOME/PAUS

while [[ $# -gt 0 ]]; do
  case $1 in
    --base-dir)
      BASEDIR="$2"
      shift # past argument
      shift # past value
      ;;
    --name)
      NAME="$2"
      shift # past argument
      shift # past value
      ;;
    --appid)
      APPID="$2"
      shift # past argument
      shift # past value
      ;;
    --pr)
      PR="$2"
      shift # past argument
      shift # past value
      ;;
    --disk)
      DISK="$2"
      shift # past argument
      shift # past value
      ;;
    --nic)
      NIC="$2"
      shift # past argument
      shift # past value
      ;;
    --tz)
      SPARK_TZ="$2"
      shift # past argument
      shift # past value
      ;;
    --proxy)
      PROXY="$2"
      shift # past argument
      shift # past value
      ;;
    --emails)
      EMAILS="$2"
      shift # past argument
      shift # past value
      ;;
    --comp-appid)
      COMP_APPID="$2"
      shift # past argument
      shift # past value
      ;;
    --comp-base-dir)
      COMP_BASEDIR="$2"
      shift # past argument
      shift # past value
      ;;
    --comp-name)
      COMP_NAME="$2"
      shift # past argument
      shift # past value
      ;;
    --baseline-appid)
      BASELINE_APPID="$2"
      shift # past argument
      shift # past value
      ;;
    --baseline-base-dir)
      BASELINE_BASEDIR="$2"
      shift # past argument
      shift # past value
      ;;
    *)
      echo "Error: Unknown argument: $1"
      exit 1
      ;;
  esac
done

# Validation: Check if any of the required variables are empty
if [[ -z "${BASEDIR+x}" || -z "${NAME+x}" || -z "${APPID+x}" || -z "${DISK+x}" || -z "${NIC+x}" || -z "${SPARK_TZ+x}" ]]; then
  echo "Error: One or more required arguments are missing or empty."
  exit 1
fi

mkdir -p $PAUS
if [ ! -f "$PAUS/perf_analysis_template.ipynb" ]; then
  cp $SCRIPT_LOCATION/perf_analysis_template.ipynb $PAUS/
fi
if [ ! -f "$PAUS/sparklog.ipynb" ]; then
  cp $SCRIPT_LOCATION/sparklog.ipynb $PAUS/
fi

workdir=$PAUS/$BASEDIR
mkdir -p $workdir
mkdir -p $workdir/html

nb_name0=${NAME}_${APPID}
nb_name=${nb_name0}.ipynb

# Upload eventlog
cp -f $PAUS/perf_analysis_template.ipynb $workdir/$nb_name
hdfs dfs -mkdir -p /history
hdfs dfs -ls /history/$APPID >/dev/null 2>&1 || { hdfs dfs -cp /$BASEDIR/$APPID/app.log /history/$APPID || exit 1; }

EXTRA_ARGS=""
if [ -v COMP_APPID ]
then
  if [[ -z "${COMP_BASEDIR+x}" || -z "${COMP_NAME+x}" ]]; then
	echo "Missing --comp-base-dir or --comp-name"
	exit 1
  fi
  hdfs dfs -ls /history/$COMP_APPID >/dev/null 2>&1 || { hdfs dfs -cp /$COMP_BASEDIR/$COMP_APPID/app.log /history/$COMP_APPID || exit 1; }
  EXTRA_ARGS=$EXTRA_ARGS" -r comp_appid $COMP_APPID -r comp_base_dir $COMP_BASEDIR -r comp_name $COMP_NAME"
  sed -i "s/# Compare to\"/# Compare to $COMP_NAME\"/g" $workdir/$nb_name
fi
if [ -v BASELINE_APPID ]
then
  if [[ -z "${BASELINE_BASEDIR+x}" ]]; then
	echo "Missing --baseline-base-dir"
	exit 1
  fi
  hdfs dfs -ls /history/$BASELINE_APPID >/dev/null 2>&1 || { hdfs dfs -cp /$BASELINE_BASEDIR/$BASELINE_APPID/app.log /history/$BASELINE_APPID || exit 1; }
  EXTRA_ARGS=$EXTRA_ARGS" -r baseline_appid $BASELINE_APPID -r baseline_base_dir $BASELINE_BASEDIR"
fi


if [ -n "${PR}" ]
then
  EXTRA_ARGS=$EXTRA_ARGS" -r pr $PR"
fi

if [ -n "${PROXY}" ]
then
  EXTRA_ARGS=$EXTRA_ARGS" -r proxy $PROXY"
fi

if [ -n "${EMAILS}" ]
then
  EXTRA_ARGS=$EXTRA_ARGS" -r emails $EMAILS"
fi

source ~/paus-env/bin/activate

notebook_html=html/${nb_name0}.html

papermill --cwd $workdir \
	-r appid $APPID \
	-r disk $DISK \
	-r nic $NIC \
	-r tz $SPARK_TZ \
	-r base_dir $BASEDIR \
	-r name $NAME \
	-r notebook $nb_name \
	-r notebook_html $notebook_html \
	$EXTRA_ARGS $workdir/$nb_name $workdir/$nb_name

jupyter nbconvert --to html --no-input $workdir/$nb_name --output $workdir/$notebook_html --template classic > /dev/null 2>&1

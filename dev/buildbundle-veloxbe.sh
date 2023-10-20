#!/bin/bash

BASEDIR=$(dirname $0)
source "$BASEDIR/builddeps-veloxbe.sh"

cd $GLUTEN_DIR
mvn clean package -Pbackends-velox -Prss -Pspark-3.2 -DskipTests
mvn clean package -Pbackends-velox -Prss -Pspark-3.3 -DskipTests
mvn clean package -Pbackends-velox -Prss -Pspark-3.4 -DskipTests
mvn clean package -Pbackends-velox -Prss -Pspark-3.5 -DskipTests
# No celbron Scala 2.13 support yet.
mvn clean package -Pbackends-velox -Pspark-3.5-2.13 -DskipTests

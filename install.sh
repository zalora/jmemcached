#!/bin/bash
mvn clean package && 
mvn install:install-file -Dfile=target/jmemcached-core-$1.jar -DgroupId=com.zalora.jmemcached -DartifactId=jmemcached-core -Dversion=$1 -Dpackaging=jar

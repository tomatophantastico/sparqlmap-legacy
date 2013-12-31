#!/bin/bash 
SCRIPT=`readlink -f $0`
SCRIPTPATH=`dirname $SCRIPT`

if [ ${PWD} != $SCRIPTPATH  ]; then
        echo "run from the script dir (${SCRIPTPATH})"
        exit 1
fi 

cd $SCRIPTPATH/..

SCRIPTPATHPARENT=$PWD

if [ -n  ${SPARQLMAP_HOME}]; then
 SPARQLMAP_HOME="${SCRIPTPATHPARENT}/map-conf"
fi 

echo "Starting RDF dump...." >&2
java  -classpath "lib/*:lib-jdbc/*" sparqlmap -dump -dbfile ./map-conf/db.properties -r2rmlfile ./map-conf/mapping.ttl

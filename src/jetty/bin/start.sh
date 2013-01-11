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

echo "Using SparqlMap configuration in ${SPARQLMAP_HOME}"

java  -Xms128m -Xmx512m -DSPARQLMAP_HOME=${SPARQLMAP_HOME} -jar lib/start-6.1.26.jar
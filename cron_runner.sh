#!/bin/bash

export PYTHONPATH=$PYTHONPATH:$HOME/OMERO.server/lib/python

cd $(dirname $0)

if [ -z "$1" ] ; then
    echo "ERROR: file with list of usernames required!"
    exit 1
else
    USERLIST="$1"
fi

# extglob is required for the pattern matching in the OMERO_EXPORT line below
shopt -s extglob

REPOBASE="/export/omero/OMERO/ManagedRepository"
TREE="omero_hierarchy/tree"

for OMERO_USERNAME in $(cat "$USERLIST") ; do
    echo "========================== $OMERO_USERNAME =========================="
    OMERO_EXPORT=$(echo $REPOBASE/${OMERO_USERNAME}_+([[:digit:]])/omero_hierarchy)
    if [ -d "$OMERO_EXPORT/tree" ] ; then
	mv "$OMERO_EXPORT/tree" "$OMERO_EXPORT/tree_old"
    fi
    cat localconfig_template.py | sed "s,OMERO_USERNAME,${OMERO_USERNAME}," > localconfig.py
    TSTART=$(date +%s)
    LOGFILE="$OMERO_EXPORT/.cron_run_$TSTART.log"
    echo "Starting export of OMERO hierarchy, logging to '$LOGFILE'."
    ./export_treestructure.py > "$LOGFILE" 2>&1
    TDELTA=$(echo "$(date +%s) - $TSTART" | bc -l)
    echo "Export of OMERO hierarchy took $TDELTA seconds."
    rm -rf "$OMERO_EXPORT/tree_old"
done

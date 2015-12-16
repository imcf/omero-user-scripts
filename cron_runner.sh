#!/bin/bash

LOGDIR="$HOME/OMERO.server/var/log/cron"
REPOBASE="/export/omero/OMERO/ManagedRepository"

export PYTHONPATH=$PYTHONPATH:$HOME/OMERO.server/lib/python

cd $(dirname $0)

if [ -z "$1" ] ; then
    echo "ERROR: comma-separated list of usernames required!"
    exit 1
else
    USERS="$1"
fi

if ! [ -d "$LOGDIR" ] ; then
    set -e
    mkdir -v "$LOGDIR"
    set +e
fi

# extglob is required for the pattern matching in the OMERO_EXPORT line below
shopt -s extglob

for OMERO_USERNAME in ${USERS//,/ } ; do
    TREE=$(echo $REPOBASE/${OMERO_USERNAME}_+([[:digit:]])/omero_hierarchy/tree)
    if [ -d "$TREE" ] ; then
	echo "Temporarily moving old export tree:"
        mv -v "$TREE" "${TREE}_old"
    fi
done

TSTART=$(date +%s)
LOGFILE="$LOGDIR/export_treestructure-$TSTART.log"
echo "Starting export of OMERO hierarchy, logging to '$LOGFILE'."
# ./export_treestructure.py --user $USERS -vv 2>&1 | tee log
./export_treestructure.py --user $USERS -vv > "$LOGFILE" 2>&1
TDELTA=$(echo "$(date +%s) - $TSTART" | bc -l)
echo "Export of OMERO hierarchy took $TDELTA seconds."

for OMERO_USERNAME in ${USERS//,/ } ; do
    TREE=$(echo $REPOBASE/${OMERO_USERNAME}_+([[:digit:]])/omero_hierarchy/tree)
    if [ -d "$TREE" ] ; then
	echo "Removing temporary backupg of old export tree: ${TREE}_old"
        rm -rf "${TREE}_old"
    fi
done

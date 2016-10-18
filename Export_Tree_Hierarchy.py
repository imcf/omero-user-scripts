#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Title: Export Tree Hierarchy
Description: Trigger the export of the tree hierarchy.
__author__  Niko Ehrenfeuchter
"""

import omero
import omero.scripts as scripts
from omero.gateway import BlitzGateway
from omero.rtypes import rstring

import os


def run_as_script():
    """
    Main entry point of the script, as called via the scripting service.
    """

    client = scripts.client(
    	'Export_Tree_Hierarchy.py',
    	'Trigger an update of the symlink tree hierarchy on the sciCORE '
        'cluster filesystem.',
    	authors = ["Niko Ehrenfeuchter"],
    	institutions = ["IMCF, University of Basel"],
    	contact = "nikolaus.ehrenfeuchter@unibas.ch",
    )

    try:
        # wrap client to use the Blitz Gateway
        conn = BlitzGateway(client_obj=client)

        username = conn.getUser().getName()
        markdir = os.path.join(os.environ['HOME'], '.omero_tree_export_usernames')

        if not os.path.exists(markdir):
            # do not create the marker directory, send back an error message
            # instead - the directory has to exist, otherwise the wrapper
            # daemon is not running!
            message = "ERROR: Marker directory '%s' missing!" % markdir
            client.setOutput("Message", rstring(message))
            raise IOError("directory '%s' missing!" % markdir)

        filename = os.path.join(markdir, username)

        if os.path.exists(filename):
            message = ("WARNING: a request for username '%s' is already "
                       "existing! Please contact an administrator if this "
                       "request does not get processed soon!" % username)
        else:
            message = "Requested update for username '%s'." % username
            with open(filename, 'a') as out:
                out.write('%s' % username)

        client.setOutput("Message", rstring(message))

    finally:
        # Cleanup
        client.closeSession()

if __name__ == "__main__":
    run_as_script()

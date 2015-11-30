#!/usr/bin/env python

import sys
import os

try:
    from omero.gateway import BlitzGateway, FileAnnotationWrapper
except ImportError:
    print "Adjust your PYTHONPATH to include the omero package, e.g.:"
    print
    print "export PYTHONPATH=/opt/OMERO/OMERO.server/lib/python:$PYTHONPATH"
    print
    sys.exit()

from omero.gateway import BlitzGateway, FileAnnotationWrapper

HOST = 'localhost'
HOST = 'omero.biozentrum.unibas.ch'

PORT = 4064
USER = 'demo01'
# PASS = 'Dem0o1'
# PASS = '7777'
SU_USER = 'root'
SU_PASS = 'omero'

try:
    from localconfig import USER, PASS, SU_USER, SU_PASS, MANAGED_REPO
except ImportError:
    print "Using hard-coded configuration values!"


su_conn = BlitzGateway(SU_USER, SU_PASS, host=HOST, port=PORT)
if su_conn.connect() is False:
    raise RuntimeError('Connection to OMERO failed, check settings!')

conn = su_conn.suConn(USER)
if conn.connect() is False:
    raise RuntimeError('User switching in OMERO failed, check settings!')



su_conn = BlitzGateway(SU_USER, SU_PASS, host=HOST, port=PORT)
su_conn.connect()
conn = su_conn.suConn(USER)
conn.connect()

# projs = [ x for x in conn.listProjects() ]
projs = [ x for x in conn.listProjects(eid=conn.getUserId()) ]
proj = projs[0]
print proj.getName()
datasets = [x for x in proj.listChildren()]
ds = datasets[0]
print ds.getName()

images = [ x for x in ds.listChildren() ]
img = images[0]
print img.getName()

fileset = img.getFileset()
print fileset
fsId = fileset.getId()
print fsId
for fsImage in fileset.copyImages():
    print fsImage.getId(), fsImage.getName()

for origFile in fileset.listFiles():
    print origFile.getName()
    print origFile.getPath()



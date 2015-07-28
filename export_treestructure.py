#!/usr/bin/env python

import sys
import argparse
import os
import hrm_config

sys.path.insert(0, '%s/lib/python' % hrm_config.CONFIG['OMERO_PKG'])
from omero.gateway import BlitzGateway, FileAnnotationWrapper

HOST = hrm_config.CONFIG['OMERO_HOSTNAME']
PORT = 4064
USER = 'demo01'
PASS = 'Dem0o1'

BASE = '/tmp/foo234/tree'

conn = BlitzGateway(USER, PASS, host=HOST, port=PORT)
conn.connect()


def mkdir_verbose(directory):
    print directory
    if os.path.exists(directory):
        return
    os.makedirs(directory)


def link_origfiles(img, directory):
    for origfile in img.getImportedImageFiles():
        name = os.path.join(directory, origfile.getName())
        link_tgt = os.path.join(origfile.getPath(), origfile.getName())
        print "LINK: %s -> %s" % (name, link_tgt)
        # TODO: replace lexists() by exists() once we're on real paths:
        if not os.path.lexists(name):
            os.symlink(link_tgt, name)

def process_annotations(obj, directory):
    for ann in obj.listAnnotations():
        if not isinstance(ann, FileAnnotationWrapper):
            next
        ann_name = ann.getFile().getName()
        ann_id = ann.getFile().getId()
        if obj.OMERO_CLASS == 'Dataset':
            tgt = os.path.join(directory, '_attachments')
        elif obj.OMERO_CLASS == 'Image':
            tgt = os.path.join(directory, obj.getName() + '_attachments')
        else:
            print "Unknown object type: %s" % obj.OMERO_CLASS
            next
        mkdir_verbose(tgt)
        print tgt + '/' + ann_name, str(ann_id)
        os.symlink(str(ann_id), tgt + '/' + ann_name)
        print "ANNOTATION: (%s) %s" % (ann_id, ann_name)

    

mkdir_verbose(BASE)

for proj in conn.listProjects():
    mkdir_verbose(os.path.join(BASE, proj.name))
    for ds in proj.listChildren():
        ds_dir = os.path.join(BASE, proj.name, ds.name)
        mkdir_verbose(ds_dir)
        process_annotations(ds, ds_dir)
        for img in ds.listChildren():
            link_origfiles(img, ds_dir)
            process_annotations(img, ds_dir)

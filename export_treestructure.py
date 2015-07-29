#!/usr/bin/env python

import sys
import argparse
import os

try:
    from omero.gateway import BlitzGateway, FileAnnotationWrapper
except ImportError:
    print "Adjust your PYTHONPATH to include the omero package, e.g.:"
    print
    print "export PYTHONPATH=/opt/OMERO/OMERO.server/lib/python:$PYTHONPATH"
    print
    sys.exit()

HOST = 'vbox.omero-512'
PORT = 4064
USER = 'demo01'
PASS = 'Dem0o1'

BASE = '/tmp/foo234/tree'

conn = BlitzGateway(USER, PASS, host=HOST, port=PORT)
conn.connect()


def mkdir_verbose(directory):
    """Verbose mkdir, creating the directory only if it doesn't exist."""
    print directory
    if os.path.exists(directory):
        return
    os.makedirs(directory)


def link_origfiles(img, directory):
    """Create a symlink to the original file of an OMERO image."""
    for origfile in img.getImportedImageFiles():
        name = os.path.join(directory, origfile.getName())
        link_tgt = os.path.join(origfile.getPath(), origfile.getName())
        print "LINK: %s -> %s" % (name, link_tgt)
        # TODO: replace lexists() by exists() once we're on real paths:
        if not os.path.lexists(name):
            os.symlink(link_tgt, name)

def process_annotations(obj, directory):
    """Process all annotations of an object, downloading attachments."""
    for ann in obj.listAnnotations():
        if not isinstance(ann, FileAnnotationWrapper):
            next
        if obj.OMERO_CLASS == 'Dataset':
            tgt = os.path.join(directory, '_attachments')
        elif obj.OMERO_CLASS == 'Image':
            tgt = os.path.join(directory, obj.getName() + '_attachments')
        else:
            print "Unknown object type: %s" % obj.OMERO_CLASS
            next
        download_attachment(ann, tgt)


def download_attachment(ann, tgt):
    """Download a file annotation."""
    mkdir_verbose(tgt)
    ann_name = ann.getFile().getName()
    ann_id = ann.getFile().getId()
    file_path = os.path.join(tgt + '/' + ann_name)
    if os.path.exists(file_path):
        print "Skipping existing attachment:", file_path
        return
    print file_path, " [%s]" % str(ann_id)
    fout = open(str(file_path), 'w')
    try:
        for chunk in ann.getFileInChunks():
            fout.write(chunk)
    finally:
        fout.close()

    

mkdir_verbose(BASE)

for proj in conn.listProjects():
    mkdir_verbose(os.path.join(BASE, proj.name))
    for ds in proj.listChildren():
        ds_dir = os.path.join(BASE, proj.name, ds.name)
        mkdir_verbose(ds_dir)
        process_annotations(ds, ds_dir)
        for image in ds.listChildren():
            link_origfiles(image, ds_dir)
            process_annotations(image, ds_dir)

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

HOST = 'localhost'
PORT = 4064
USER = 'demo01'
PASS = 'Dem0o1'

MANAGED_REPO = '/home/omero/OMERO.data/ManagedRepository'

conn = BlitzGateway(USER, PASS, host=HOST, port=PORT)
conn.connect()

UID = conn.getUserId()

BASE = os.path.join(MANAGED_REPO, USER + '_' + str(UID), 'omero_hierarchy')
TREE = os.path.join(BASE, 'tree')
ATTACH = os.path.join(BASE, 'attachments')


def mkdir_verbose(directory):
    """Verbose mkdir, creating the directory only if it doesn't exist."""
    print directory
    if os.path.exists(directory):
        return
    os.makedirs(directory)


def link_origfiles(img, directory):
    """Create a symlink to the original file of an OMERO image."""
    for origfile in img.getImportedImageFiles():
        symlink = os.path.join(directory, origfile.getName())
        target = os.path.join(MANAGED_REPO, origfile.getPath(), origfile.getName())
        target = target.replace(MANAGED_REPO, '').split('/')[2:]
        relpath = directory.replace(BASE, '').split('/')
        for i in range(len(relpath)):
            relpath[i] = '..'
        target = relpath + target
        target = os.path.join(*target)
        print "LINK: %s -> %s" % (symlink, target)
        # TODO: replace lexists() by exists() once we're on real paths:
        if not os.path.lexists(symlink):
            os.symlink(target, symlink)


def link_attachment(ann, directory):
    """Create a symlink to an attachment.

    Parameters
    ----------
    ann : FileAnnotationWrapper
    directory : str
        The directory where the symlink should be placed
    """
    ### create the symlink TARGET string:
    # (1) remove BASE, split dirs, remove suffix:
    target = directory.replace(BASE, '').split('/')[:-1]
    # (2) replace all entries with '..':
    for i in range(len(target)):
        target[i] = '..'
    # (3) append the attachments directory and ID:
    target.extend(['attachments', str(ann.getFile().getId())])
    # (4) turn it into a relative path string:
    target = os.path.join(*target)
    symlink = os.path.join(directory, ann.getFile().getName())
    mkdir_verbose(directory)
    print "LINK: %s -> %s" % (symlink, target)
    if not os.path.lexists(symlink):
        os.symlink(target, symlink)


def process_annotations(obj, directory):
    """Process all annotations of an object, downloading attachments."""
    for ann in obj.listAnnotations():
        if not isinstance(ann, FileAnnotationWrapper):
            continue
        if obj.OMERO_CLASS == 'Dataset' or obj.OMERO_CLASS == 'Project':
            tgt = os.path.join(directory, '_attachments')
        elif obj.OMERO_CLASS == 'Image':
            tgt = os.path.join(directory, obj.getName() + '_attachments')
        else:
            print "Unknown object type: %s" % obj.OMERO_CLASS
            continue
        download_attachment(ann)
        link_attachment(ann, tgt)



def download_attachment(ann):
    """Download a file annotation to the attachment directory.

    Downloading is done "lazy", meaning the file won't be re-downloaded if it
    is already existing in the target directory.

    Returns
    -------
    ann_id : long
        The ID of the annotation (attachment).
    """
    ann_id = ann.getFile().getId()
    file_path = os.path.join(ATTACH, str(ann_id))
    if os.path.exists(file_path):
        print "Skipping existing attachment:", ann_id
        return None
    print file_path
    fout = open(str(file_path), 'w')
    try:
        for chunk in ann.getFileInChunks():
            fout.write(chunk)
    finally:
        fout.close()
    return ann_id

    

mkdir_verbose(TREE)
mkdir_verbose(ATTACH)

for proj in conn.listProjects():
    mkdir_verbose(os.path.join(TREE, proj.name))
    for ds in proj.listChildren():
        ds_dir = os.path.join(TREE, proj.name, ds.name)
        mkdir_verbose(ds_dir)
        process_annotations(ds, ds_dir)
        for image in ds.listChildren():
            link_origfiles(image, ds_dir)
            process_annotations(image, ds_dir)

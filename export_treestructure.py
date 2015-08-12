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
SU_USER = 'root'
SU_PASS = 'omero'

MANAGED_REPO = '/home/omero/OMERO.data/ManagedRepository'

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

# TODO: switch to target user when connecting with an admin:
# base_conn = BlitzGateway()
# base_conn.connect()
# conn = base_conn.suConn(username)

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
    """Create a symlink to the original file of an OMERO image.

    Parameters
    ----------
    img : omero.gateway._ImageWrapper
    directory : str
        The directory (full path) where the symlink should be placed.
    """
    origfiles = img.getImportedImageFilePaths()['server_paths']
    fname = img.getName().replace('/', '_--_')
    symlink = os.path.join(directory, fname)
    relpath = ['..' for x in directory.replace(BASE, '').split('/')]
    relpath = os.path.join(*relpath)
    fcount = len(origfiles)
    linkpairs = []
    if fcount > 1:
        fmt = '%0' + str(len(str(fcount))) + 'i'
        for i, origfile in enumerate(origfiles):
            target = origfile[origfile.index('/') + 1:]
            target = os.path.join(relpath, target)
            linkpairs.append((target, symlink + '_' + (fmt % i)))
    else:
        origfile = origfiles[0]
        target = origfile[origfile.index('/') + 1:]
        target = os.path.join(relpath, target)
        linkpairs.append((target, symlink))
    for pair in linkpairs:
        print "LINK: %s -> %s" % (pair[1], pair[0])
        # TODO: replace lexists() by exists() once we're on real paths:
        if not os.path.lexists(symlink):
            # os.symlink(target, symlink)
            os.symlink(*pair)


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
    fname = ann.getFile().getName().replace('/', '_--_')
    symlink = os.path.join(directory, fname)
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
            name = obj.getName().replace('/', '_--_')
            tgt = os.path.join(directory, name + '_attachments')
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
    proj_dir = os.path.join(TREE, proj.name)
    mkdir_verbose(os.path.join(TREE, proj.name))
    process_annotations(proj, proj_dir)
    for ds in proj.listChildren():
        ds_dir = os.path.join(TREE, proj.name, ds.name)
        mkdir_verbose(ds_dir)
        process_annotations(ds, ds_dir)
        for image in ds.listChildren():
            link_origfiles(image, ds_dir)
            process_annotations(image, ds_dir)

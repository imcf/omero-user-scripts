#!/usr/bin/env python

"""Generate a filesystem view resembling the hierarchy in OMERO."""

import sys
import os
import re

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
    from localconfig import USER, SU_USER, SU_PASS, MANAGED_REPO, HOST
except ImportError:
    print "Using hard-coded configuration values!"




def mkdir_verbose(directory):
    """Verbose mkdir, creating the directory only if it doesn't exist."""
    if os.path.exists(directory):
        return
    print directory
    os.makedirs(directory)


def link_origfiles(img, directory, paths):
    """Create a symlink to the original file of an OMERO image.

    Parameters
    ----------
    img : omero.gateway._ImageWrapper
    directory : str
        The directory (full path) where the symlink should be placed.
    paths : dict
        The dict containing the base paths (ATTACH, BASE, TREE).

    Returns
    -------
    True if the symlink creation was successful, False otherwise.
    """
    relpath = ['..' for _ in directory.replace(paths['BASE'], '').split('/')]
    relpath = os.path.join(*relpath)
    def tgt_name(origfile):
        """Build the target name from the original file's name."""
        target = origfile[origfile.index('/') + 1:]
        target = os.path.join(relpath, target)
        return target
    ### def filter_fileset(fname, origfiles): # TODO
    origfiles = img.getImportedImageFilePaths()['server_paths']
    fname = img.getName().replace('/', '_--_')
    symlink = os.path.join(directory, fname)
    fcount = len(origfiles)
    pairs = []
    if fcount > 1:
        # workaround for the fileset problem: if the image name contains a
        # square bracket, we assume this is the original image name and match
        # it against the file names, using only those that DO contain the
        # image's name (therefore exluding all "original" files that actually
        # belong to another image of this fileset)
        if "[" in fname:
            tmplist = []
            match = re.search(r"\[(.*)\]", fname)
            if match is None:
                print "WARNING: filename matching failed: %s" % fname
                return False
            imgname = match.group(1)
            # create a temporary (new) origfiles list
            for origfile in origfiles:
                if re.search(imgname, origfile):
                    tmplist.append(origfile)
            # now we replace the "origfiles" list:
            origfiles = tmplist
            fcount = len(origfiles)
        else:
            print "WARNING: unexpected fileset name formatting: %s" % fname
            return False
        fmt = '%0' + str(len(str(fcount))) + 'i'
        for i, origfile in enumerate(origfiles):
            pairs.append((tgt_name(origfile), symlink + '_' + (fmt % i)))
    else:
        pairs = [(tgt_name(origfiles[0]), symlink)]
    for pair in pairs:
        print "LINK: %s -> %s" % (pair[1], pair[0])
        # TODO: replace lexists() by exists() once we're on real paths:
        if not os.path.lexists(symlink):
            # os.symlink(target, symlink)
            os.symlink(*pair)
    return True


def link_attachment(ann, directory, paths):
    """Create a symlink to an attachment.

    Parameters
    ----------
    ann : FileAnnotationWrapper
    directory : str
        The directory where the symlink should be placed.
    paths : dict
        The dict containing the base paths (ATTACH, BASE, TREE).
    """
    ### create the symlink TARGET string:
    # (1) remove BASE, split dirs, remove suffix:
    target = directory.replace(paths['BASE'], '').split('/')[:-1]
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


def process_annotations(obj, directory, paths):
    """Process all annotations of an object, downloading attachments.

    Parameters
    ----------
    obj : FIXME
    directory : str
        The directory where the symlinks to the attachments should be placed.
    paths : dict
        The dict containing the base paths (ATTACH, BASE, TREE).
    """
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
        download_attachment(ann, paths['ATTACH'])
        link_attachment(ann, tgt, paths)


def download_attachment(ann, ann_dir):
    """Download a file annotation to the attachment directory.

    Downloading is done "lazy", meaning the file won't be re-downloaded if it
    is already existing in the target directory.

    Parameters
    ----------
    ann : OMERO annotation object (FIXME!)
    ann_dir : str
        The directory where the attachments will be placed in.

    Returns
    -------
    ann_id : long
        The ID of the annotation (attachment).
    """
    ann_id = ann.getFile().getId()
    file_path = os.path.join(ann_dir, str(ann_id))
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


def connect_as_user(username):
    """Establish a connection to OMERO with a given user context.

    To establish a connection as a specific user without knowing their
    credential, a two-stage process is required: first the bsae connection is
    created with an admin user, then this existing connection is switched over
    to a (non-privileged) user account.

    Returns the connection in the user's context.
    """
    # establish the base connection with an admin account
    su_conn = BlitzGateway(SU_USER, SU_PASS, host=HOST, port=PORT)
    if su_conn.connect() is False:
        raise RuntimeError('Connection to OMERO failed, check settings!')
    # now switch to the requested user
    conn = su_conn.suConn(username)
    if conn.connect() is False:
        raise RuntimeError('User switching in OMERO failed, check settings!')
    return conn


def gen_treestructure(username):
    """Generate a tree structure with attachments and links to images."""
    conn = connect_as_user(username)
    uid = conn.getUserId()
    paths = dict()
    paths['BASE'] = os.path.join(MANAGED_REPO,
                                 username + '_' + str(uid),
                                 'omero_hierarchy')
    paths['TREE'] = os.path.join(paths['BASE'], 'tree')
    paths['ATTACH'] = os.path.join(paths['BASE'], 'attachments')

    mkdir_verbose(paths['TREE'])
    mkdir_verbose(paths['ATTACH'])
    # recursively build the tree:
    for proj in conn.listProjects(eid=uid):
        proj_dir = os.path.join(paths['TREE'], proj.name)
        mkdir_verbose(os.path.join(paths['TREE'], proj.name))
        process_annotations(proj, proj_dir, paths)
        for dset in proj.listChildren():
            dset_dir = os.path.join(paths['TREE'], proj.name, dset.name)
            mkdir_verbose(dset_dir)
            process_annotations(dset, dset_dir, paths)
            for image in dset.listChildren():
                link_origfiles(image, dset_dir, paths)
                process_annotations(image, dset_dir, paths)


def test():
    """Dummy function for testing purposes.

    Example
    =======
    >>> import export_treestructure as et
    >>> def run():
    ...     reload(et)
    ...     et.test()
    >>> run()
    """
    conn = connect_as_user(USER)
    uid = conn.getUserId()
    paths = dict()
    paths['BASE'] = os.path.join(MANAGED_REPO,
                                 USER + '_' + str(uid),
                                 'omero_hierarchy')
    paths['TREE'] = os.path.join(paths['BASE'], 'tree')
    paths['ATTACH'] = os.path.join(paths['BASE'], 'attachments')

    mkdir_verbose(paths['TREE'])
    mkdir_verbose(paths['ATTACH'])
    proj = [x for x in conn.listProjects(eid=conn.getUserId())][1]
    proj_dir = os.path.join(paths['TREE'], proj.name)
    process_annotations(proj, proj_dir, paths)

    ### # recursively build the tree:
    ### for proj in conn.listProjects(eid=uid):
    ###     proj_dir = os.path.join(paths['TREE'], proj.name)
    ###     mkdir_verbose(os.path.join(paths['TREE'], proj.name))
    ###     process_annotations(proj, proj_dir, paths)


def main():
    """Run tree structure exporter."""
    gen_treestructure(USER)


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python

"""Generate a filesystem view resembling the hierarchy in OMERO.

The purpose of this script is to generate a tree-like structure inside a user's
sub-directory in OMERO's managed repository that resembles the view seen by a
user when connecting via OMERO.insight or using the webclient to give a more
intuitive access to the original files from the filesystem perspective.

With this tree-export, users can traverse the same (or at least similar)
structure on the file system than in OMERO without actually having to query
OMERO itself for projects, datasets, images and attachments.

The structure is created as follows:

/omero-managed-repository/username/
    |__ omero_hierarchy
         |__ attachments
         |__ tree

The "attachments" directory will be used to download all attachments, storing
them in separate files using their OMERO id as the filename. This is required,
as OMERO stores attachments in a central place, which is NOT user specific (and
therefore can't be made accessible to the users without compromising the other
user's attachments.

The "tree" part will be pouplated with subdirectories, one per project, using
the project's name (WARNING: duplicate project names are possible in OMERO, but
will not be treated specially, meaning stuff from two independent projects that
happen to have the same name will end up in the same project directory!).

Each project directory contains sub-directories with the corresponding
datasets, with the same restrictions as above. The dataset directories will be
populated with symlinks to the original files. As the tree-structure is a
sub-directory inside a user's part of the managed repository, relative symlinks
are used for the image files, allowing fast and easy access to the original
files.

Attachments are allowed on any level of the hierarchy inside OMERO. For the
folder-like objects (Projects and Datasets), a separate sub-directory called
"_attachments" is created *inside* the corresponding folder, containing
symlinks to the downloaded attachment file (see above). For images, an extra
directory with the image's name followed by "_attachments" is used.
"""

import sys
import os
import re
import argparse

import logging

log = logging.getLogger('IMCF')

try:
    from omero.gateway import BlitzGateway, FileAnnotationWrapper
except ImportError:
    print "Adjust your PYTHONPATH to include the omero package, e.g.:"
    print
    print "export PYTHONPATH=/opt/OMERO/OMERO.server/lib/python:$PYTHONPATH"
    print
    sys.exit()

try:
    from localconfig import SU_USER, SU_PASS, MANAGED_REPO, HOST, PORT
except ImportError:
    log.error("""Could not find "localconfig.py"!

    Use this template to create one:
    -------------------------------------------------------
    HOST = 'localhost'
    PORT = 4064
    SU_USER = 'root'
    SU_PASS = 'omero'

    MANAGED_REPO = '/home/omero/OMERO.data/ManagedRepository'
    -------------------------------------------------------
    """)
    raise ImportError


def mkdir_verbose(directory):
    """Verbose mkdir, creating the directory only if it doesn't exist."""
    if os.path.exists(directory):
        return
    log.info("Creating directory: %s", directory)
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
    relpath = os.path.join(*relpath)  # pylint: disable=star-args

    def tgt_name(origfile):
        """Build the target name from the original file's name."""
        target = origfile[origfile.index('/') + 1:]
        target = os.path.join(relpath, target)
        return target

    def process_bracketed_names(fname, origfiles):
        """Workaround for the fileset naming problem.

        If the image name contains a square bracket, we assume this is the
        original image name and match it against the file names, using only
        those that DO contain the image's name (therefore exluding all
        "original" files that actually belong to another image of this fileset)
        """
        if "[" not in fname:
            log.warn("Unexpected fileset name formatting: %s", fname)
            return None
        match = re.search(r"\[(.*)\]", fname)
        if match is None:
            log.warn("Filename matching failed: %s", fname)
            return None
        # append a dot at the end to prevent "Pos1" matching "Pos10" etc.
        imgname = match.group(1) + r"\."
        log.debug("Matching pattern: '%s'", imgname)
        # create a temporary (new) origfiles list
        tmplist = []
        for origfile in origfiles:
            if re.search(imgname, origfile):
                log.debug("Matched filename: '%s'", origfile)
                tmplist.append(origfile)
        return tmplist

    origfiles = img.getImportedImageFilePaths()['server_paths']
    fname = img.getName().replace('/', '_--_')
    symlink = os.path.join(directory, fname)
    # pairs is a list of tuples of the form (target_file, symlink_file)
    pairs = []
    if len(origfiles) > 1:
        # this is a fileset, so we have to treat the names specially:
        log.debug("Found fileset: %s", origfiles)
        origfiles = process_bracketed_names(fname, origfiles)
        if origfiles is None:
            return False
        log.debug("Processed fileset: %s", origfiles)

        # we need the length of the number of origfiles for the formatting:
        fmt = '%0' + str(len(str(len(origfiles)))) + 'i'
        for i, origfile in enumerate(origfiles):
            pairs.append((tgt_name(origfile), symlink + '_' + (fmt % i)))
    else:
        # this is NOT a fileset, so simply proceed:
        pairs = [(tgt_name(origfiles[0]), symlink)]
    # now we are ready to actually create the symlinks:
    for pair in pairs:
        log.info("link_origfiles: %s -> %s", pair[1], pair[0])
        # NOTE: lexists() returns True for broken symbolic links, whereas
        # exists() would return false!
        if not os.path.lexists(pair[1]):
            os.symlink(pair[0], pair[1])
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
    target = os.path.join(*target)  # pylint: disable=star-args
    fname = ann.getFile().getName().replace('/', '_--_')
    symlink = os.path.join(directory, fname)
    mkdir_verbose(directory)
    log.info("link_attachment: %s -> %s", symlink, target)
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
            log.error("Unknown object type: %s", obj.OMERO_CLASS)
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
        log.info("Skipping existing attachment: %s", ann_id)
        return None
    log.info("Downloading attachment: %s", file_path)
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
    log.debug("Successfully connected to OMERO.")
    return conn


def gen_treestructure(username):
    """Generate a tree structure with attachments and links to images."""
    conn = connect_as_user(username)
    uid = conn.getUserId()
    log.info("Connection User ID: %s", uid)
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


def test(username):
    """Dummy function for testing purposes.

    Example
    =======
    >>> import export_treestructure as et
    >>> def run():
    ...     reload(et)
    ...     et.test()
    >>> run()
    """
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
    proj = [x for x in conn.listProjects(eid=conn.getUserId())][1]
    proj_dir = os.path.join(paths['TREE'], proj.name)
    process_annotations(proj, proj_dir, paths)


def parse_arguments():
    """Parse commandline arguments."""
    argparser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    add = argparser.add_argument
    add('--host', type=str,
        help='The OMERO server IP or DNS name (default=localhost).')
    add('--port', type=int, default=4064,
        help='The OMERO server port (default=4064).')
    add('--user', type=str, required=True,
        help='The OMERO user name, multiple users separated by commas.')
    add('-v', '--verbosity', dest='verbosity',
        action='count', default=0)
    try:
        args = argparser.parse_args()
    except IOError as err:
        argparser.error(str(err))
    return args


def main():
    """Run tree structure exporter."""
    args = parse_arguments()
    log.setLevel((3 - args.verbosity) * 10)
    for user in args.user.split(","):
        gen_treestructure(user)


if __name__ == "__main__":
    sys.exit(main())

from pathlib import Path
from shutil import copytree, make_archive, rmtree

SRC_DIR = 'app/'
DEST_ROOT_DIR = 'target/emr-autoscaling/'
DEST_DIR = 'target/emr-autoscaling/app'

if Path(DEST_DIR).exists() and Path(DEST_DIR).is_dir():
    rmtree(DEST_DIR)

copytree(SRC_DIR, DEST_DIR)

make_archive(DEST_ROOT_DIR, 'zip', DEST_ROOT_DIR)

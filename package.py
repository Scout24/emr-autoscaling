from pathlib import Path
from shutil import copytree, make_archive, rmtree

SRC_DIR = 'src/python'
DEST_DIR = 'target/emr_autoscaling'

if Path(DEST_DIR).exists() and Path(DEST_DIR).is_dir():
    rmtree(DEST_DIR)

copytree(SRC_DIR, DEST_DIR)

make_archive(DEST_DIR, 'zip', DEST_DIR)

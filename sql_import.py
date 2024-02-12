import os

from modules.models.charter_db import CharterDb
from modules.models.images_file import ImagesFile
from modules.models.mom_backup import MomBackup
from modules.utils import pick_random_items

# backup_zip = "./data/full20240202-1515.zip"
backup_zip = "./data/full20210819-0400.zip"
files_path = "./data/filelist_20240209.txt"

# Postgres settings
pg_password = os.environ.get("PG_PW")
pg_host = os.environ.get("PG_HOST")


with CharterDb(pg_host, pg_password) as db:
    with MomBackup(backup_zip) as backup:
        db.setup_db()
        # # insert images
        # images = ImagesFile(files_path).list_images()
        # print("Images listed: ", len(images))
        # db.insert_images(images)
        # print("Images inserted")
        # insert archives
        print("Listing archives...")
        archives = backup.list_archives()
        # insert archivals fonds
        print("Inserting archives...")
        db.insert_archives(archives)
        print("Listing fonds...")
        fonds = backup.list_fonds(archives)
        # TODO: for testing, remove
        # fonds = [
        #     fond
        #     for fond in fonds
        #     # if fond.archive_file == "HR-HDA" and fond.file == "643"
        #     if fond.archive_file == "DE-StaAWo" and fond.file == "Abt1AI"
        # ]
        fonds = pick_random_items(fonds, 20)  # TODO: for testing, remove
        print("Inserting fonds...")
        db.insert_fonds(fonds)
        print("Listing fond charters...")
        fond_charters = backup.list_fond_charters(fonds)
        print("Inserting fond charters...")
        db.insert_fonds_charters(fond_charters)

import os

from modules.models.charter_db import CharterDb
from modules.models.images_file import ImagesFile
from modules.models.mom_backup import MomBackup

# backup_zip = "./data/full20240202-1515.zip"
backup_zip = "./data/full20210819-0400.zip"
files_path = "./data/filelist_20240209.txt"

# Postgres settings
pg_password = os.environ.get("PG_PW")
pg_host = os.environ.get("PG_HOST")


with CharterDb(pg_host, pg_password) as db:
    with MomBackup(backup_zip) as backup:
        # db.setup_db(["users"])
        db.setup_db()

        # insert users
        print("Listing users...")
        users = backup.list_users()
        print("Inserting users...")
        db.insert_users(users)

        # insert images
        images = ImagesFile(files_path).list_images()
        print("Images listed: ", len(images))
        db.insert_images(images)
        print("Images inserted")

        # insert archives
        print("Listing archives...")
        archives = backup.list_archives()

        # insert archivals fonds
        print("Inserting archives...")
        db.insert_archives(archives)
        print("Listing fonds...")
        fonds = backup.list_fonds(archives)
        print("Inserting fonds...")
        db.insert_fonds(fonds)
        print("Listing fond charters...")
        fond_charters = backup.list_fond_charters(fonds, users)
        print("Inserting fond charters...")
        db.insert_fonds_charters(fond_charters)

        # insert collections
        print("Listing collections...")
        collections = backup.list_collections(fonds)
        db.insert_collections(collections)
        print("Listing collection charters...")
        collection_charters = backup.list_collection_charters(collections, users)
        print("Inserting collection charters...")
        db.insert_collections_charters(collection_charters)

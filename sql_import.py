import os

from modules.logger import Logger
from modules.models.charter_db import CharterDb
from modules.models.images_file import ImagesFile
from modules.models.mom_backup import MomBackup

log = Logger()

# Backup settings
backup_zip = "./data/full20240223-0400.zip"
# backup_zip = "./data/full20210819-0400.zip"

# Image file list settings
image_files_path = "./data/filelist_20240209.txt"

# Postgres settings
pg_password = os.environ.get("PG_PW")
pg_host = os.environ.get("PG_HOST")

log.info(f"Connecting to database at {pg_host}")
with CharterDb(pg_host, pg_password) as db:
    log.info(f"Opening zip file {backup_zip}...")
    with MomBackup(backup_zip) as backup:
        log.info("Setting up database...")
        db.setup_db()

        # insert index locations
        log.info("Inserting index locations...")
        db.insert_index_locations()

        # insert users
        log.info("Listing users...")
        users = backup.list_users()
        log.info(f"Inserting {len(users)} users...")
        db.insert_users(users)

        # insert images
        log.info("Listing images...")
        images = ImagesFile(image_files_path).list_images()
        log.info(f"Inserting {len(images)} images...")
        db.insert_images(images)

        # Initialize person index
        log.info("Initializing person index...")
        person_index = backup.init_person_index()

        # insert archives
        log.info("Listing archives...")
        archives = backup.list_archives()
        log.info(f"Inserting {len(archives)} archives...")
        db.insert_archives(archives)

        # insert fonds
        log.info("Listing fonds...")
        fonds = backup.list_fonds(archives)
        log.info(f"Inserting {len(fonds)} fonds...")
        db.insert_fonds(fonds)

        # insert fond charters
        log.info("Listing fond charters...")
        fond_charters = backup.list_fond_charters(fonds, users, person_index)
        log.info(f"Inserting {len(fond_charters)} fond charters...")
        db.insert_fonds_charters(fond_charters)

        # insert collections
        log.info("Listing collections...")
        collections = backup.list_collections(fonds)
        log.info(f"Inserting {len(collections)} collections...")
        db.insert_collections(collections)

        # insert collection charters
        log.info("Listing collection charters...")
        collection_charters = backup.list_collection_charters(
            collections, users, person_index
        )
        log.info(f"Inserting {len(collection_charters)} collection charters...")
        db.insert_collections_charters(collection_charters)

        public_charters = fond_charters + collection_charters

        # insert user bookmarks
        log.info("Inserting user charter bookmarks...")
        db.insert_user_charter_bookmarks(users)

        # insert saved charters
        log.info("Listing saved charters...")
        saved_charters = backup.list_saved_charters(
            users, fonds, collections, person_index
        )
        log.info(f"Inserting {len(saved_charters)} saved charters...")
        db.insert_saved_charters(saved_charters)

        # insert private mycollections
        log.info("Listing private collections...")
        private_mycollections = backup.list_private_mycollections(users)
        log.info(f"Inserting {len(private_mycollections)} private mycollections...")
        db.insert_private_collections(private_mycollections)

        # insert private mycollection charters
        log.info("Listing private collection charters...")
        private_mycharters = backup.list_private_charters(
            users, private_mycollections, public_charters, person_index
        )
        log.info(f"Inserting {len(private_mycharters)} private collection charters...")
        db.insert_private_mycharters(private_mycharters)

        # insert public mycollections
        log.info("Listing public collections...")
        public_mycollections = backup.list_public_mycollections(
            users, private_mycollections
        )
        log.info(f"Inserting {len(public_mycollections)} public mycollections...")
        db.insert_public_mycollections(public_mycollections)

        # insert public mycollection charters
        log.info("Listing public collection charters...")
        public_mycharters = backup.list_public_charters(
            private_mycharters, public_mycollections, person_index
        )
        log.info(f"Inserting {len(public_mycharters)} public collection charters...")
        db.insert_public_mycharters(public_mycharters)

        public_charters = public_charters + public_mycharters

        # insert persons
        log.info(f"Inserting {person_index.count_persons()} indexes...")
        db.insert_persons(
            person_index, public_charters, private_mycharters, saved_charters
        )

        # reset sequences
        log.info("Resetting id sequences...")
        db.reset_serial_id_sequences()

        # enable triggers
        log.info("Enabling triggers...")
        db.enable_triggers()

        # finished
        log.info("Database import complete")

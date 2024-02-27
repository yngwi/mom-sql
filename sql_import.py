import os

from modules.models.charter_db import CharterDb
from modules.models.images_file import ImagesFile
from modules.models.mom_backup import MomBackup

# Backup settings
backup_zip = "./data/full20240223-0400.zip"
# backup_zip = "./data/full20210819-0400.zip"

# Image file list settings
image_files_path = "./data/filelist_20240209.txt"

# Postgres settings
pg_password = os.environ.get("PG_PW")
pg_host = os.environ.get("PG_HOST")

print(f"*** Connecting to database at {pg_host}")
with CharterDb(pg_host, pg_password) as db:
    print(f"*** Opening zip file {backup_zip}...")
    with MomBackup(backup_zip) as backup:
        print("\n** Setting up database...")
        db.setup_db()

        # insert index locations
        print("\n** Inserting index locations...")
        db.insert_index_locations()

        # insert users
        print("\n** Listing users...")
        users = backup.list_users()
        print(f"** Inserting {len(users)} users...")
        db.insert_users(users)

        # insert images
        print("\n** Listing images...")
        images = ImagesFile(image_files_path).list_images()
        print(f"** Inserting {len(images)} images...")
        db.insert_images(images)

        # Initialize person index
        print("\n** Initializing person index...")
        person_index = backup.init_person_index()

        # insert archives
        print("\n** Listing archives...")
        archives = backup.list_archives()
        print(f"** Inserting {len(archives)} archives...")
        db.insert_archives(archives)

        # insert fonds
        print("\n** Listing fonds...")
        fonds = backup.list_fonds(archives)
        print(f"** Inserting {len(fonds)} fonds...")
        db.insert_fonds(fonds)

        # insert fond charters
        print("\n** Listing fond charters...")
        fond_charters = backup.list_fond_charters(fonds, users, person_index)
        print(f"** Inserting {len(fond_charters)} fond charters...")
        db.insert_fonds_charters(fond_charters)

        # insert collections
        print("\n** Listing collections...")
        collections = backup.list_collections(fonds)
        print(f"** Inserting {len(collections)} collections...")
        db.insert_collections(collections)

        # insert collection charters
        print("\n** Listing collection charters...")
        collection_charters = backup.list_collection_charters(
            collections, users, person_index
        )
        print(f"** Inserting {len(collection_charters)} collection charters...")
        db.insert_collections_charters(collection_charters)

        public_charters = fond_charters + collection_charters

        # insert user bookmarks
        print("\n** Inserting user charter bookmarks...")
        db.insert_user_charter_bookmarks(users)

        # insert saved charters
        print("\n** Listing saved charters...")
        saved_charters = backup.list_saved_charters(
            users, fonds, collections, person_index
        )
        print(f"** Inserting {len(saved_charters)} saved charters...")
        db.insert_saved_charters(saved_charters)

        # insert private mycollections
        print("\n** Listing private collections...")
        private_mycollections = backup.list_private_mycollections(users)
        print(f"** Inserting {len(private_mycollections)} private mycollections...")
        db.insert_private_collections(private_mycollections)

        # insert private mycollection charters
        print("\n** Listing private collection charters...")
        private_mycharters = backup.list_private_charters(
            users, private_mycollections, public_charters, person_index
        )
        print(f"** Inserting {len(private_mycharters)} private collection charters...")
        db.insert_private_mycharters(private_mycharters)

        # insert public mycollections
        print("\n** Listing public collections...")
        public_mycollections = backup.list_public_mycollections(
            users, private_mycollections
        )
        print(f"** Inserting {len(public_mycollections)} public mycollections...")
        db.insert_public_mycollections(public_mycollections)

        # insert public mycollection charters
        print("\n** Listing public collection charters...")
        public_mycharters = backup.list_public_charters(
            private_mycharters, public_mycollections, person_index
        )
        print(f"** Inserting {len(public_mycharters)} public collection charters...")
        db.insert_public_mycharters(public_mycharters)

        public_charters = public_charters + public_mycharters

        # insert persons
        print(f"\n** Inserting {person_index.count_persons()} indexes...")
        db.insert_persons(
            person_index, public_charters, private_mycharters, saved_charters
        )

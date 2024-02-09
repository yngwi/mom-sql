from modules.models.charter_db import CharterDb
from modules.models.images_file import ImagesFile
from modules.models.mom_backup import MomBackup

# backup_zip = "./data/full20240202-1515.zip"
backup_zip = "./data/full20210819-0400.zip"
db_path = "./mom.sqlite3"
files_path = "./data/filelist_20240209.txt"

with CharterDb(db_path) as db:
    with MomBackup(backup_zip) as backup:
        db.setup_db()
        # insert images
        images = ImagesFile(files_path).list_images()
        db.insert_images(images)
        # insert archives
        archives = backup.list_archives()
        # insert archivals fonds
        db.insert_archives(archives)
        fonds = backup.list_fonds(archives)
        db.insert_fonds(fonds)
        # TODO: for testing, remove
        # fonds = [
        #     fond
        #     for fond in fonds
        #     # if fond.archive_file == "HR-HDA" and fond.file == "643"
        #     if fond.archive_file == "DE-StaAWo" and fond.file == "Abt1AI"
        # ]
        # fonds = fonds[:1]
        fond_charters = backup.list_fond_charters(fonds)
        db.insert_fonds_charters(fond_charters)

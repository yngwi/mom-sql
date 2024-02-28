# MOM-Check DB transformation

## Introduction

This repository contains python code that, when run with provided with paths to
a Monasterium MOM-CA database backup archive file and a text file with the paths
of all images on the Monasterium image server as well as connections to a
PostgreSQL server, sets up a database and transforms the XML-based data from the
backup into relational data in the new relational database. The resulting
database aims at staying true to the current XML-based structures in the MOM-CA
eXist database. During the transformations some inconstencies are fixed and
structured relationships are being established, but no new elements are
introduced and the XML data objects still exist. This enables the use of this
database for research and error-finding purposes that are more difficult with
the original data structure.

At the same time, the transformation process and SQL database setup aims at
being a proof-of-concept for a mapping of the document-based no-SQL databese
structure into a relational one therefore modeling the relations between the
various entities and there properties in a more approachable and efficient way
while still retaining much of the expressiveness of XML where beneficial. Among
other things, it utilizes the existing native XML functionalities of PostgreSQL
to store some of the more complex text fields in the Postgres XML data type and
at the same time makes the text contents of these fields available in generated
fields based on the XML content. It also extracts person names from some of
these XML fields dynamically and stores them in a person name table among the
person names gotten from the original charters’ explicit _cei:back_ index
elements.

_Note:_ Only some of the XML content is exacted as the whole process related to
the XML content is only intended to be a proof-of-concept, as has been stated
above. More could be added at any time.

### Examplary questions that can be answered with the new database

- What images on the image server are never being used in any charter
- Find charters that don’t have a date, an abstract, images, a “recent” editor
  and other things
- Identify users with a specific moderator
- Identify users with saved charters that are missing due to eXist problems
- Find saved charters where the original charter doesn’t exist any more
- Show published saved charters that are abandoned by their moderator
- Find fonds or collections with fonds that don’t have any content, possibly due
  to incomplete import processes

## Environment

### Python

This repository provides a `requirements.txt` file that can be used in
combination with Python `pip` to set up the necessary dependencies on the sytem
or a virtual environment.

### Environment variables

The following environment variables can/have to be defined for the script to be
executed successfully.

| Variable        | Default    | Example                  | Description                               |
| --------------- | ---------- | ------------------------ | ----------------------------------------- |
| BACKUP_PATH     |            | `/full20210819-0400.zip` | The path to the full MOM-CA backup        |
| IMAGE_LIST_PATH |            | `/imagelist.txt`         | The path to the image file path list      |
| PG_DB           | `momcheck` | `momcheck`               | The name of the db to be created and used |
| PG_HOST         |            | `localhost`              | The postgres db host                      |
| PG_PORT         | `5432`     | `5432`                   | The postgres db port                      |
| PG_PW           |            | `mom_is_superb_software` | The postgres db user password             |
| PG_USER         | `postgres` | `postgres`               | The postgres db user to use the db        |

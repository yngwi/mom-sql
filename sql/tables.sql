-- Table to store user information
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    first_name TEXT,
    moderator_id INTEGER,
    name TEXT,
    FOREIGN KEY (moderator_id) REFERENCES users (id)
);
CREATE INDEX ON users (moderator_id);

-- Table to store person entities
CREATE TABLE IF NOT EXISTS persons (
    id SERIAL PRIMARY KEY,
    label TEXT NOT NULL,
    mom_iri TEXT UNIQUE,
    wikidata_iri TEXT UNIQUE
);
CREATE INDEX ON persons (label);

-- Table to store unique locations
CREATE TABLE IF NOT EXISTS index_locations (
    id SERIAL PRIMARY KEY,
    location TEXT UNIQUE NOT NULL
);
CREATE INDEX ON index_locations (location);

-- Table for storing private collections with an owner
CREATE TABLE IF NOT EXISTS private_collections (
    id SERIAL PRIMARY KEY,
    atom_id TEXT NOT NULL UNIQUE,
    identifier TEXT NOT NULL,
    title TEXT NOT NULL,
    owner_id INTEGER NOT NULL,
    FOREIGN KEY (owner_id) REFERENCES users (id)
);
CREATE INDEX ON private_collections (owner_id);

-- Table for general collections that may or may not be private
CREATE TABLE IF NOT EXISTS collections (
    id SERIAL PRIMARY KEY,
    atom_id TEXT NOT NULL UNIQUE,
    identifier TEXT NOT NULL,
    image_base TEXT,
    oai_shared BOOLEAN DEFAULT FALSE,
    source_collection_id INTEGER,
    title TEXT NOT NULL,
    FOREIGN KEY (source_collection_id) REFERENCES private_collections (id)
);
CREATE INDEX ON collections (source_collection_id);

-- Table to store information about archives
CREATE TABLE IF NOT EXISTS archives (
    id SERIAL PRIMARY KEY,
    atom_id TEXT NOT NULL UNIQUE,
    country_code CHAR(2) NOT NULL,
    name TEXT NOT NULL,
    oai_shared BOOLEAN DEFAULT FALSE,
    repository_id TEXT NOT NULL
);

-- Table for fonds within an archive
CREATE TABLE IF NOT EXISTS fonds (
    id SERIAL PRIMARY KEY,
    archive_id INTEGER NOT NULL,
    atom_id TEXT NOT NULL UNIQUE,
    free_image_access BOOLEAN NOT NULL DEFAULT FALSE,
    identifier TEXT NOT NULL,
    image_base TEXT,
    oai_shared BOOLEAN DEFAULT FALSE,
    title TEXT NOT NULL,
    FOREIGN KEY (archive_id) REFERENCES archives (id)
);
CREATE INDEX ON fonds (archive_id);
CREATE INDEX ON fonds (image_base);

-- Table to store individual charters
CREATE TABLE IF NOT EXISTS charters (
    id SERIAL PRIMARY KEY,
    abstract XML,
    atom_id TEXT NOT NULL UNIQUE,
    idno_id TEXT,
    idno_text TEXT,
    issued_date DATERANGE,
    issued_date_text TEXT,
    last_editor_id INTEGER,
    sort_date DATE NOT NULL DEFAULT CURRENT_DATE,
    tenor XML,
    url TEXT NOT NULL,
    FOREIGN KEY (last_editor_id) REFERENCES users (id)
);
CREATE INDEX ON charters (last_editor_id);
CREATE INDEX ON charters (sort_date);


-- Table for storing versions of charters that are being edited
CREATE TABLE IF NOT EXISTS saved_charters (
    id SERIAL PRIMARY KEY,
    abstract XML,
    atom_id TEXT NOT NULL UNIQUE,
    editor_id INTEGER NOT NULL,
    idno_id TEXT,
    idno_text TEXT,
    is_released BOOLEAN NOT NULL DEFAULT FALSE,
    issued_date DATERANGE,
    issued_date_text TEXT,
    original_charter_id INTEGER NOT NULL,
    sort_date DATE NOT NULL DEFAULT CURRENT_DATE,
    start_time TIMESTAMP NOT NULL,
    tenor XML,
    url TEXT NOT NULL,
    FOREIGN KEY (editor_id) REFERENCES users (id),
    FOREIGN KEY (original_charter_id) REFERENCES charters (id)
);
CREATE INDEX ON saved_charters (editor_id);
CREATE INDEX ON saved_charters (sort_date);

-- Table for private charters within a private collection
CREATE TABLE IF NOT EXISTS private_charters (
    id SERIAL PRIMARY KEY,
    abstract XML,
    atom_id TEXT NOT NULL,
    idno_id TEXT,
    idno_text TEXT,
    issued_date DATERANGE,
    issued_date_text TEXT,
    private_collection_id INTEGER NOT NULL,
    sort_date DATE NOT NULL DEFAULT CURRENT_DATE,
    source_charter_id INTEGER,
    tenor XML,
    FOREIGN KEY (private_collection_id) REFERENCES private_collections (id),
    FOREIGN KEY (source_charter_id) REFERENCES charters (id)
);
CREATE INDEX ON private_charters (private_collection_id);
CREATE INDEX ON private_charters (sort_date);
CREATE INDEX ON private_charters (source_charter_id);

-- Table for sharing private charters with other users
CREATE TABLE IF NOT EXISTS private_charter_user_shares (
    private_charter_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    FOREIGN KEY (private_charter_id) REFERENCES private_charters (id),
    FOREIGN KEY (user_id) REFERENCES users (id),
    PRIMARY KEY (private_charter_id, user_id)
);
CREATE INDEX ON private_charter_user_shares (private_charter_id);
CREATE INDEX ON private_charter_user_shares (user_id);

-- Association table for collections and charters
CREATE TABLE IF NOT EXISTS collections_charters (
    collection_id INTEGER NOT NULL,
    charter_id INTEGER NOT NULL,
    private_charter_id INTEGER,
    FOREIGN KEY (collection_id) REFERENCES collections (id),
    FOREIGN KEY (charter_id) REFERENCES charters (id),
    FOREIGN KEY (private_charter_id) REFERENCES private_charters (id),
    PRIMARY KEY (collection_id, charter_id)
);
CREATE INDEX ON collections_charters (charter_id);
CREATE INDEX ON collections_charters (collection_id);
CREATE INDEX ON collections_charters (private_charter_id);

-- Association table for fonds and charters
CREATE TABLE IF NOT EXISTS fonds_charters (
    fond_id INTEGER NOT NULL,
    charter_id INTEGER NOT NULL,
    FOREIGN KEY (fond_id) REFERENCES fonds (id),
    FOREIGN KEY (charter_id) REFERENCES charters (id),
    PRIMARY KEY (fond_id, charter_id)
);
CREATE INDEX ON fonds_charters (charter_id);
CREATE INDEX ON fonds_charters (fond_id);

-- Association table for collections and fonds
CREATE TABLE IF NOT EXISTS collection_fonds (
    collection_id INTEGER NOT NULL,
    fond_id INTEGER NOT NULL,
    FOREIGN KEY (collection_id) REFERENCES collections (id),
    FOREIGN KEY (fond_id) REFERENCES fonds (id),
    PRIMARY KEY (collection_id, fond_id)
);
CREATE INDEX ON collection_fonds (collection_id);
CREATE INDEX ON collection_fonds (fond_id);

-- Table to store images
CREATE TABLE IF NOT EXISTS images (
    id SERIAL PRIMARY KEY,
    url TEXT UNIQUE NOT NULL,
    is_external BOOLEAN NOT NULL DEFAULT TRUE
);

-- Association table for charters and images
CREATE TABLE IF NOT EXISTS charters_images (
    charter_id INTEGER NOT NULL,
    image_id INTEGER NOT NULL,
    FOREIGN KEY (charter_id) REFERENCES charters (id),
    FOREIGN KEY (image_id) REFERENCES images (id),
    PRIMARY KEY (charter_id, image_id)
);
CREATE INDEX ON charters_images (charter_id);
CREATE INDEX ON charters_images (image_id);

CREATE TABLE IF NOT EXISTS person_names (
    id SERIAL PRIMARY KEY,
    person_id INTEGER,
    text TEXT NOT NULL,
    reg TEXT,
    key TEXT,
    FOREIGN KEY (person_id) REFERENCES persons (id)
);
CREATE INDEX ON person_names (key);
CREATE INDEX ON person_names (person_id);
CREATE INDEX ON person_names (reg);
CREATE INDEX ON person_names (text);


-- Table to store person names mentioned in charters
CREATE TABLE IF NOT EXISTS charters_person_names (
    charter_id INTEGER NOT NULL,
    person_name_id INTEGER NOT NULL,
    location_id INTEGER NOT NULL,
    PRIMARY KEY (charter_id, person_name_id),
    FOREIGN KEY (charter_id) REFERENCES charters (id),
    FOREIGN KEY (person_name_id) REFERENCES person_names (id),
    FOREIGN KEY (location_id) REFERENCES index_locations (id)
);
CREATE INDEX ON charters_person_names (charter_id);
CREATE INDEX ON charters_person_names (location_id);
CREATE INDEX ON charters_person_names (person_name_id);

-- Similar to `charters_person_names` but for saved charters
CREATE TABLE IF NOT EXISTS saved_charters_person_names (
    saved_charter_id INTEGER NOT NULL,
    person_name_id INTEGER NOT NULL,
    location_id INTEGER NOT NULL,
    PRIMARY KEY (saved_charter_id, person_name_id),
    FOREIGN KEY (saved_charter_id) REFERENCES saved_charters (id),
    FOREIGN KEY (person_name_id) REFERENCES person_names (id),
    FOREIGN KEY (location_id) REFERENCES index_locations (id)
);
CREATE INDEX ON saved_charters_person_names (location_id);
CREATE INDEX ON saved_charters_person_names (person_name_id);
CREATE INDEX ON saved_charters_person_names (saved_charter_id);


-- Association table for saved charters and images
CREATE TABLE IF NOT EXISTS saved_charters_images (
    saved_charter_id INTEGER NOT NULL,
    image_id INTEGER NOT NULL,
    FOREIGN KEY (saved_charter_id) REFERENCES saved_charters (id),
    FOREIGN KEY (image_id) REFERENCES images (id),
    PRIMARY KEY (saved_charter_id, image_id)
);
CREATE INDEX ON saved_charters_images (image_id);
CREATE INDEX ON saved_charters_images (saved_charter_id);

-- Association table for private charters and images
CREATE TABLE IF NOT EXISTS private_charters_images (
    private_charter_id INTEGER NOT NULL,
    image_id INTEGER NOT NULL,
    FOREIGN KEY (private_charter_id) REFERENCES private_charters (id),
    FOREIGN KEY (image_id) REFERENCES images (id),
    PRIMARY KEY (private_charter_id, image_id)
);
CREATE INDEX ON private_charters_images (image_id);
CREATE INDEX ON private_charters_images (private_charter_id);

-- Similar to `charters_person_names` but for private charters
CREATE TABLE IF NOT EXISTS private_charters_person_names (
    private_charter_id INTEGER NOT NULL,
    person_name_id INTEGER NOT NULL,
    location_id INTEGER NOT NULL,
    PRIMARY KEY (private_charter_id, person_name_id),
    FOREIGN KEY (private_charter_id) REFERENCES private_charters (id),
    FOREIGN KEY (person_name_id) REFERENCES person_names (id),
    FOREIGN KEY (location_id) REFERENCES index_locations (id)
);
CREATE INDEX ON private_charters_person_names (location_id);
CREATE INDEX ON private_charters_person_names (person_name_id);
CREATE INDEX ON private_charters_person_names (private_charter_id);

-- Table for users to bookmark charters with optional notes
CREATE TABLE IF NOT EXISTS user_charter_bookmarks (
    user_id INTEGER NOT NULL,
    charter_id INTEGER NOT NULL,
    note TEXT,
    FOREIGN KEY (user_id) REFERENCES users (id),
    FOREIGN KEY (charter_id) REFERENCES charters (id),
    PRIMARY KEY (user_id, charter_id)
);
CREATE INDEX ON user_charter_bookmarks (charter_id);
CREATE INDEX ON user_charter_bookmarks (note);
CREATE INDEX ON user_charter_bookmarks (user_id);

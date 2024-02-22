CREATE INDEX ON charters (last_editor_id);
CREATE INDEX ON charters (sort_date);
CREATE INDEX ON charters USING btree (issuer_text);
CREATE INDEX ON charters USING gin (TO_TSVECTOR('simple', abstract_fulltext));
CREATE INDEX ON charters USING gin (TO_TSVECTOR('simple', tenor_fulltext));

CREATE INDEX ON charters_images (charter_id);
CREATE INDEX ON charters_images (image_id);

CREATE INDEX ON charters_person_names (charter_id);
CREATE INDEX ON charters_person_names (key);
CREATE INDEX ON charters_person_names (location_id);
CREATE INDEX ON charters_person_names (person_id);
CREATE INDEX ON charters_person_names (reg);
CREATE INDEX ON charters_person_names (text);

CREATE INDEX ON collection_fonds (collection_id);
CREATE INDEX ON collection_fonds (fond_id);

CREATE INDEX ON collections (source_collection_id);

CREATE INDEX ON collections_charters (charter_id);
CREATE INDEX ON collections_charters (collection_id);
CREATE INDEX ON collections_charters (private_charter_id);

CREATE INDEX ON fonds (archive_id);
CREATE INDEX ON fonds (image_base);

CREATE INDEX ON fonds_charters (charter_id);
CREATE INDEX ON fonds_charters (fond_id);

CREATE INDEX ON index_locations (location);

CREATE INDEX ON persons (label);

CREATE INDEX ON private_charter_user_shares (private_charter_id);
CREATE INDEX ON private_charter_user_shares (user_id);

CREATE INDEX ON private_charters (private_collection_id);
CREATE INDEX ON private_charters (sort_date);
CREATE INDEX ON private_charters (source_charter_id);

CREATE INDEX ON private_charters_images (image_id);
CREATE INDEX ON private_charters_images (private_charter_id);

CREATE INDEX ON private_charters_person_names (key);
CREATE INDEX ON private_charters_person_names (location_id);
CREATE INDEX ON private_charters_person_names (person_id);
CREATE INDEX ON private_charters_person_names (private_charter_id);
CREATE INDEX ON private_charters_person_names (reg);
CREATE INDEX ON private_charters_person_names (text);

CREATE INDEX ON private_collections (owner_id);

CREATE INDEX ON saved_charters (editor_id);
CREATE INDEX ON saved_charters (sort_date);

CREATE INDEX ON saved_charters_images (image_id);
CREATE INDEX ON saved_charters_images (saved_charter_id);

CREATE INDEX ON saved_charters_person_names (key);
CREATE INDEX ON saved_charters_person_names (location_id);
CREATE INDEX ON saved_charters_person_names (person_id);
CREATE INDEX ON saved_charters_person_names (reg);
CREATE INDEX ON saved_charters_person_names (saved_charter_id);
CREATE INDEX ON saved_charters_person_names (text);

CREATE INDEX ON user_charter_bookmarks (charter_id);
CREATE INDEX ON user_charter_bookmarks (note);
CREATE INDEX ON user_charter_bookmarks (user_id);

CREATE INDEX ON users (moderator_id);

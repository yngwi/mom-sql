-- Create charters abstract field
ALTER TABLE charters ADD COLUMN abstract_fulltext TEXT GENERATED ALWAYS AS (
    public.mom_text_content('.//text()', abstract::XML)
) STORED;
CREATE INDEX ON charters USING gin (TO_TSVECTOR('simple', abstract_fulltext));

-- Create charters issuer field
ALTER TABLE charters ADD COLUMN issuer_text TEXT GENERATED ALWAYS AS (
    public.mom_text_content('.//cei:issuer//text()', abstract::XML)
) STORED;
CREATE INDEX ON charters USING btree (issuer_text);

-- Create charters tenor field
ALTER TABLE charters ADD COLUMN tenor_fulltext TEXT GENERATED ALWAYS AS (
    public.mom_text_content('.//text()', tenor::XML)
) STORED;
CREATE INDEX ON charters USING gin (TO_TSVECTOR('simple', tenor_fulltext));

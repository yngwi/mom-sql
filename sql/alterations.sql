-- Add various calculated columns to the charters table
ALTER TABLE charters ADD COLUMN abstract_fulltext TEXT GENERATED ALWAYS AS (
    public.xpath_to_text(abstract::XML, './/text()')
) STORED;
ALTER TABLE charters ADD COLUMN issuer_text TEXT GENERATED ALWAYS AS (
    public.xpath_to_text(abstract::XML, './/cei:issuer//text()')
) STORED;
ALTER TABLE charters ADD COLUMN tenor_fulltext TEXT GENERATED ALWAYS AS (
    public.xpath_to_text(tenor::XML, './/text()')
) STORED;

-- Escapes all special characters in a given text string so that it
-- can be used as a literal in a regular expression.
CREATE OR REPLACE FUNCTION public.regexp_escape(input_text TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE STRICT PARALLEL SAFE
AS $function$
BEGIN
    RETURN regexp_replace(input_text, '([!$()*+.:<=>?[\\\]^{|}-])', '\\\1', 'g');
END;
$function$;

-- Removes all XML namespace declarations from the given XML text.
CREATE OR REPLACE FUNCTION public.remove_xml_namespaces(xml_text TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE STRICT PARALLEL SAFE
AS $function$
BEGIN
    RETURN regexp_replace(xml_text, ' xmlns(:[a-zA-Z0-9_-]+)?="[^"]*"', '', 'g');
END;
$function$;

-- Get the XML results matching a given XPath expression with
-- the common MOM namespaces already defined.
CREATE OR REPLACE FUNCTION public.mom_xpath(mom_xpath TEXT, input_xml XML)
RETURNS XML []
LANGUAGE plpgsql
IMMUTABLE
AS $function$
DECLARE
    mom_namespaces TEXT[] := ARRAY[
        ARRAY['cei', 'http://www.monasterium.net/NS/cei']
    ];
BEGIN
    RETURN xpath(mom_xpath, input_xml, mom_namespaces);
END;
$function$;

-- Function to check if the XML data contains any nested XML
-- content inside a specific root element. Has the common
-- MOM namespaces already defined.
CREATE OR REPLACE FUNCTION public.mom_has_any_xml(
    root_element_name TEXT, xml_data XML
)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
AS $function$
DECLARE
    element_count INTEGER;
    xpath TEXT := format('/%s/*', root_element_name);
BEGIN
    SELECT count(*) INTO element_count
    FROM unnest(mom_xpath(xpath, xml_data)) AS e(element_nodes);
    RETURN element_count > 0;
END;
$function$;

-- Function to extract text from an XML element using an XPath expression.
-- Has the common MOM namespaces already defined.
CREATE OR REPLACE FUNCTION public.mom_text_content(
    mom_xpath TEXT, xml_input XML
)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $function$
DECLARE
    text_nodes TEXT[];
    result TEXT := '';
BEGIN
    text_nodes := ARRAY(SELECT unnest(mom_xpath(mom_xpath, xml_input)));
    IF text_nodes IS NULL OR array_length(text_nodes, 1) = 0 THEN
        RETURN NULL;
    ELSE
        SELECT trim(BOTH FROM regexp_replace(string_agg(node, ' '), '[\n\r\t]| +', ' ', 'g'), ' ') INTO result FROM unnest(text_nodes) AS node;
        RETURN result;
    END IF;
END;
$function$;

-- Extracts the content of the first processing instruction with a given name.
-- Has the common MOM namespaces already defined.
CREATE OR REPLACE FUNCTION public.mom_proc_inst_content(
    proc_inst_name TEXT, parent_xpath TEXT, xml_input XML
)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $function$
DECLARE
    result XML[];
    proc_inst_content TEXT;
    matches TEXT[];
BEGIN
    result := mom_xpath(parent_xpath || '/processing-instruction(' || quote_literal(proc_inst_name) || ')', xml_input);
    IF cardinality(result) > 0 THEN
        proc_inst_content := result[1]::text;
        matches := regexp_matches(proc_inst_content, '<\?' || proc_inst_name || '\s+(.*?)\?>');
        IF matches IS NOT NULL THEN
            RETURN matches[1];
        END IF;
    END IF;
    RETURN NULL;
END;
$function$;

-- Function to insert or replace a processing instruction in an XML element.
CREATE OR REPLACE FUNCTION public.mom_replace_proc_inst(
    proc_inst_name TEXT, proc_inst_content TEXT, xml_input XML
)
RETURNS XML
LANGUAGE plpgsql
IMMUTABLE
AS $function$
DECLARE
    existing_pi_content TEXT;
    old_pi TEXT;
    new_pi TEXT := format('<?%s %s?>', proc_inst_name, proc_inst_content);
    xml_output XML;
BEGIN
    existing_pi_content := public.mom_proc_inst_content(proc_inst_name, '/*', xml_input);
    IF existing_pi_content IS NULL THEN
        -- Insert the new processing instruction directly after the closing tag of the root element.
        xml_output := regexp_replace(xml_input::text, '(>)', '>' || new_pi, 1, 1, 'g');
    ELSE
        -- Replace the existing processing instruction with the new one.
        old_pi := format('<?%s %s?>', proc_inst_name, existing_pi_content);
        xml_output := regexp_replace(xml_input::text, public.regexp_escape(old_pi), new_pi, 1, 1, 'g');
    END IF;
    RETURN xml_output::xml;
END;
$function$;

-- Function to process person names in charter XML data
CREATE OR REPLACE FUNCTION public.process_charter_person_names(
    current_charter_id INTEGER, current_location_id INTEGER, xml_input XML
)
RETURNS XML
LANGUAGE plpgsql
VOLATILE
AS $function$
DECLARE
    current_person_id INTEGER;
    current_person_name_id INTEGER;
    existing_person_name_ids INTEGER[] := ARRAY[]::INTEGER[];
    new_pers_name XML;
    pers_name XML;
    pers_name_key TEXT;
    pers_name_reg TEXT;
    pers_name_text TEXT;
    to_delete_person_name_ids INTEGER[];
BEGIN
    FOREACH pers_name IN ARRAY COALESCE(mom_xpath('.//cei:persName', xml_input), ARRAY[]::XML[]) loop
        -- Get db ids
        current_person_name_id := mom_proc_inst_content('person_names', '/*', pers_name)::INT;
        current_person_id := mom_proc_inst_content('persons', '/*', pers_name)::INT;
        -- Get XML data
        pers_name_text := mom_text_content('/cei:persName//text()', pers_name);
        pers_name_key := mom_text_content('/cei:persName/@key', pers_name);
        pers_name_reg := mom_text_content('/cei:persName/@reg', pers_name);
        -- Upsert person_names row
        IF pers_name_text IS NOT NULL THEN
            IF current_person_name_id IS NOT NULL THEN
                UPDATE public.person_names 
                SET "key" = pers_name_key,
                    location_id = current_location_id,
                    person_id = current_person_id,
                    reg = pers_name_reg,
                    text = pers_name_text 
                WHERE id = current_person_name_id;
            ELSE
                INSERT INTO public.person_names ("key", location_id, person_id, reg, text) 
                VALUES (pers_name_key, current_location_id, current_person_id, pers_name_reg, pers_name_text)
                RETURNING id INTO current_person_name_id;
            END IF;
            existing_person_name_ids := array_append(existing_person_name_ids, current_person_name_id);
            new_pers_name := mom_replace_proc_inst('person_names', current_person_name_id::TEXT, pers_name);
            xml_input := REPLACE(
                xml_input::TEXT, 
                public.remove_xml_namespaces(pers_name::TEXT),    
                public.remove_xml_namespaces(new_pers_name::TEXT)
            )::XML;
        END IF;
        -- Upsert charter_person_names row
        INSERT INTO public.charters_person_names (charter_id, person_name_id)
        VALUES (current_charter_id, current_person_name_id)
        ON CONFLICT (charter_id, person_name_id) DO NOTHING;
    END LOOP;
    to_delete_person_name_ids := ARRAY(
        SELECT pn.id FROM public.charters_person_names cpn 
        JOIN public.person_names pn ON pn.id = cpn.person_name_id  
        WHERE cpn.charter_id = current_charter_id AND pn.location_id = current_location_id AND NOT (cpn.person_name_id = ANY (existing_person_name_ids))
    );
    DELETE FROM public.charters_person_names WHERE person_name_id = ANY(to_delete_person_name_ids);
    DELETE FROM public.person_names WHERE id = ANY(to_delete_person_name_ids);
    RETURN xml_input;
END;
$function$;

-- Trigger function to transform charter data, mainly
-- related to data in XML fields
CREATE OR REPLACE FUNCTION public.charters_on_upsert()
RETURNS TRIGGER
LANGUAGE plpgsql
VOLATILE
AS $function$
DECLARE
    current_location_id INTEGER;
BEGIN
    -- Handle abstract xml related to person names
    SELECT id INTO current_location_id FROM public.index_locations WHERE location = 'ABSTRACT' LIMIT 1;
    NEW.abstract = public.process_charter_person_names(NEW.id, current_location_id, NEW.abstract);
    -- Handle tenor xml related to person names
    SELECT id INTO current_location_id FROM public.index_locations WHERE location = 'TENOR' LIMIT 1;
    NEW.tenor = public.process_charter_person_names(NEW.id, current_location_id, NEW.tenor);
    -- Return updated charter
    RETURN NEW;
END;
$function$;

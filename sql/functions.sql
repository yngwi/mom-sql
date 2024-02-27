-- Escapes all special characters in a given text string so that it
-- can be used as a literal in a regular expression.
CREATE OR REPLACE FUNCTION public.regexp_escape(input_text TEXT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE STRICT PARALLEL SAFE
AS $function$
SELECT regexp_replace(input_text, '([!$()*+.:<=>?[\\\]^{|}-])', '\\\1', 'g')
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
    mom_namespaces text[] := ARRAY[
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
    xpath TEXT := '/' || root_element_name || '/*';
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
    text_nodes text[];
    result text := '';
BEGIN
    text_nodes := ARRAY(SELECT unnest(mom_xpath(mom_xpath, xml_input)));
    IF text_nodes IS NULL OR array_length(text_nodes, 1) = 0 THEN
        RETURN NULL;
    ELSE
        SELECT trim(both from regexp_replace(string_agg(node, ' '), '[\n\r\t]| +', ' ', 'g'), ' ') INTO result FROM unnest(text_nodes) AS node;
        RETURN result;
    END IF;
END
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
END
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
    existing_pi_content text;
    old_pi text;
    new_pi text := format('<?%s %s?>', proc_inst_name, proc_inst_content);
    xml_output xml;
BEGIN
    existing_pi_content := public.mom_proc_inst_content(proc_inst_name, '/*', xml_input);
    if existing_pi_content is NULL then
        -- Insert the new processing instruction directly after the closing tag of the root element.
        xml_output := regexp_replace(xml_input::text, '(>)', '>' || new_pi, 1, 1, 'g');
    else
        -- Replace the existing processing instruction with the new one.
        old_pi := format('<?%s %s?>', proc_inst_name, existing_pi_content);
       raise notice '%', old_pi;
        xml_output := regexp_replace(xml_input::text, public.regexp_escape(old_pi), new_pi, 1, 1, 'g');
    end if;
    return xml_output::xml;
END;
$function$;

CREATE OR REPLACE FUNCTION public.charters_on_upsert()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $function$
    declare
        abstract xml := NEW.abstract;
        current_location_id integer;
        current_person_id integer;
        current_person_name_id integer;
        existing_person_name_ids integer[] := array[]::integer[];
        new_pers_name xml;
        pers_name xml;
        pers_name_key text;
        pers_name_reg text;
        pers_name_text text;
        pers_names xml[];
        to_delete_person_name_ids integer[];
    begin
        -- Handle abstract
        pers_names := mom_xpath('.//cei:persName', abstract);
        SELECT id INTO current_location_id FROM public.index_locations il WHERE il.location = 'ABSTRACT' LIMIT 1;
        foreach pers_name in array pers_names loop
            -- get db ids
            current_person_name_id := mom_proc_inst_content('person_names', '/*', pers_name)::int;
            current_person_id := mom_proc_inst_content('persons', '/*', pers_name)::int;
            -- get xml data
            pers_name_text := mom_text_content('/cei:persName//text()', pers_name);
            pers_name_key := mom_text_content('/cei:persName/@key', pers_name);
            pers_name_reg := mom_text_content('/cei:persName/@reg', pers_name);
            -- upsert person_names row
            if pers_name_text is not null then
                raise notice '%s', current_person_name_id;
                if current_person_name_id is not null then
                    UPDATE public.person_names 
                        SET
                            "key" = pers_name_key,
                            location_id = current_location_id,
                            person_id = current_person_id,
                            reg = pers_name_reg,
                            text = pers_name_text 
                        where id = current_person_name_id;
                else
                    INSERT INTO public.person_names ("key", location_id, person_id, reg, text) 
                        VALUES (pers_name_key, current_location_id, current_person_id, pers_name_reg, pers_name_text)
                        returning id into current_person_name_id;
                end if;
                existing_person_name_ids := array_append(existing_person_name_ids, current_person_name_id);
                new_pers_name := mom_replace_proc_inst('person_names', current_person_name_id::text, pers_name);
                abstract := replace(
                    abstract::text, 
                    public.remove_xml_namespaces(pers_name::text),    
                    public.remove_xml_namespaces(new_pers_name::text)
                )::xml;
            end if;
            -- upsert charter_person_names row
            insert into public.charters_person_names (charter_id, person_name_id)
                values (NEW.id, current_person_name_id)
                on conflict (charter_id, person_name_id) do nothing;
        end loop;
        to_delete_person_name_ids := array(
            select pn.id from public.charters_person_names cpn 
                inner join public.person_names pn on pn.id = cpn.person_name_id  
                where cpn.charter_id = NEW.id and pn.location_id = current_location_id and not (cpn.person_name_id = any (existing_person_name_ids))
        );
        raise notice '%', to_delete_person_name_ids;
        delete from public.charters_person_names where (person_name_id = any( to_delete_person_name_ids));
        delete from public.person_names where (id = any( to_delete_person_name_ids));
        new.abstract := abstract;
        return NEW;
    end;
$function$;

CREATE TRIGGER charters_before_insert_or_update
BEFORE INSERT OR UPDATE ON public.charters
FOR EACH ROW EXECUTE FUNCTION public.charters_on_upsert()

-- Escapes all special characters in a given text string so that it
-- can be used as a literal in a regular expression.
CREATE OR REPLACE FUNCTION public.mom_regexp_escape(input_text TEXT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE STRICT PARALLEL SAFE
AS $function$
SELECT regexp_replace(input_text, '([!$()*+.:<=>?[\\\]^{|}-])', '\\\1', 'g')
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
        xml_output := regexp_replace(xml_input::text, public.mom_regexp_escape(old_pi), new_pi, 1, 1, 'g');
    end if;
    return xml_output::xml;
END;
$function$;

-- Function to check if the XML data contains content 
-- within a specific root element.
CREATE OR REPLACE FUNCTION public.has_xml_content(
    xml_data XML, root_element_name TEXT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
AS $function$
DECLARE
    namespace_array text[] := ARRAY[
        ARRAY['cei','http://www.monasterium.net/NS/cei'],
        ARRAY['atom','http://www.w3.org/2005/Atom'],
        ARRAY['ead','urn:isbn:1-931666-22-9'],
        ARRAY['eag','http://www.archivgut-online.de/eag'],
        ARRAY['exist','http://exist.sourceforge.net/NS/exist'],
        ARRAY['oei','http://www.monasterium.net/NS/oei'],
        ARRAY['xrx','http://www.monasterium.net/NS/xrx']
    ];
    element_count INTEGER;
BEGIN
    SELECT count(*)
    INTO element_count
    FROM unnest(xpath('/' || root_element_name || '/*', xml_data, namespace_array)) AS e(element_nodes);
    RETURN element_count > 0;
END;
$function$;

-- Function to extract text from an XML element using an XPath expression.
CREATE OR REPLACE FUNCTION public.xpath_to_text(xml_input XML, xpath_expr TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $function$
DECLARE
    namespace_array text[] := ARRAY[
        ARRAY['cei', 'http://www.monasterium.net/NS/cei'],
        ARRAY['atom', 'http://www.w3.org/2005/Atom'],
        ARRAY['ead', 'urn:isbn:1-931666-22-9'],
        ARRAY['eag', 'http://www.archivgut-online.de/eag'],
        ARRAY['exist', 'http://exist.sourceforge.net/NS/exist'],
        ARRAY['oei', 'http://www.monasterium.net/NS/oei'],
        ARRAY['xrx', 'http://www.monasterium.net/NS/xrx']
    ];
    text_nodes text[];
    result text := '';
BEGIN
    text_nodes := ARRAY(SELECT unnest(xpath(xpath_expr, xml_input, namespace_array)));
    IF text_nodes IS NULL OR array_length(text_nodes, 1) = 0 THEN
        RETURN NULL; -- Returns NULL if no text nodes are found
    ELSE
        SELECT string_agg(trim(node), ' ') INTO result FROM unnest(text_nodes) AS node;
        RETURN result; -- Returns concatenated and normalized text nodes
    END IF;
END
$function$;

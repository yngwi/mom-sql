-- Triggers for any write operations on the charters table
CREATE TRIGGER charters_before_insert_or_update
BEFORE INSERT OR UPDATE ON public.charters
FOR EACH ROW EXECUTE FUNCTION public.charters_on_upsert()

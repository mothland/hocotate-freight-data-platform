-- ===========================================
--  GLOBAL TRIGGERS FOR mission_id TABLES
-- ===========================================

CREATE OR REPLACE FUNCTION params.update_mission_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  -- For missions table itself → just bump updated_at directly
  IF TG_TABLE_SCHEMA = 'params' AND TG_TABLE_NAME = 'missions' THEN
    NEW.updated_at := now();
    RETURN NEW;
  END IF;

  -- For other mission-linked tables → update the mission record
  UPDATE params.missions
  SET updated_at = now()
  WHERE mission_id = COALESCE(NEW.mission_id, OLD.mission_id);

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;



DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT table_schema, table_name
        FROM information_schema.columns
        WHERE column_name = 'mission_id'
          AND table_schema IN ('params', 'LL')
    LOOP
        -- 🔹 AFTER INSERT
        EXECUTE format('
            CREATE TRIGGER trg_%I_%I_insert
            AFTER INSERT ON %I.%I
            FOR EACH ROW
            EXECUTE FUNCTION update_mission_timestamp();
        ', r.table_schema, r.table_name, r.table_schema, r.table_name);

        -- 🔹 AFTER UPDATE
        EXECUTE format('
            CREATE TRIGGER trg_%I_%I_update
            AFTER UPDATE ON %I.%I
            FOR EACH ROW
            WHEN (OLD.mission_id IS NOT DISTINCT FROM NEW.mission_id)
            EXECUTE FUNCTION update_mission_timestamp();
        ', r.table_schema, r.table_name, r.table_schema, r.table_name);

        -- 🔹 AFTER DELETE
        EXECUTE format('
            CREATE TRIGGER trg_%I_%I_delete
            AFTER DELETE ON %I.%I
            FOR EACH ROW
            EXECUTE FUNCTION update_mission_timestamp();
        ', r.table_schema, r.table_name, r.table_schema, r.table_name);
    END LOOP;
END $$;
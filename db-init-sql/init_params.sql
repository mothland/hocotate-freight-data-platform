-- ===========================================
--  PARAMS SCHEMA  —  Mission Registry
-- ===========================================

DROP SCHEMA IF EXISTS params CASCADE;
CREATE SCHEMA params;

CREATE TABLE params.missions (
  mission_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  ship_name          TEXT        NOT NULL,
  captain            TEXT        NOT NULL,
  target             TEXT        NOT NULL,
  report_freq_min    INT         NOT NULL,
  status             TEXT        DEFAULT 'RUNNING',   -- SUCCESS / FAILURE / RUNNING
  started_at         TIMESTAMP   DEFAULT now(),
  finished_at        TIMESTAMP
);

COMMENT ON TABLE  params.missions IS 'Mission registry: defines each mission and its overall lifecycle.';
COMMENT ON COLUMN params.missions.status IS 'Overall mission outcome: RUNNING, SUCCESS, or FAILURE.';
COMMENT ON COLUMN params.missions.started_at IS 'Timestamp when the mission was registered.';
COMMENT ON COLUMN params.missions.finished_at IS 'Timestamp when the mission was marked complete.';

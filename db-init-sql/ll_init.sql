-- ===========================================
--  LL SCHEMA  —  Landing Layer (Coherent)
-- ===========================================

DROP SCHEMA IF EXISTS LL CASCADE;
CREATE SCHEMA LL;

-- -------------------------------------------
--  SHIP STATE  (telemetry ticks, fully coherent)
-- -------------------------------------------
CREATE TABLE LL.ship_state (
    mission_id              BIGINT REFERENCES params.missions(mission_id) ON DELETE CASCADE,
    timestamp               TIMESTAMP,
    status                  TEXT,               -- OK / REPAIR / FAILURE
    ship_condition           TEXT,              -- GOOD / BAD / REPAIRING / UNOPERATIONAL
    normalized_health_total  NUMERIC(4,3),
    motor_health             NUMERIC(4,3),
    engine_health            NUMERIC(4,3),
    core_health              NUMERIC(4,3),
    fuel_health              NUMERIC(4,3),
    fuel_prc                 NUMERIC(5,2),
    temperature_C            NUMERIC(5,1),
    radiation_uSv            NUMERIC(8,3),
    cargo_integrity_prc      NUMERIC(5,2),
    coord_x                  NUMERIC(18,0),
    coord_y                  NUMERIC(18,0),
    coord_z                  NUMERIC(18,0),
    planet                   VARCHAR(255),
    file_checksum            CHAR(64),
    source_file              TEXT,
    loaded_at                TIMESTAMP DEFAULT now(),
    PRIMARY KEY (mission_id, timestamp)
);

COMMENT ON TABLE  LL.ship_state IS 'Per-tick telemetry and system health of each ship.';
COMMENT ON COLUMN LL.ship_state.status IS 'Operational status: OK, REPAIR, or FAILURE.';
COMMENT ON COLUMN LL.ship_state.ship_condition IS 'Derived from health; describes the current system condition.';
COMMENT ON COLUMN LL.ship_state.normalized_health_total IS 'Global normalized health score (0.0–1.0).';
COMMENT ON COLUMN LL.ship_state.motor_health IS 'Motor subsystem normalized score.';
COMMENT ON COLUMN LL.ship_state.engine_health IS 'Engine subsystem normalized score.';
COMMENT ON COLUMN LL.ship_state.core_health IS 'Power core subsystem normalized score.';
COMMENT ON COLUMN LL.ship_state.fuel_health IS 'Fuel subsystem normalized score.';

-- -------------------------------------------
--  MANIFESTS  (metadata of uploaded missions)
-- -------------------------------------------
CREATE TABLE LL.manifests (
    mission_id          BIGINT REFERENCES params.missions(mission_id) ON DELETE CASCADE,
    mission_date        DATE,
    manifest_path       TEXT,
    mission_file        TEXT,
    checksum_mission    CHAR(64),
    checksum_manifest   CHAR(64),
    upload_time         TIMESTAMP,
    status              TEXT,
    PRIMARY KEY (mission_id, mission_date)
);

COMMENT ON TABLE LL.manifests IS 'Index of mission manifests uploaded to MinIO.';

-- -------------------------------------------
--  MISSION REPORTS  (aggregated item metrics)
-- -------------------------------------------
CREATE TABLE LL.mission_reports (
    mission_id      BIGINT REFERENCES params.missions(mission_id) ON DELETE CASCADE,
    item            TEXT,
    value           NUMERIC,
    timestamp       TIMESTAMP,
    file_checksum   CHAR(64),
    source_file     TEXT,
    loaded_at       TIMESTAMP DEFAULT now()
);

COMMENT ON TABLE LL.mission_reports IS 'Mission-specific report values, extracted from manifests.';
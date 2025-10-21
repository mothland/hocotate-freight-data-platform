-- ==================================================
--  BUSINESS LAYER (BL)
--  Derived aggregates from LL + PARAMS
--  Deterministic full rebuild (fresh, consistent)
-- ==================================================

DROP SCHEMA IF EXISTS BL CASCADE;
CREATE SCHEMA BL;

-- ------------------------------------------
-- Mission Summary
-- ------------------------------------------
CREATE TABLE BL.mission_summary AS
SELECT
    m.mission_id,
    m.ship_name,
    m.captain,
    m.target,
    m.report_freq_min,
    m.status,
    m.started_at,
    m.finished_at,
    EXTRACT(EPOCH FROM (COALESCE(m.finished_at, now()) - m.started_at)) / 3600.0 AS mission_duration_hr,

    COUNT(s.timestamp)                                   AS total_ticks,
    AVG(s.normalized_health_total)::numeric              AS avg_health,
    MIN(s.fuel_prc)::numeric                             AS min_fuel_prc,
    MAX(s.temperature_C)::numeric                        AS max_temp_c,
    AVG(s.radiation_uSv)::numeric                        AS avg_radiation_usv,
    AVG(s.cargo_integrity_prc)::numeric                  AS avg_cargo_int_prc,

    (SUM(CASE WHEN s.ship_condition = 'GOOD' THEN 1 ELSE 0 END)::numeric
     / NULLIF(COUNT(*),0)) AS uptime_ratio,

    now() AS computed_at
FROM params.missions m
LEFT JOIN LL.ship_state s ON s.mission_id = m.mission_id
GROUP BY m.mission_id, m.ship_name, m.captain, m.target,
         m.report_freq_min, m.status, m.started_at, m.finished_at;

ALTER TABLE BL.mission_summary
    ADD PRIMARY KEY (mission_id),
    ADD CONSTRAINT mission_summary_fk
        FOREIGN KEY (mission_id) REFERENCES params.missions(mission_id) ON DELETE CASCADE;

COMMENT ON TABLE BL.mission_summary IS
'Aggregated operational KPIs per mission from LL.ship_state joined with params.missions.';


-- ------------------------------------------
-- Cargo Summary
-- ------------------------------------------
CREATE TABLE BL.cargo_summary AS
SELECT
    m.mission_id,
    m.ship_name,
    m.captain,
    m.target,
    COUNT(DISTINCT t.item)               AS nb_unique_items,
    SUM(t.value)::numeric                AS total_value_poko,
    AVG(t.value)::numeric                AS avg_value,
    MAX(t.value)::numeric                AS max_item_value,
    now() AS computed_at
FROM params.missions m
LEFT JOIN LL.mission_reports t ON t.mission_id = m.mission_id
GROUP BY m.mission_id, m.ship_name, m.captain, m.target;

ALTER TABLE BL.cargo_summary
    ADD PRIMARY KEY (mission_id),
    ADD CONSTRAINT cargo_summary_fk
        FOREIGN KEY (mission_id) REFERENCES params.missions(mission_id) ON DELETE CASCADE;

COMMENT ON TABLE BL.cargo_summary IS
'Aggregated mission metrics joined with mission metadata.';


-- ------------------------------------------
-- Manifest Summary
-- ------------------------------------------
CREATE TABLE BL.manifest_summary AS
SELECT
    m.mission_id,
    m.ship_name,
    m.captain,
    m.target,
    COUNT(lm.manifest_path)               AS nb_manifests,
    MAX(lm.upload_time)                   AS last_upload_time,
    MAX(lm.status)                        AS last_status,
    now() AS computed_at
FROM params.missions m
LEFT JOIN LL.manifests lm ON lm.mission_id = m.mission_id
GROUP BY m.mission_id, m.ship_name, m.captain, m.target;

ALTER TABLE BL.manifest_summary
    ADD PRIMARY KEY (mission_id),
    ADD CONSTRAINT manifest_summary_fk
        FOREIGN KEY (mission_id) REFERENCES params.missions(mission_id) ON DELETE CASCADE;

COMMENT ON TABLE BL.manifest_summary IS
'Manifest upload metadata per mission, joined with params.';


-- ------------------------------------------
-- Planet Summary (depends on mission + cargo)
-- ------------------------------------------
CREATE TABLE BL.planet_summary AS
SELECT
    m.target AS planet,
    COUNT(m.mission_id)                               AS nb_missions,
    SUM(CASE WHEN m.status='SUCCESS' THEN 1 ELSE 0 END) AS nb_success,
    SUM(CASE WHEN m.status='FAILURE' THEN 1 ELSE 0 END) AS nb_failure,
    ROUND(100.0 * SUM(CASE WHEN m.status='SUCCESS' THEN 1 ELSE 0 END)::numeric
          / NULLIF(COUNT(*),0), 1) AS success_rate_pct,
    ROUND(AVG(ms.avg_health)::numeric, 3)             AS avg_health,
    ROUND(AVG(ms.uptime_ratio * 100)::numeric, 1)     AS avg_uptime_pct,
    ROUND(AVG(cs.total_value_poko)::numeric, 0)       AS avg_value_poko,
    now() AS computed_at
FROM params.missions m
LEFT JOIN BL.mission_summary ms ON ms.mission_id = m.mission_id
LEFT JOIN BL.cargo_summary cs ON cs.mission_id = m.mission_id
GROUP BY m.target;

ALTER TABLE BL.planet_summary
    ADD PRIMARY KEY (planet);

COMMENT ON TABLE BL.planet_summary IS
'Aggregate KPIs per planet derived from missions joined with BL summaries.';
-- ==================================================
--  SERVING LAYER (SL)
--  Marts built from BL
--  Deterministic full rebuild (fresh, consistent)
-- ==================================================

DROP SCHEMA IF EXISTS SL CASCADE;
CREATE SCHEMA SL;

-- ==================================================
--  1. Fleet KPIs — global snapshot of the fleet
-- ==================================================
CREATE MATERIALIZED VIEW SL.mart_fleet_kpis AS
SELECT
    COUNT(DISTINCT ms.mission_id)                      AS nb_missions_total,
    COUNT(DISTINCT ms.ship_name)                       AS nb_ships_total,
    ROUND(AVG(ms.avg_health)::numeric, 3)              AS avg_fleet_health,
    ROUND(AVG(ms.uptime_ratio * 100)::numeric, 1)      AS avg_uptime_pct,
    ROUND(AVG(ms.min_fuel_prc)::numeric, 1)            AS avg_min_fuel_prc,
    ROUND(AVG(ms.max_temp_c)::numeric, 1)              AS avg_max_temp_c,
    ROUND(SUM(cs.total_value_poko)::numeric, 0)        AS total_value_poko,
    ROUND((SUM(cs.total_value_poko)::numeric / COUNT(DISTINCT ms.ship_name)), 2) AS avg_value_per_ship,
    now() AS computed_at
FROM BL.mission_summary ms
LEFT JOIN BL.cargo_summary cs USING (mission_id);

COMMENT ON MATERIALIZED VIEW SL.mart_fleet_kpis IS
'Fleet-wide KPIs derived from BL mission and cargo summaries.';

CREATE UNIQUE INDEX IF NOT EXISTS mart_fleet_kpis_idx
    ON SL.mart_fleet_kpis ((1));


-- ==================================================
--  2. Captain Performance — leaderboard
-- ==================================================
CREATE MATERIALIZED VIEW SL.mart_captain_performance AS
WITH ship_counts AS (
  SELECT
    ms.captain,
    ms.ship_name,
    COUNT(*) AS nb_missions
  FROM BL.mission_summary ms
  GROUP BY ms.captain, ms.ship_name
),
ship_mode AS (
  SELECT DISTINCT ON (captain)
    captain,
    ship_name AS favorite_ship,
    nb_missions AS nb_fav_ship_missions
  FROM ship_counts
  ORDER BY captain, nb_missions DESC
)
SELECT
    ms.captain,
    sm.favorite_ship,
    sm.nb_fav_ship_missions,
    COUNT(ms.mission_id)                                AS nb_missions_done,
    ROUND(AVG(ms.avg_health)::numeric, 3)               AS avg_mission_health,
    ROUND(AVG(ms.uptime_ratio * 100)::numeric, 1)       AS avg_uptime_pct,
    ROUND(AVG(cs.total_value_poko)::numeric, 0)         AS avg_value_poko,
    ROUND(SUM(cs.total_value_poko)::numeric, 0)         AS total_value_poko,
    ROUND((SUM(CASE WHEN ms.status='SUCCESS' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*),0)), 2) AS success_rate,
    now() AS computed_at
FROM BL.mission_summary ms
LEFT JOIN BL.cargo_summary cs USING (mission_id)
LEFT JOIN ship_mode sm ON sm.captain = ms.captain
GROUP BY ms.captain, sm.favorite_ship, sm.nb_fav_ship_missions
ORDER BY total_value_poko DESC;

COMMENT ON MATERIALIZED VIEW SL.mart_captain_performance IS
'Captain leaderboard with favorite ship, success rate, uptime, and economic yield.';

CREATE INDEX IF NOT EXISTS mart_captain_perf_idx
    ON SL.mart_captain_performance (total_value_poko DESC);


-- ==================================================
--  3. Planet Performance — most visited, profitable, or dangerous
-- ==================================================
CREATE MATERIALIZED VIEW SL.mart_planet_performance AS
SELECT
    ps.planet                                        AS planet_name,
    COUNT(DISTINCT ms.mission_id)                    AS nb_missions_total,
    SUM(CASE WHEN ms.status='SUCCESS' THEN 1 ELSE 0 END) AS nb_missions_success,
    SUM(CASE WHEN ms.status='FAILURE' THEN 1 ELSE 0 END) AS nb_missions_failure,
    ROUND((100.0 * SUM(CASE WHEN ms.status='SUCCESS' THEN 1 ELSE 0 END)::numeric
          / NULLIF(COUNT(*),0)), 1) AS success_rate_pct,
    ROUND(AVG(ms.avg_health)::numeric, 3)            AS avg_health,
    ROUND(AVG(ms.uptime_ratio * 100)::numeric, 1)    AS avg_uptime_pct,
    ROUND(AVG(ms.min_fuel_prc)::numeric, 1)          AS avg_min_fuel_prc,
    ROUND(AVG(ms.max_temp_c)::numeric, 1)            AS avg_max_temp_c,
    ROUND(SUM(cs.total_value_poko)::numeric, 0)      AS total_value_poko,
    ROUND((SUM(cs.total_value_poko)::numeric / COUNT(DISTINCT ms.ship_name)), 2) AS avg_value_per_ship,
    ROUND((SUM(cs.total_value_poko)::numeric / COUNT(*)), 2) AS avg_value_per_mission,
    now() AS computed_at
FROM BL.planet_summary ps
LEFT JOIN BL.mission_summary ms ON ms.target = ps.planet
LEFT JOIN BL.cargo_summary cs ON cs.mission_id = ms.mission_id
GROUP BY ps.planet
ORDER BY nb_missions_total DESC;

COMMENT ON MATERIALIZED VIEW SL.mart_planet_performance IS
'Route analytics: planet visit frequency, reliability, and profitability.';

CREATE INDEX IF NOT EXISTS mart_planet_perf_idx
    ON SL.mart_planet_performance (nb_missions_total DESC);


-- ==================================================
--  4. Ship Performance — reliability & yield per vessel
-- ==================================================
CREATE MATERIALIZED VIEW SL.mart_ship_performance AS
SELECT
    ms.ship_name,
    COUNT(ms.mission_id)                             AS nb_missions_total,
    SUM(CASE WHEN ms.status='SUCCESS' THEN 1 ELSE 0 END) AS nb_missions_success,
    SUM(CASE WHEN ms.status='FAILURE' THEN 1 ELSE 0 END) AS nb_missions_failure,
    ROUND((100.0 * SUM(CASE WHEN ms.status='SUCCESS' THEN 1 ELSE 0 END)::numeric
          / NULLIF(COUNT(*),0)), 1) AS success_rate_pct,
    ROUND(AVG(ms.avg_health)::numeric, 3)            AS avg_health,
    ROUND(AVG(ms.uptime_ratio * 100)::numeric, 1)    AS avg_uptime_pct,
    ROUND(AVG(ms.min_fuel_prc)::numeric, 1)          AS avg_min_fuel_prc,
    ROUND(AVG(ms.max_temp_c)::numeric, 1)            AS avg_max_temp_c,
    ROUND(AVG(ms.mission_duration_hr)::numeric, 2)   AS avg_duration_hr,
    ROUND(SUM(cs.total_value_poko)::numeric, 0)      AS total_value_poko,
    ROUND((SUM(cs.total_value_poko)::numeric / COUNT(*)), 2) AS avg_value_per_mission,
    MAX(ms.finished_at)                              AS last_mission_end,
    now() AS computed_at
FROM BL.mission_summary ms
LEFT JOIN BL.cargo_summary cs USING (mission_id)
GROUP BY ms.ship_name
ORDER BY total_value_poko DESC;

COMMENT ON MATERIALIZED VIEW SL.mart_ship_performance IS
'Ship-level performance and reliability metrics derived from BL summaries.';

CREATE INDEX IF NOT EXISTS mart_ship_perf_idx
    ON SL.mart_ship_performance (total_value_poko DESC);
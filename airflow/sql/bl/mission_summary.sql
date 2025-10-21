-- mission_summary.sql

INSERT INTO BL.mission_summary AS msum (
    mission_id, ship_name, captain, target,
    report_freq_min, status, started_at, finished_at,
    mission_duration_hr, total_ticks, avg_health,
    min_fuel_prc, max_temp_c, avg_radiation_usv,
    avg_cargo_int_prc, uptime_ratio, computed_at
)
SELECT
    m.mission_id, m.ship_name, m.captain, m.target,
    m.report_freq_min, m.status, m.started_at, m.finished_at,
    EXTRACT(EPOCH FROM (COALESCE(m.finished_at, now()) - m.started_at))/3600.0,
    COUNT(s.timestamp),
    AVG(s.normalized_health_total)::numeric,
    MIN(s.fuel_prc)::numeric,
    MAX(s.temperature_C)::numeric,
    AVG(s.radiation_uSv)::numeric,
    AVG(s.cargo_integrity_prc)::numeric,
    (SUM(CASE WHEN s.ship_condition='GOOD' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*),0)),
    now()
FROM params.missions m
LEFT JOIN LL.ship_state s USING (mission_id)
WHERE m.updated_at > (SELECT COALESCE(MAX(computed_at), '1970-01-01') FROM BL.mission_summary)
   OR m.finished_at IS NULL
GROUP BY m.mission_id, m.ship_name, m.captain, m.target,
         m.report_freq_min, m.status, m.started_at, m.finished_at
ON CONFLICT (mission_id)
DO UPDATE SET
    avg_health = EXCLUDED.avg_health,
    uptime_ratio = EXCLUDED.uptime_ratio,
    min_fuel_prc = EXCLUDED.min_fuel_prc,
    max_temp_c = EXCLUDED.max_temp_c,
    computed_at = now();
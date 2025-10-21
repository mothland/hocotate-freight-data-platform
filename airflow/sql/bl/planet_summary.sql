-- =======================================================
-- BL.planet_summary : incremental upsert version
-- =======================================================
INSERT INTO BL.planet_summary AS ps (
    planet, nb_missions, nb_success, nb_failure,
    success_rate_pct, avg_health, avg_uptime_pct,
    avg_value_poko, computed_at
)
SELECT
    m.target AS planet,
    COUNT(m.mission_id) AS nb_missions,
    SUM(CASE WHEN m.status='SUCCESS' THEN 1 ELSE 0 END) AS nb_success,
    SUM(CASE WHEN m.status='FAILURE' THEN 1 ELSE 0 END) AS nb_failure,
    ROUND(100.0 * SUM(CASE WHEN m.status='SUCCESS' THEN 1 ELSE 0 END)::numeric
          / NULLIF(COUNT(*),0), 1) AS success_rate_pct,
    ROUND(AVG(ms.avg_health)::numeric, 3) AS avg_health,
    ROUND(AVG(ms.uptime_ratio * 100)::numeric, 1) AS avg_uptime_pct,
    ROUND(AVG(cs.total_value_poko)::numeric, 0) AS avg_value_poko,
    now() AS computed_at
FROM params.missions m
LEFT JOIN BL.mission_summary ms ON ms.mission_id = m.mission_id
LEFT JOIN BL.cargo_summary cs ON cs.mission_id = m.mission_id
GROUP BY m.target
ON CONFLICT (planet)
DO UPDATE SET
    nb_missions = EXCLUDED.nb_missions,
    nb_success = EXCLUDED.nb_success,
    nb_failure = EXCLUDED.nb_failure,
    success_rate_pct = EXCLUDED.success_rate_pct,
    avg_health = EXCLUDED.avg_health,
    avg_uptime_pct = EXCLUDED.avg_uptime_pct,
    avg_value_poko = EXCLUDED.avg_value_poko,
    computed_at = now();
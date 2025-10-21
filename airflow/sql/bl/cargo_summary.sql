-- =======================================================
-- BL.cargo_summary : incremental upsert version
-- =======================================================
INSERT INTO BL.cargo_summary AS cs (
    mission_id, ship_name, captain, target,
    nb_unique_items, total_value_poko, avg_value,
    max_item_value, computed_at
)
SELECT
    m.mission_id,
    m.ship_name,
    m.captain,
    m.target,
    COUNT(DISTINCT t.item) AS nb_unique_items,
    SUM(t.value)::numeric AS total_value_poko,
    AVG(t.value)::numeric AS avg_value,
    MAX(t.value)::numeric AS max_item_value,
    now() AS computed_at
FROM params.missions m
LEFT JOIN LL.mission_reports t USING (mission_id)
WHERE COALESCE(
    (SELECT MAX(computed_at) FROM BL.cargo_summary), '1970-01-01'
) < COALESCE(m.finished_at, now())
GROUP BY m.mission_id, m.ship_name, m.captain, m.target
ON CONFLICT (mission_id)
DO UPDATE SET
    nb_unique_items = EXCLUDED.nb_unique_items,
    total_value_poko = EXCLUDED.total_value_poko,
    avg_value = EXCLUDED.avg_value,
    max_item_value = EXCLUDED.max_item_value,
    computed_at = now();
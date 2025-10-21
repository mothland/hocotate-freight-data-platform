-- =======================================================
-- BL.manifest_summary : incremental upsert version (fixed)
-- =======================================================
INSERT INTO BL.manifest_summary AS ms (
    mission_id, ship_name, captain, target,
    nb_manifests, last_upload_time, last_status, computed_at
)
SELECT
    m.mission_id,
    m.ship_name,
    m.captain,
    m.target,
    COUNT(lm.manifest_path) AS nb_manifests,
    MAX(lm.upload_time) AS last_upload_time,
    MAX(lm.status) AS last_status,
    now() AS computed_at
FROM params.missions m
LEFT JOIN LL.manifests lm USING (mission_id)
GROUP BY m.mission_id, m.ship_name, m.captain, m.target
HAVING
    COALESCE(MAX(lm.upload_time), '1970-01-01') >
    (SELECT COALESCE(MAX(computed_at), '1970-01-01') FROM BL.manifest_summary)
ON CONFLICT (mission_id)
DO UPDATE SET
    nb_manifests = EXCLUDED.nb_manifests,
    last_upload_time = EXCLUDED.last_upload_time,
    last_status = EXCLUDED.last_status,
    computed_at = now();
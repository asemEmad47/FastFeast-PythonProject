DELETE FROM QUARANTINE.OrphanRecords
WHERE resolved = FALSE
AND quarantined_at < DATEADD(day, -2, CURRENT_TIMESTAMP());
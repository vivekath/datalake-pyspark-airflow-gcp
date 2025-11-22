Drop table if exists `quantum-episode-345713.gold_dataset.FACT_SONGPLAYS`;
CREATE TABLE `quantum-episode-345713.gold_dataset.FACT_SONGPLAYS` as
SELECT DISTINCT
                                       l.ts as ts,
                                       l.userId as user_id,
                                       l.level as level,
                                       s.song_id as song_id,
                                       s.artist_id as artist_id,
                                       l.sessionId as session_id,
                                       l.userAgent as user_agent
                                   FROM `silver_dataset.DIM_SONGS` s
                                   JOIN `silver_dataset.DIM_LOG_DATA` l
                                       on s.title = l.song
                                       AND s.duration = l.length
                                   JOIN `silver_dataset.DIM_TIME` t
                                       ON t.ts = l.ts
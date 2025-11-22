CREATE TABLE IF NOT EXISTS `quantum-episode-345713.silver_dataset.DIM_TIME` (
    TS INT64,
    START_TIME STRING,
    WEEK INT64,
    WEEKDAY INT64,
    DAY INT64,
    HOUR INT64,
    INSERTED_DATE TIMESTAMP,
    IS_QUARANTINED BOOLEAN
    
);

TRUNCATE TABLE `quantum-episode-345713.silver_dataset.DIM_TIME`;

INSERT INTO `quantum-episode-345713.silver_dataset.DIM_TIME`
SELECT DISTINCT 
    ts,
    start_time,
    week,
    weekday,
    day,
    hour,
    CURRENT_TIMESTAMP() AS INSERTED_DATE,
    CASE 
        WHEN ts IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_quarantined
FROM `quantum-episode-345713.bronze_dataset.time`;
-----------------------------------------------------
CREATE TABLE IF NOT EXISTS `quantum-episode-345713.silver_dataset.DIM_SONGS` (
    SONG_ID STRING,
    TITLE STRING,
    ARTIST_ID STRING,
    YEAR INT64,
    DURATION FLOAT64,
    IS_QUARANTINED BOOLEAN,
    INSERTED_DATE TIMESTAMP,
    MODIFIED_DATE TIMESTAMP,
    IS_CURRENT BOOL
);
create or replace table `quantum-episode-345713.silver_dataset.quality_checks` as
SELECT DISTINCT 
    SONG_ID,
    TITLE,
    ARTIST_ID,
    YEAR,
    DURATION,
    CURRENT_TIMESTAMP() AS INSERTED_DATE,
    CURRENT_TIMESTAMP() AS MODIFIED_DATE,
    CASE 
        WHEN SONG_ID IS NULL THEN TRUE
        ELSE FALSE
    END AS is_quarantined
    FROM `quantum-episode-345713.bronze_dataset.songs`;

MERGE INTO `quantum-episode-345713.silver_dataset.DIM_SONGS` AS target
USING `quantum-episode-345713.silver_dataset.quality_checks` AS source
ON target.SONG_ID = source.SONG_ID
AND target.is_current = TRUE 

-- 1. Close out old row when something changed
WHEN MATCHED AND (
    target.SONG_ID     <> source.SONG_ID OR
    target.TITLE         <> source.TITLE OR
    target.ARTIST_ID          <> source.ARTIST_ID OR
    target.YEAR        <> source.YEAR OR
    target.DURATION               <> source.DURATION OR
    target.IS_QUARANTINED       <> source.IS_QUARANTINED
)
THEN UPDATE SET 
    target.is_current = FALSE,
    target.MODIFIED_DATE = CURRENT_TIMESTAMP()

-- 2. Insert a new row when changes are detected or no match exists
WHEN NOT MATCHED BY TARGET
THEN INSERT (
    SONG_ID,
    TITLE,
    ARTIST_ID,
    YEAR,
    DURATION,
    IS_QUARANTINED,
    inserted_date,
    modified_date,
    is_current
)
VALUES (
    source.SONG_ID,
    source.TITLE,
    source.ARTIST_ID,
    source.YEAR,
    source.DURATION,
    source.IS_QUARANTINED,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    TRUE
);

DROP TABLE IF EXISTS `quantum-episode-345713.silver_dataset.quality_checks`;
-----------------------------------------------------
CREATE TABLE IF NOT EXISTS `quantum-episode-345713.silver_dataset.DIM_ARTISTS` (
    ARTIST_ID STRING,
    ARTIST_NAME STRING,
    ARTIST_LOCATION STRING,
    ARTIST_LATITUDE FLOAT64,
    ARTIST_LONGITUDE FLOAT64,
    IS_QUARANTINED BOOLEAN,
    INSERTED_DATE TIMESTAMP,
    MODIFIED_DATE TIMESTAMP,
    IS_CURRENT BOOL
);
create or replace table `quantum-episode-345713.silver_dataset.quality_checks` as
SELECT DISTINCT 
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude,
    CURRENT_TIMESTAMP() AS INSERTED_DATE,
    CURRENT_TIMESTAMP() AS modified_DATE,
    CASE 
        WHEN artist_id IS NULL THEN TRUE
        ELSE FALSE
    END AS is_quarantined
    FROM `quantum-episode-345713.bronze_dataset.artists`;

MERGE INTO `quantum-episode-345713.silver_dataset.DIM_ARTISTS` AS target
USING `quantum-episode-345713.silver_dataset.quality_checks` AS source
ON target.artist_id = source.artist_id
AND target.is_current = TRUE 

WHEN MATCHED AND (
    target.artist_id     <> source.artist_id OR
    target.artist_name         <> source.artist_name OR
    target.artist_location          <> source.artist_location OR
    target.artist_latitude        <> source.artist_latitude OR
    target.artist_longitude               <> source.artist_longitude OR
    target.IS_QUARANTINED       <> source.IS_QUARANTINED
)
THEN UPDATE SET 
    target.is_current = FALSE,
    target.modified_DATE = CURRENT_TIMESTAMP()


WHEN NOT MATCHED BY TARGET
THEN INSERT (
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude,
    IS_QUARANTINED,
    inserted_date,
    modified_DATE,
    is_current
)
VALUES (
    source.artist_id,
    source.artist_name,
    source.artist_location,
    source.artist_latitude,
    source.artist_longitude,
    source.IS_QUARANTINED,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    TRUE
);

DROP TABLE IF EXISTS `quantum-episode-345713.silver_dataset.quality_checks`;
-----------------------------------------------------
CREATE TABLE IF NOT EXISTS `quantum-episode-345713.silver_dataset.DIM_USER` (
    USERID STRING,
    FIRSTNAME STRING,
    LASTNAME STRING,
    GENDER STRING,
    LEVEL STRING,
    IS_QUARANTINED BOOLEAN,
    INSERTED_DATE TIMESTAMP,
    MODIFIED_DATE TIMESTAMP,
    IS_CURRENT BOOL
);
create or replace table `quantum-episode-345713.silver_dataset.quality_checks` as
SELECT DISTINCT 
    userId,
    firstName,
    lastName,
    gender,
    level,
    CURRENT_TIMESTAMP() AS INSERTED_DATE,
    CURRENT_TIMESTAMP() AS modified_DATE,
    CASE 
        WHEN userId IS NULL THEN TRUE
        ELSE FALSE
    END AS is_quarantined
    FROM `quantum-episode-345713.bronze_dataset.user`;

MERGE INTO `quantum-episode-345713.silver_dataset.DIM_USER` AS target
USING `quantum-episode-345713.silver_dataset.quality_checks` AS source
ON target.userId = source.userId
AND target.is_current = TRUE 

WHEN MATCHED AND (
    target.userId     <> source.userId OR
    target.firstName         <> source.firstName OR
    target.lastName          <> source.lastName OR
    target.gender        <> source.gender OR
    target.level               <> source.level OR
    target.IS_QUARANTINED       <> source.IS_QUARANTINED
)
THEN UPDATE SET 
    target.is_current = FALSE,
    target.modified_DATE = CURRENT_TIMESTAMP()

WHEN NOT MATCHED BY TARGET
THEN INSERT (
    userId,
    firstName,
    lastName,
    gender,
    level,
    IS_QUARANTINED,
    inserted_date,
    modified_DATE,
    is_current
)
VALUES (
    source.userId,
    source.firstName,
    source.lastName,
    source.gender,
    source.level,
    source.IS_QUARANTINED,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    TRUE
);

DROP TABLE IF EXISTS `quantum-episode-345713.silver_dataset.quality_checks`;
-----------------------------------------------------
CREATE TABLE IF NOT EXISTS `quantum-episode-345713.silver_dataset.DIM_LOG_DATA` (
    Patient_Key STRING,
    artist STRING,
    auth STRING,
    firstName STRING,
    gender STRING,
    itemInSession INT64,
    lastName STRING,
    length FLOAT64,
    level STRING,
    location STRING,
    method STRING,
    page STRING,
    registration FLOAT64,
    sessionId INT64,
    song STRING,
    status INT64,
    ts INT64,
    userAgent STRING,
    userId STRING,
    start_time STRING,
    IS_QUARANTINED BOOLEAN,
    INSERTED_DATE TIMESTAMP,
    MODIFIED_DATE TIMESTAMP,
    IS_CURRENT BOOL
);
create or replace table `quantum-episode-345713.silver_dataset.quality_checks` as
SELECT DISTINCT 
    CONCAT(userId, '-', artist, '-', registration, '-', sessionId, '-', firstName, '-', lastName, '-', auth, '-', ts) AS Patient_Key,   
    artist,
    auth,
    firstName,
    gender,
    itemInSession,
    lastName,
    length,
    level,
    location,
    method,
    page,
    registration,
    sessionId,
    song,
    status,
    ts,
    userAgent,
    userId,
    start_time,
    CURRENT_TIMESTAMP() AS INSERTED_DATE,
    CURRENT_TIMESTAMP() AS modified_DATE,
    CASE 
        WHEN userId IS NULL OR artist is NULL OR status <> 200  THEN TRUE
        ELSE FALSE
    END AS is_quarantined,

    FROM `quantum-episode-345713.bronze_dataset.log_data`;

MERGE INTO `quantum-episode-345713.silver_dataset.DIM_LOG_DATA` AS target
USING `quantum-episode-345713.silver_dataset.quality_checks` AS source
ON target.Patient_Key = source.Patient_Key
AND target.is_current = TRUE 

WHEN MATCHED AND (
    target.artist     <> source.artist OR
    target.auth         <> source.auth OR
    target.firstName          <> source.firstName OR
    target.gender        <> source.gender OR
    target.itemInSession               <> source.itemInSession OR
    target.lastName       <> source.lastName OR
    target.length       <> source.length OR 
    target.level       <> source.level OR
    target.location       <> source.location OR
    target.method       <> source.method OR
    target.page       <> source.page OR
    target.registration       <> source.registration OR
    target.sessionId       <> source.sessionId OR
    target.song       <> source.song OR
    target.status       <> source.status OR
    target.ts       <> source.ts OR
    target.userAgent       <> source.userAgent OR
    target.userId       <> source.userId OR
    target.start_time       <> source.start_time OR
    target.IS_QUARANTINED       <> source.IS_QUARANTINED
)
THEN UPDATE SET 
    target.is_current = FALSE,
    target.modified_DATE = CURRENT_TIMESTAMP()

WHEN NOT MATCHED BY TARGET
THEN INSERT (
    Patient_Key,
    artist,
    auth,
    firstName,
    gender,
    itemInSession,
    lastName,
    length,
    level,
    location,
    method,
    page,
    registration,
    sessionId,
    song,
    status,
    ts,
    userAgent,
    userId,
    start_time,
    IS_QUARANTINED,
    inserted_date,
    modified_DATE,
    is_current
)
VALUES (
    source.Patient_Key,
    source.artist,
    source.auth,
    source.firstName,
    source.gender,
    source.itemInSession,
    source.lastName,    
    source.length,
    source.level,
    source.location,
    source.method,
    source.page,
    source.registration,
    source.sessionId,
    source.song,
    source.status,
    source.ts,
    source.userAgent,
    source.userId,
    source.start_time,
    source.IS_QUARANTINED,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    TRUE
);

DROP TABLE IF EXISTS `quantum-episode-345713.silver_dataset.quality_checks`;
-------------------------------------------------------
-- make user as full load
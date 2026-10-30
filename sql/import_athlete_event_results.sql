CREATE DATABASE IF NOT EXISTS olympic_dataset;
USE olympic_dataset;

CREATE TABLE IF NOT EXISTS athlete_event_results (
    edition TEXT,
    edition_id VARCHAR(20),
    country_noc VARCHAR(16),
    sport VARCHAR(255),
    event TEXT,
    result_id VARCHAR(30),
    athlete TEXT,
    athlete_id VARCHAR(30),
    pos VARCHAR(100),
    medal VARCHAR(20),
    isTeamSport VARCHAR(10)
) CHARACTER SET utf8mb4;

TRUNCATE TABLE athlete_event_results;

LOAD DATA INFILE '/var/lib/mysql-files/athlete_event_results.csv'
INTO TABLE athlete_event_results
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    edition,
    edition_id,
    country_noc,
    sport,
    event,
    result_id,
    athlete,
    athlete_id,
    pos,
    medal,
    isTeamSport
);

SELECT
    COUNT(*) AS total_records,
    SUM(medal = 'Bronze') AS bronze,
    SUM(medal = 'Silver') AS silver,
    SUM(medal = 'Gold') AS gold
FROM athlete_event_results;

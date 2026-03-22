-- Grant the airflow user access to all schemas it needs
GRANT ALL PRIVILEGES ON airflow.* TO 'airflow'@'%';
GRANT ALL PRIVILEGES ON olympic_dataset.* TO 'airflow'@'%';
GRANT ALL PRIVILEGES ON neo_data.* TO 'airflow'@'%';
FLUSH PRIVILEGES;

-- Create source schema and table used by the DAG
CREATE DATABASE IF NOT EXISTS olympic_dataset;

CREATE TABLE IF NOT EXISTS olympic_dataset.athlete_event_results (
    edition       VARCHAR(100),
    edition_id    INT,
    country_noc   VARCHAR(10),
    sport         VARCHAR(100),
    event         VARCHAR(200),
    result_id     INT,
    athlete       VARCHAR(200),
    athlete_id    INT,
    pos           VARCHAR(10),
    medal         VARCHAR(10),
    isTeamSport   BOOLEAN
);

-- Insert a representative sample so medal counts are non-zero
INSERT INTO olympic_dataset.athlete_event_results
    (edition, edition_id, country_noc, sport, event, result_id, athlete, athlete_id, pos, medal, isTeamSport)
VALUES
    ('2020 Summer Olympics', 2020, 'USA', 'Athletics', '100m Men', 1, 'Athlete A', 1, '1', 'Gold', FALSE),
    ('2020 Summer Olympics', 2020, 'GBR', 'Athletics', '100m Men', 2, 'Athlete B', 2, '2', 'Silver', FALSE),
    ('2020 Summer Olympics', 2020, 'JAM', 'Athletics', '100m Men', 3, 'Athlete C', 3, '3', 'Bronze', FALSE),
    ('2020 Summer Olympics', 2020, 'KEN', 'Athletics', '200m Men', 4, 'Athlete D', 4, '1', 'Gold', FALSE),
    ('2020 Summer Olympics', 2020, 'USA', 'Athletics', '200m Men', 5, 'Athlete E', 5, '2', 'Silver', FALSE),
    ('2020 Summer Olympics', 2020, 'NGR', 'Athletics', '200m Men', 6, 'Athlete F', 6, '3', 'Bronze', FALSE),
    ('2020 Summer Olympics', 2020, 'CHN', 'Swimming', '100m Freestyle Men', 7, 'Athlete G', 7, '1', 'Gold', FALSE),
    ('2020 Summer Olympics', 2020, 'AUS', 'Swimming', '100m Freestyle Men', 8, 'Athlete H', 8, '2', 'Silver', FALSE),
    ('2020 Summer Olympics', 2020, 'USA', 'Swimming', '100m Freestyle Men', 9, 'Athlete I', 9, '3', 'Bronze', FALSE),
    ('2020 Summer Olympics', 2020, 'FRA', 'Cycling', 'Road Race Men', 10, 'Athlete J', 10, '1', 'Gold', FALSE),
    ('2020 Summer Olympics', 2020, 'BEL', 'Cycling', 'Road Race Men', 11, 'Athlete K', 11, '2', 'Silver', FALSE),
    ('2020 Summer Olympics', 2020, 'NED', 'Cycling', 'Road Race Men', 12, 'Athlete L', 12, '3', 'Bronze', FALSE),
    ('2016 Summer Olympics', 2016, 'USA', 'Athletics', '100m Men', 13, 'Athlete M', 13, '1', 'Gold', FALSE),
    ('2016 Summer Olympics', 2016, 'JAM', 'Athletics', '100m Men', 14, 'Athlete N', 14, '2', 'Silver', FALSE),
    ('2016 Summer Olympics', 2016, 'USA', 'Athletics', '100m Men', 15, 'Athlete O', 15, '3', 'Bronze', FALSE),
    ('2016 Summer Olympics', 2016, 'ETH', 'Athletics', 'Marathon Men', 16, 'Athlete P', 16, '1', 'Gold', FALSE),
    ('2016 Summer Olympics', 2016, 'KEN', 'Athletics', 'Marathon Men', 17, 'Athlete Q', 17, '2', 'Silver', FALSE),
    ('2016 Summer Olympics', 2016, 'USA', 'Athletics', 'Marathon Men', 18, 'Athlete R', 18, '3', 'Bronze', FALSE);

-- Create the target schema for the DAG results
CREATE DATABASE IF NOT EXISTS neo_data;

CREATE TABLE IF NOT EXISTS neo_data.oleksii_shcherbak_medal_counts (
    id         INT AUTO_INCREMENT PRIMARY KEY,
    medal_type VARCHAR(10),
    count      INT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

DROP DATABASE IF EXISTS Jupiler_Pro_League_Matches;

CREATE DATABASE Jupiler_Pro_League_Matches;
USE Jupiler_Pro_League_Matches;

CREATE TABLE Teams (
    team VARCHAR(100) PRIMARY KEY,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6)
);

CREATE TABLE Wedstrijden (
    id INT IDENTITY(1,1) PRIMARY KEY,
    div VARCHAR(50) NOT NULL,
    wedstrijddatum DATE NOT NULL,
    wedstrijdtijd TIME NOT NULL,
    hometeam VARCHAR(100) NOT NULL,
    awayteam VARCHAR(100) NOT NULL,

    FOREIGN KEY (hometeam) REFERENCES Teams(team),
    FOREIGN KEY (awayteam) REFERENCES Teams(team)
);

CREATE TABLE WedstrijdStatistieken (
    wedstrijdid INT PRIMARY KEY,

    full_time_home_goals INT,
    full_time_away_goals INT,
    full_time_result CHAR(1),

    half_time_home_goals INT,
    half_time_away_goals INT,
    half_time_result CHAR(1),

    home_shots INT,
    away_shots INT,
    home_shots_on_target INT,
    away_shots_on_target INT,

    home_fouls INT,
    away_fouls INT,

    home_corners INT,
    away_corners INT,

    home_yellow_cards INT,
    away_yellow_cards INT,
    home_red_cards INT,
    away_red_cards INT,

    FOREIGN KEY (wedstrijdid) REFERENCES Wedstrijden(id)
);

CREATE TABLE WedstrijdWeer (
    wedstrijdid INT PRIMARY KEY,

    temperature_2m DECIMAL(4,1),
    precipitation DECIMAL(5,2),
    relative_humidity_2m INT,

    windspeed_10m DECIMAL(4,1),
    winddirection_10m INT,
    windgusts_10m DECIMAL(4,1),

    cloudcover INT,
    weathercode INT,
    pressure_msl DECIMAL(6,1),

    -- gemiddelde tussen 1 uur voor en na de aftrap
    temperature_avg DECIMAL(4,1),
    precipitation_sum DECIMAL(5,2),
    windspeed_avg DECIMAL(4,1),
    windgusts_max DECIMAL(4,1),

    FOREIGN KEY (wedstrijdid) REFERENCES Wedstrijden(id)
);

CREATE TABLE WedstrijdWeerVooraf (
    wedstrijdid INT PRIMARY KEY,

    temp_avg_prev48 DECIMAL(4,1),
    precip_sum_prev48 DECIMAL(6,2),
    max_windgust_prev48 DECIMAL(4,1),
    rain_hours_prev48 INT,
    heat_hours_prev48 INT,

    FOREIGN KEY (wedstrijdid) REFERENCES Wedstrijden(id)
);


CREATE INDEX idx_wedstrijden_hometeam ON Wedstrijden(hometeam);
CREATE INDEX idx_wedstrijden_awayteam ON Wedstrijden(awayteam);

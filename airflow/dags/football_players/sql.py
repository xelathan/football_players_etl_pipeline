CREATE_PRODUCTION_TABLE_IF_NEEDED = """
CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    player_id INT,
    player_name VARCHAR(255),
    player_firstname VARCHAR(255),
    player_lastname VARCHAR(255),
    player_age INT,
    player_birth_date VARCHAR(255),
    player_birth_place VARCHAR(255),
    player_birth_country VARCHAR(255),
    player_nationality VARCHAR(255),
    player_height VARCHAR(255),
    player_weight VARCHAR(255),
    player_injured BOOLEAN,
    player_photo VARCHAR(255),
    cards_red INT,
    cards_yellow INT,
    cards_yellowred INT,
    dribbles_attempts FLOAT,
    dribbles_past INT,
    dribbles_success FLOAT,
    duels_total FLOAT,
    duels_won FLOAT,
    fouls_committed FLOAT,
    fouls_drawn FLOAT,
    games_appearences INT,
    games_captain BOOLEAN,
    games_lineups INT,
    games_minutes INT,
    games_number FLOAT,
    games_position VARCHAR(255),
    games_rating VARCHAR(255),
    goals_assists FLOAT,
    goals_conceded INT,
    goals_saves INT,
    goals_total INT,
    league_country VARCHAR(255),
    league_flag VARCHAR(255),
    league_id INT,
    league_logo VARCHAR(255),
    league_name VARCHAR(255),
    league_season INT,
    passes_accuracy FLOAT,
    passes_key FLOAT,
    passes_total FLOAT,
    penalty_commited FLOAT,
    penalty_missed FLOAT,
    penalty_saved FLOAT,
    penalty_scored FLOAT,
    penalty_won FLOAT,
    shots_on FLOAT,
    shots_total FLOAT,
    substitutes_bench INT,
    substitutes_in INT,
    substitutes_out INT,
    tackles_blocks FLOAT,
    tackles_interceptions FLOAT,
    tackles_total FLOAT,
    team_id INT,
    team_logo VARCHAR(255),
    team_name VARCHAR(255),
    CONSTRAINT unique_player_team_league_season UNIQUE (player_id, team_id, league_id, league_season)
);
"""

CREATE_STAGING_TABLE = """
CREATE TABLE staging_table (
    player_id INT,
    player_name VARCHAR(255),
    player_firstname VARCHAR(255),
    player_lastname VARCHAR(255),
    player_age INT,
    player_birth_date VARCHAR(255),
    player_birth_place VARCHAR(255),
    player_birth_country VARCHAR(255),
    player_nationality VARCHAR(255),
    player_height VARCHAR(255),
    player_weight VARCHAR(255),
    player_injured BOOLEAN,
    player_photo VARCHAR(255),
    cards_red INT,
    cards_yellow INT,
    cards_yellowred INT,
    dribbles_attempts FLOAT,
    dribbles_past INT,
    dribbles_success FLOAT,
    duels_total FLOAT,
    duels_won FLOAT,
    fouls_committed FLOAT,
    fouls_drawn FLOAT,
    games_appearences INT,
    games_captain BOOLEAN,
    games_lineups INT,
    games_minutes INT,
    games_number FLOAT,
    games_position VARCHAR(255),
    games_rating VARCHAR(255),
    goals_assists FLOAT,
    goals_conceded INT,
    goals_saves INT,
    goals_total INT,
    league_country VARCHAR(255),
    league_flag VARCHAR(255),
    league_id INT,
    league_logo VARCHAR(255),
    league_name VARCHAR(255),
    league_season INT,
    passes_accuracy FLOAT,
    passes_key FLOAT,
    passes_total FLOAT,
    penalty_commited FLOAT,
    penalty_missed FLOAT,
    penalty_saved FLOAT,
    penalty_scored FLOAT,
    penalty_won FLOAT,
    shots_on FLOAT,
    shots_total FLOAT,
    substitutes_bench INT,
    substitutes_in INT,
    substitutes_out INT,
    tackles_blocks FLOAT,
    tackles_interceptions FLOAT,
    tackles_total FLOAT,
    team_id INT,
    team_logo VARCHAR(255),
    team_name VARCHAR(255)
);
"""

MERGE_STAGING_WITH_PRODUCTION = """
INSERT INTO {{ params.production_table }}
SELECT *
FROM staging_table
ON CONFLICT (player_id, team_id, league_id, league_season)
DO UPDATE SET
    player_id = EXCLUDED.player_id,
    team_id = EXCLUDED.team_id,
    league_id = EXCLUDED.league_id,
    league_season = EXCLUDED.league_season;
"""

DROP_STAGING_TABLE = """DROP TABLE IF EXISTS staging_table;"""
CREATE KEYSPACE solitaire WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE solitaire.boards (state blob PRIMARY KEY, client_id text);
CREATE TABLE solitaire.winning_boards (state blob PRIMARY KEY);
CREATE TABLE solitaire.board_rel (child blob, parent blob, PRIMARY KEY(child, parent));
CREATE TABLE solitaire.client_metrics (client_id text PRIMARY KEY, boards_processed counter);
CREATE TABLE solitaire.level_metrics (level tinyint PRIMARY KEY, boards_processed counter);

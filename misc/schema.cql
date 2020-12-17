CREATE KEYSPACE solitaire WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE solitaire.winning_boards (state blob PRIMARY KEY);
CREATE TABLE solitaire.board_rel (child blob, parent blob, PRIMARY KEY(child, parent));

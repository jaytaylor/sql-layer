CREATE TABLE t(id BIGINT NOT NULL PRIMARY KEY, viewable_id INT, viewable_type VARCHAR(255), type VARCHAR(75));
CREATE INDEX viewable_id ON t(viewable_id);
CREATE INDEX viewable_type_and_type ON t(viewable_type, type);
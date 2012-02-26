CREATE TABLE blocks (
  bid int NOT NULL,
  module varchar(64) NOT NULL DEFAULT '',
  delta varchar(32) NOT NULL DEFAULT '0',
  theme varchar(64) NOT NULL DEFAULT '',
  status int NOT NULL DEFAULT '0',
  weight int NOT NULL DEFAULT '0',
  region varchar(64) NOT NULL DEFAULT '',
  custom int NOT NULL DEFAULT '0',
  throttle int NOT NULL DEFAULT '0',
  visibility int NOT NULL DEFAULT '0',
  pages clob NOT NULL,
  title varchar(64) NOT NULL DEFAULT '',
  cache int NOT NULL DEFAULT '1',
  PRIMARY KEY (bid),
  CONSTRAINT tmd UNIQUE (theme,module,delta)
);
CREATE INDEX list ON blocks(theme,status,region,weight,module);

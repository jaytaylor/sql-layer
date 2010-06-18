-- A very primitive expression of the group table relationships
-- in the xxxxxxxx schema. The column names to the right of the
-- table name indicate the parent-child keys.
--

/*

schema toy_test baseid=1000;

group access {
  table gdm_access
};

group construct {
  table gdm_construct {
    table gdm_sentence (CONSTRUCT_ID)
    }
};

*/

CREATE TABLE `gdm_access` (
  `ACCESS_ID` int NOT NULL default '0',
  `CLIENT_IP` int NOT NULL default '0',
  `SERVER_IP` int NOT NULL default '0',
  `BOOTSTRAP` int NOT NULL DEFAULT '0',
  `WEIGHT`    int NOT NULL DEFAULT '0',
  PRIMARY KEY  (`ACCESS_ID`),
  KEY `GDM_ACCESS_I1` (`CLIENT_IP`),
  KEY `GDM_ACCESS_I2` (`SERVER_IP`)
) ENGINE=akibadb DEFAULT CHARSET=utf8;

CREATE TABLE `gdm_construct` (
  `CONSTRUCT_ID` int NOT NULL default '0',
  `ORIGINAL_SQL` int not null default '0',
  PRIMARY KEY  (`CONSTRUCT_ID`)
) ENGINE=akibadb DEFAULT CHARSET=utf8;

CREATE TABLE `gdm_sentence` (
  `SENTENCE_ID`  int NOT NULL default '0',
  `CONSTRUCT_ID` int NOT NULL default '0',
  `VERB`         int NOT NULL default '0',
  `DEPTH`        int NOT NULL default '0',
  PRIMARY KEY  (`SENTENCE_ID`, `CONSTRUCT_ID`),
  KEY `GDM_SENTENCE_I1` (`CONSTRUCT_ID`)
) ENGINE=akibadb AUTO_INCREMENT=174691 DEFAULT CHARSET=utf8;


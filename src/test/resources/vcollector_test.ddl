-- A very primitive expression of the group table relationships
-- in the xxxxxxxx schema. The column names to the right of the
-- table name indicate the parent-child keys.
--

/*
schema toy_test baseid=1000;

group access {
  table gdm_access
};

group htest {
    table great_grandparent {
        table grandparent (great_grandparent_id) {
            table parent (grandparent_id) {
                table child (parent_id)
            }
        },
        table grandparent_1 (great_grandparent_id)
    }
};

*/
CREATE TABLE `gdm_access` (
  `ACCESS_ID` int(11) NOT NULL default '0',
  `CLIENT_IP` int(11) NOT NULL default '0',
  `SERVER_IP` int(11) NOT NULL default '0',
  `a1` int(11) NOT NULL default '0',
  PRIMARY KEY  (`ACCESS_ID`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `great_grandparent` (
  `great_grandparent_id` int not null,
  PRIMARY KEY  (`great_grandparent_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `grandparent` (
  `grandparent_id` int not null,
  `great_grandparent_id` int not null,
  PRIMARY KEY  (`grandparent_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `grandparent_1` (
  `grandparent_1_id` int not null,
  `great_grandparent_id` int not null,
  PRIMARY KEY  (`grandparent_1_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `parent` (
  `parent_id` int not null,
  `grandparent_id` int not null,
  PRIMARY KEY  (`parent_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `child` (
  `child_id` int NOT NULL,
  `parent_id` int NOT NULL,
  PRIMARY KEY  (`child_id`)
) ENGINE=MyISAM AUTO_INCREMENT=174691 DEFAULT CHARSET=utf8;



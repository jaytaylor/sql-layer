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
        }
    }
};

*/
CREATE TABLE `gdm_access` (
  `ACCESS_ID` int(11) NOT NULL default '0',
  `CLIENT_IP` int(11) NOT NULL default '0',
  `SERVER_IP` int(11) NOT NULL default '0',
  `a1` int(11) NOT NULL default '0',
    `a2` int(11) NOT NULL default '0',
  `a3` int(11) NOT NULL default '0',
  `a4` int(11) NOT NULL default '0',
  `a5` int(11) NOT NULL default '0',
  `a6` int(11) NOT NULL default '0',
  `a7` int(11) NOT NULL default '0',
  `a8` int(11) NOT NULL default '0',
  `a9` int(11) NOT NULL default '0',
  `a10` int(11) NOT NULL default '0',
  `a11` int(11) NOT NULL default '0',
  `a12` int(11) NOT NULL default '0',
  `a13` int(11) NOT NULL default '0',
  `a14` int(11) NOT NULL default '0',
  `a15` int(11) NOT NULL default '0',
  `a16` int(11) NOT NULL default '0',
  `a17` int(11) NOT NULL default '0',
  `a18` int(11) NOT NULL default '0',
  `a19` int(11) NOT NULL default '0',
  `a20` int(11) NOT NULL default '0',
  `a21` int(11) NOT NULL default '0',
  `a22` int(11) NOT NULL default '0',
  `a23` int(11) NOT NULL default '0',
  `a24` int(11) NOT NULL default '0',
  `a25` int(11) NOT NULL default '0',
  `a26` int(11) NOT NULL default '0',
  `a27` int(11) NOT NULL default '0',
  `a28` int(11) NOT NULL default '0',
  `a29` int(11) NOT NULL default '0',
  `a30` int(11) NOT NULL default '0',
  `a31` int(11) NOT NULL default '0',
  `a32` int(11) NOT NULL default '0',
  `a33` int(11) NOT NULL default '0',
  `a34` int(11) NOT NULL default '0',
  `a35` int(11) NOT NULL default '0',
  `a36` int(11) NOT NULL default '0',
  `a37` int(11) NOT NULL default '0',
  `a39` int(11) NOT NULL default '0',
  `a40` int(11) NOT NULL default '0',
  `a41` int(11) NOT NULL default '0',
  `a42` int(11) NOT NULL default '0',
  `a43` int(11) NOT NULL default '0',
  `a44` int(11) NOT NULL default '0',
  `a45` int(11) NOT NULL default '0',
  `a46` int(11) NOT NULL default '0',
  `a47` int(11) NOT NULL default '0',
  `a48` int(11) NOT NULL default '0',
  `a49` int(11) NOT NULL default '0',
  `a50` int(11) NOT NULL default '0',
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



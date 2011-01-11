CREATE TABLE `batch` (
  `bid` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
  `token` varchar(64) NOT NULL,
  `timestamp` int(11) NOT NULL,
  `batch` longtext,
  PRIMARY KEY (`bid`),
  KEY `token` (`token`)
) ENGINE=akibandb AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;
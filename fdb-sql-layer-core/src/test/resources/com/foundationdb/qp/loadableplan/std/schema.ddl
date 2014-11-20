
CREATE TABLE `customers`
(
  `cid` INT NOT NULL PRIMARY KEY, 
  `name` VARCHAR(32)
);

CREATE TABLE `orders`
(
  `oid` INT NOT NULL PRIMARY KEY,
  `cid` INT NOT NULL,
  GROUPING FOREIGN KEY (`cid`) REFERENCES `customers`(`cid`),
  `order_date` DATE
);

CREATE TABLE `items`
(
  `iid` INT NOT NULL PRIMARY KEY,
  `oid` INT NOT NULL,
  GROUPING FOREIGN KEY (`oid`) REFERENCES `orders`(`oid`),
  `sku` VARCHAR(32),
  `quan` INT
);

CREATE TABLE `addresses`
(
  `aid` INT NOT NULL PRIMARY KEY,
  `cid` int NOT NULL,
  GROUPING FOREIGN KEY (`cid`) REFERENCES `customers`(`cid`),
  `state` CHAR(2),
  `city` VARCHAR(100)
);

CREATE TABLE `guid_table`
(
  `id` GUID
);

CREATE TABLE "values"
(
  `id` INT
);

CREATE TABLE `strings`
(
  `s` VARCHAR(32)
);

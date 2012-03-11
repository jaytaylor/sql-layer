CREATE TABLE customers(
    id INT NOT NULL,
    PRIMARY KEY(id),
    name VARCHAR(256) NOT NULL,
    vipstatus INT NOT NULL,
    yob INT NOT NULL,
    password VARCHAR(32) NOT NULL
);

CREATE INDEX namekey ON customers(name);
CREATE INDEX name_yob ON customers(name, yob);
CREATE INDEX name_vipstatus ON customers(name, vipstatus);
CREATE INDEX yob_password ON customers(yob, password);
CREATE INDEX yobkey ON customers(yob);
CREATE INDEX yob_vipstatus ON customers(yob, vipstatus);

CREATE TABLE orders(
    id INT NOT NULL,
    PRIMARY KEY(id),
    cid INT,
    GROUPING FOREIGN KEY(cid) REFERENCES customers(id),
    odate VARCHAR(256) NOT NULL
);

CREATE INDEX odatekey ON orders(odate);

CREATE TABLE addresses(
    id INT NOT NULL,
    PRIMARY KEY(id),
    cid INT,
    GROUPING FOREIGN KEY(cid) REFERENCES customers(id),
    street VARCHAR (32) NOT NULL
);

CREATE INDEX streetkey ON addresses(street);

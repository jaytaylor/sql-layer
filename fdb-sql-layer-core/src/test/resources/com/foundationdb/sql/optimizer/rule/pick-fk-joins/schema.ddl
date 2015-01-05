CREATE TABLE parent(id bigint NOT NULL, PRIMARY KEY(id), 
    name VARCHAR(256) NOT NULL, UNIQUE(name), 
    state CHAR(2));
CREATE TABLE child(id bigint NOT NULL, PRIMARY KEY(id), 
    pid bigint, GROUPING FOREIGN KEY(pid) REFERENCES parent(id), 
    name VARCHAR(256) NOT NULL);

Create Table zoo (
    id bigint not null primary key,
    name varchar(255)
);

create table animal (
	id bigint not null,        primary key (id),
	description varchar(255),
	body_weight float(19),
	mother_id bigint,
	father_id bigint,
	zoo_id bigint, UNIQUE (zoo_id),
	serial_number varchar(255),
	CONSTRAINT FK_MOTHER FOREIGN KEY (mother_id) REFERENCES parent,
	CONSTRAINT FK_FATHER FOREIGN KEY (father_id) REFERENCES parent,
	CONSTRAINT FK_ZOO FOREIGN KEY (zoo_id) REFERENCES zoo
);


create table Mammal (
	animal bigint not null,    primary key (animal),
	pregnant integer,
	birthdate date,
	mammalZoo_id bigint,
	name varchar(255),
	CONSTRAINT FK_ANIMAL_M FOREIGN KEY (animal) REFERENCES animal
);

create table DomesticAnimal (
	mammal bigint not null,
	owner bigint,
	primary key (mammal),
	CONSTRAINT FK_MAMMAL_D FOREIGN KEY (mammal) references mammal
);

 create table Reptile (
	animal bigint not null,    primary key (animal),
	bodyTemperature float(19),
	name varchar(255),
	CONSTRAINT FK_ANIMAL_R FOREIGN KEY (animal) REFERENCES animal
);

create table Lizard (
    reptile bigint not null,
    primary key (reptile),
    constraint FK_REPTILE foreign key (reptile) references Reptile
);

create table Cat (
    mammal bigint not null,
    primary key (mammal),
    constraint FK_DOMESTICCAT foreign key (mammal) REFERENCES domesticanimal
);

create table Dog (
	mammal bigint not null, primary key (mammal),
	FOREIGN KEY (mammal) REFERENCES domesticanimal
);

create table Human (
    mammal bigint not null,
    name_first varchar(255) character set latin1,
    name_initial char(255) character set latin1,
    name_last varchar(255) character set latin1,
    nickName varchar(255) character set latin1,
    height_centimeters double precision not null,
    intValue integer,
    floatValue float(19),
    bigDecimalValue numeric(19,2),
    bigIntegerValue numeric(19,2),
    primary key (mammal),
    constraint FK_MAMMAL foreign key (mammal) references mammal
);

create table Human_friends (
    human1 bigint not null,
    human2 bigint not null,
    constraint fk_friend1 foreign key (human1) references human,
    constraint fk_friend2 foreign key (human2) references human
);

CREATE TABLE t1 (c1 integer not null, c2 integer not null, primary key (c1, c2));

CREATE TABLE t2 (id integer not null primary key, c1 integer, c2 integer, 
    foreign key (c1, c2) references t1 (c1, c2));

CREATE TABLE t3 (id integer not null primary key);

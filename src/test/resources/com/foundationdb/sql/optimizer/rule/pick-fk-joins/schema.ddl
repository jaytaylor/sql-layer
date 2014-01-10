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
	CONSTRAINT FK_ANIMAL FOREIGN KEY (animal) REFERENCES animal
);

create table DomesticAnimal (
	mammal bigint not null,
	owner bigint,
	primary key (mammal),
	CONSTRAINT FK_MAMMAL FOREIGN KEY (mammal) references mammal
);

 create table Reptile (
	animal bigint not null,    primary key (animal),
	bodyTemperature float(19),
	name varchar(255),
	CONSTRAINT FK_ANIMAL FOREIGN KEY (animal) REFERENCES animal
);

create table Dog (
	mammal bigint not null, primary key (mammal),
	FOREIGN KEY (mammal) REFERENCES domesticanimal
)


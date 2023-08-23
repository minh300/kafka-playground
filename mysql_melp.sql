CREATE SCHEMA `melp` ;

CREATE TABLE melp.business (
	business_id varchar(255) not null PRIMARY KEY,
    name varchar(255) not null,
    address varchar(255) not null,
    city varchar(255) not null,
	state varchar(255) not null,
	postal_code varchar(255) not null,
    latitude float(32) not null,
    longitude float(32) not null,
    stars float(32) not null,
    review_count int not null,
    is_open int not null,
    categories varchar(4096) not null
);

CREATE TABLE melp.business_hour (
	business_id varchar(255) not null PRIMARY KEY,
    monday varchar(255),
    tuesday varchar(255),
    wednesday varchar(255),
	thursday varchar(255),
	friday varchar(255),
    saturday varchar(255),
    sunday varchar(255),
    FOREIGN KEY (business_id) REFERENCES business(business_id)
);

    

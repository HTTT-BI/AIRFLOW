create database stage

create database nds

create database dds

create database metadata

create database katinat_01
create database katinat_02
create database katinat_03
create database katinat_04
create database katinat_05

create table if not exists data_flow  (
  "ID" integer not null generated always as identity (increment by 1),
  "TableName" varchar(50) NULL,
  "CET" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  "LSET" TIMESTAMP WITH TIME ZONE DEFAULT NULL
)

INSERT INTO data_flow ("TableName") VALUES ('katinat_01');
INSERT INTO data_flow ("TableName") VALUES ('katinat_02');
INSERT INTO data_flow ("TableName") VALUES ('katinat_03');
INSERT INTO data_flow ("TableName") VALUES ('katinat_04');
INSERT INTO data_flow ("TableName") VALUES ('katinat_05');

-- Execute with katinat_01;02;03;04 & stage
create table if not exists katinat_customer_rainbow_drink (
  "ID" integer not null generated always as identity (increment by 1),
  "Name" varchar(50) NULL,
  "Gender" varchar(10) NULL,
  "TimeArrived"TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  "TimeAway" TIMESTAMP WITH TIME ZONE DEFAULT NULL
)


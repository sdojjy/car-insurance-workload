create table faceidentify
(
customercode varchar(32) not null,
customername varchar(80) not null,
idtype varchar(3) not null,
idcode varchar(50) not null,
identifycode text not null,
identifyversion varchar(80) not null,
sex smallint not null default 1,
createtime timestamp(6) not null default current_timestamp(6) on update current_timestamp(6),
amendtime timestamp(6) not null default current_timestamp(6) on update current_timestamp(6),
reserve1 varchar(80) not null,
reserve2 numeric(12,4)  not null,
primary key (customercode )) shard_row_id_bits=5 pre_split_regions=4;

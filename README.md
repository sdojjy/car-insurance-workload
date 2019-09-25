# car-insurance-workload

## Build

Run gradlew command to build it
```shell
#./gradlew fatJar
#tree build/libs/

build/libs/
└── car-insurance-workload-all.jar
0 directories, 1 file
```

## Insert Data
- create the table, the default database name used in application is test, you can change it by specify -s option.
```sql
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
```

- run java command to insert data 
```shell
cd build/libs/
java -Xmx12G -Xms12G -cp ./car-insurance-workload-all.jar com.pingcap.tidb.workload.insurance.InsertData   -P 3306 -p abc -i  50000 -t 100
```

- view the more command options information
```shell
java -cp ./car-insurance-workload-all.jar com.pingcap.tidb.workload.insurance.InsertData  -v
usage: workload [-b <arg>] [-c <arg>] [-d] [-h <arg>] [-i <arg>] [-P
       <arg>] [-p <arg>] [-s <arg>] [-t <arg>] [-u <arg>] [-v] [-w <arg>]
 -b,--batch <arg>      batch size per insert
 -c,--count <arg>      total insert row count
 -d,--dryRun           dry run model
 -h,--host <arg>       mysql host
 -i,--print <arg>      print a log per insert count
 -P,--port <arg>       mysql port
 -p,--password <arg>   mysql password
 -s,--database <arg>   database name
 -t,--thread <arg>     thread num
 -u,--user <arg>       mysql user
 -v,--help             print help message
 -w,--work <arg>       the start snow flake work node id, one work id per
                       thread
```
## Run workload
- build 
- run command like below
```shell
cd build/libs/
java -Xmx12G -Xms12G -cp ./car-insurance-workload-all.jar com.pingcap.tidb.workload.insurance.Workload  --thread 200 --query-percent 50 --exists-percent 90 -P 8000 -h 172.16.5.92
```
- check help message to customize workload
```shell
java -cp ./car-insurance-workload-all.jar com.pingcap.tidb.workload.insurance.Workload  -v
usage: workload [-e <arg>] [-f <arg>] [-h <arg>] [-i <arg>] [-P <arg>] [-p
       <arg>] [-q <arg>] [-s <arg>] [-t <arg>] [-u <arg>] [-v] [-w <arg>]
 -e,--exists-percent <arg>   exists percent, default value is 90
 -f,--fetch <arg>            fetch id size from db as the exists ids to
                             point get or point update
 -h,--host <arg>             mysql host
 -i,--print <arg>            print a log per insert count
 -P,--port <arg>             mysql port
 -p,--password <arg>         mysql password
 -q,--query-percent <arg>    query percent , default value is 50
 -s,--database <arg>         database name
 -t,--thread <arg>           thread num
 -u,--user <arg>             mysql user
 -v,--help                   print help message
 -w,--workload-size <arg>    query or update per thread
```
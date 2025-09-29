#!/usr/bin/env bash
# Script Name: generate_tpch_sf4.sh
# Purpose: Clone tpch-dbgen repo, compile dbgen, generate TPC-H SF4 data,
#          create a MySQL database (dropping any old one), create tables,
#          and load data into them.
# Usage: ./generate_tpch_sf4.sh

# Exit immediately on error
set -e

# 1. Clone the tpch-dbgen repository if it doesn't exist.
if [ ! -d "tpch-dbgen" ]; then
  git clone https://github.com/electrum/tpch-dbgen.git
fi

# 2. Enter the repository directory.
cd tpch-dbgen

# 3. Clean any previous builds and compile only the dbgen tool for MySQL.
make clean
make CC=gcc DATABASE=mysql dbgen

# 4. Drop the 'tpch4' database if it exists and create a fresh one.
mysql -u dbbert -pdbbert -e "DROP DATABASE IF EXISTS tpch4; CREATE DATABASE tpch4;"

# 5. Enable LOCAL INFILE on the server side (requires sufficient privileges).
mysql -u dbbert -pdbbert --local-infile=1 -e "SET GLOBAL local_infile = 1;"

# 6. Create a temporary SQL file with the TPC-H table definitions.
cat <<'EOF' > create_tables.sql
-- Create the TPC-H tables for MySQL

DROP TABLE IF EXISTS region;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS lineitem;

create table region  ( r_regionkey integer primary key,
    r_name       char(25) not null,
    r_comment    varchar(152));

create table nation  ( n_nationkey integer primary key,
    n_name       char(25) not null,
    n_regionkey  integer not null,
    n_comment    varchar(152),
    foreign key (n_regionkey) references region(r_regionkey));

create table part  ( p_partkey integer primary key,
    p_name        varchar(55) not null,
    p_mfgr        char(25) not null,
    p_brand       char(10) not null,
    p_type        varchar(25) not null,
    p_size        integer not null,
    p_container   char(10) not null,
    p_retailprice decimal(15,2) not null,
    p_comment     varchar(23) not null );

create table supplier  ( s_suppkey integer primary key,
    s_name        char(25) not null,
    s_address     varchar(40) not null,
    s_nationkey   integer not null,
    s_phone       char(15) not null,
    s_acctbal     decimal(15,2) not null,
    s_comment     varchar(101) not null,
    foreign key (s_nationkey) references nation(n_nationkey));

create table partsupp  ( ps_partkey integer not null,
    ps_suppkey     integer not null,
    ps_availqty    integer not null,
    ps_supplycost  decimal(15,2)  not null,
    ps_comment     varchar(199) not null, primary key (ps_partkey, ps_suppkey),
    foreign key (ps_suppkey) references supplier(s_suppkey));

create table customer  ( c_custkey integer primary key,
    c_name        varchar(25) not null,
    c_address     varchar(40) not null,
    c_nationkey   integer not null,
    c_phone       char(15) not null,
    c_acctbal     decimal(15,2)   not null,
    c_mktsegment  char(10) not null,
    c_comment     varchar(117) not null,
    foreign key (c_nationkey) references nation(n_nationkey));

create table orders  ( o_orderkey integer primary key,
    o_custkey        integer not null,
    o_orderstatus    char(1) not null,
    o_totalprice     decimal(15,2) not null,
    o_orderdate      date not null,
    o_orderpriority  char(15) not null,
    o_clerk          char(15) not null,
    o_shippriority   integer not null,
    o_comment        varchar(79) not null,
    foreign key (o_custkey) references customer(c_custkey));

create table lineitem ( l_orderkey integer not null,
    l_partkey     integer not null,
    l_suppkey     integer not null,
    l_linenumber  integer not null,
    l_quantity    decimal(15,2) not null,
    l_extendedprice  decimal(15,2) not null,
    l_discount    decimal(15,2) not null,
    l_tax         decimal(15,2) not null,
    l_returnflag  char(1) not null,
    l_linestatus  char(1) not null,
    l_shipdate    date not null,
    l_commitdate  date not null,
    l_receiptdate date not null,
    l_shipinstruct char(25) not null,
    l_shipmode     char(10) not null,
    l_comment      varchar(44) not null,
    primary key(l_orderkey,l_linenumber),
    foreign key (l_orderkey) references orders(o_orderkey),
    foreign key (l_partkey, l_suppkey) references partsupp(ps_partkey, ps_suppkey));

create index idx_supplier_nation_key on supplier (s_nationkey);
create index idx_partsupp_partkey on partsupp (ps_partkey);
create index idx_partsupp_suppkey on partsupp (ps_suppkey);
create index idx_customer_nationkey on customer (c_nationkey);
create index idx_orders_custkey on orders (o_custkey);
create index idx_lineitem_orderkey on lineitem (l_orderkey);
create index idx_lineitem_part_supp on lineitem (l_partkey,l_suppkey);
create index idx_nation_regionkey on nation (n_regionkey);
create index idx_lineitem_shipdate on lineitem (l_shipdate, l_discount, l_quantity);
create index idx_orders_orderdate on orders (o_orderdate);
EOF

# 7. Load the table definitions into the tpch4 database.
mysql -u dbbert -pdbbert tpch4 < create_tables.sql

# 8. Generate TPC-H data files with scale factor 4.
yes | ./dbgen -s 4

# 9. Load the generated .tbl files into their corresponding tables.
for t in region nation part supplier partsupp customer orders lineitem
do
  echo "Loading data into table: ${t}"
  mysql -u dbbert -pdbbert --local-infile=1 tpch4 \
    -e "LOAD DATA LOCAL INFILE '${t}.tbl' 
         INTO TABLE ${t}
         FIELDS TERMINATED BY '|' 
         LINES TERMINATED BY '\n';"
done

# Optionally, remove the temporary SQL file.
rm create_tables.sql

echo "TPC-H scale factor 2 data generation and loading complete!"

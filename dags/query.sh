#!/bin/bash

CSV_PATH=$HOME/data/csv/{{ds_nodash}}/csv.csv

user="root"
password="qwer123"
database="history_db"

mysql -u"$user" -p"$password" "$database" <<EOF
-- LOAD DATA INFILE '/var/lib/mysql-files/csv.csv'
use history_db;
LOAD DATA INFILE '${CSV_PATH}'
INTO TABLE history_db.tmp_cmd_usage
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';
EOF

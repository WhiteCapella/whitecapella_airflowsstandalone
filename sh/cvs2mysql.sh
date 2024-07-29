#!/bin/bash

CSV_PATH="/var/lib/mysql-files/20240718.csv"

user="root"
password="qwer123"
database="history_db"

mysql -u"$user" -p"$password" "$database" <<EOF
LOAD DATA LOCAL INFILE '$home/data/csv/20240718/csv.csv' INTO TABLE history_db.cmd_usage FIELDS TERMINATED BY ',' ENCLOSED BY '^' ESCAPED BY '\b' LINES TERMINATED BY '\n';
EOF

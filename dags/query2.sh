#!/bin/bash

CSV_PATH=$HOME/data/csv/{{ds_nodash}}/csv.csv

user="root"
password="qwer123"
database="history_db"

mysql -u"$user" -p"$password" "$database" <<EOF
insert into history_db.cmd_usage
from tmp_cmd_usage
EOF

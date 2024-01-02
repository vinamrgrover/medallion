#!/bin/bash

source .env
export PGPASSWORD="$DB_PASSWORD"

for file in `ls data/*.txt`; do
	table_name=`echo $file | sed 's/.txt//g' | sed 's/data\///g'`;
    
	psql -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -U "$DB_USER" << EOF
		\copy $table_name FROM $file DELIMITER '|' CSV HEADER;

EOF
done


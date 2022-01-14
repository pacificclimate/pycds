#!/bin/bash
owner=tester
database=pycds_test
schema1=crmp
schema2=other

here=${BASH_SOURCE%/*}
actions=$here/../actions

rm -rf "$actions"
mkdir "$actions"

"$here"/create_role.sh $owner > "$actions"/01_create_roles.sql
"$here"/create_role.sh pcicdba >> "$actions"/01_create_roles.sql
"$here"/create_db.sh $database $owner > "$actions"/02_create_db.sql
"$here"/create_schema.sh $database $owner $schema1 > "$actions"/03_create_schemas.sql
"$here"/create_schema.sh $database $owner $schema2 >> "$actions"/03_create_schemas.sql

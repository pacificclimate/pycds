#!/bin/bash
# Argument: Db connection string
connection_string=$1; shift

for item in networks stations variables; do
  psql "${connection_string}" -f "${item}-query.sql" > "${item}-post.out"
  diff "${item}-pre.out" "${item}-post.out" > "${item}-diff.out"
done
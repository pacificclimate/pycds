files=$(cat "$1")

CRMP_PATH=~/env_4.5.0/bin:~/bin:/usr/local/bin:/usr/bin:/bin
CRMP_DB=postgresql://crmprtd@db.pcic.uvic.ca:5433/crmp
CRMP_DB_PRIV=postgresql://crmp@db.pcic.uvic.ca:5433/crmp
CRMP_TAG=crmp_hx_performance
LD_LIBRARY_PATH=/modules/openssl/1.1.1w/lib

echo "cat " $files " | LD_LIBRARY_PATH=$LD_LIBRARY_PATH PATH=$CRMP_PATH crmprtd_process -N ec -c postgresql://dbtest02.pcic.uvic.ca:5432/crmp_hx -L ~/logging.yaml --log_filename ~/ec/logs/crmp_dbtest02_bc_hourly_json.log" | batch


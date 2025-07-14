#!/bin/bash
if (( $# < 1 )); then
  echo "Usage: $0 connection_string [num_thousand_inserts [obs_time [vars_id [history_id [datum]]]]]"
  exit 0
fi

# Argument: Db connection string
connection_string=$1; shift

# Argument: number of thousands of records to be inserted
num_thousand_inserts=${1:-4}; shift

# Arguments: values to be inserted in database
obs_time=${1:-"2100-01-01 00:00"}; shift
vars_id=${1:-"1200"}; shift
history_id=${1:-"2828"}; shift
datum=${1:-"99"}; shift

echo "Creating test data with ${num_thousand_inserts} thousand records"
echo "Test data values: obs_time = ${obs_time}, vars_id = ${vars_id}, history_id = ${history_id}, datum = ${datum}"
read -p "Continue? [Yy|Nn] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    echo "Exiting"
    exit 1
fi

date_str=$(date +'%Y-%m-%d-%H-%M-%S')

psql="insert-performance-test-${date_str}.psql"

echo "\timing on" > ${psql}

# Construct insert statement
echo "INSERT INTO obs_raw(obs_time, vars_id, history_id, datum) VALUES" >> ${psql}

for ((s=1; s<=num_thousand_inserts; s++)); do
  ss=$(printf "%02d" ${s})
  for ms in {000..999}; do
    val="('${obs_time}:${ss}.${ms}', ${vars_id}, ${history_id}, ${datum})";
    echo "${val}," >> ${psql};
    echo "${val}," >> ${psql};
  done;
done

echo "${val}" >> ${psql};
echo "ON CONFLICT DO NOTHING"  >> ${psql}
echo ";"  >> ${psql}

# Remove the test records and all history records associated with them
echo "SELECT count(*) AS obs_raw_count_before FROM obs_raw WHERE obs_time >= '${obs_time}';" >> ${psql}
echo "SELECT count(*) AS obs_raw_hx_count_before FROM obs_raw_hx WHERE obs_time >= '${obs_time}';" >> ${psql}
echo "DELETE FROM obs_raw WHERE obs_time >= '${obs_time}';" >> ${psql}
echo "DELETE FROM obs_raw_hx WHERE obs_time >= '${obs_time}';" >> ${psql}
echo "SELECT count(*) AS obs_raw_count_after FROM obs_raw WHERE obs_time >= '${obs_time}';" >> ${psql}
echo "SELECT count(*) AS obs_raw_hx_count_after FROM obs_raw_hx WHERE obs_time >= '${obs_time}';" >> ${psql}

echo
echo "-- Statements to be executed --"
head -n 10 ${psql}
echo "..."
tail -n 10 ${psql}
echo "--------------------------------"
echo

read -p "Execute? [Yy|Nn] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    echo "Exiting"
    exit 1
fi

# Execute the file and store the results
out="insert-performance-test-${date_str}.out"
echo "Results will be logged in ${out}"
psql "${connection_string}" -a -f "${psql}" > ${out}
echo "Done."
echo "Tail of log file follows"
tail -n 50 ${out}

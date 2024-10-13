#!/bin/bash
sage-exec /config.yaml http://example.org/wdbench -f /query_469.sparql --timeout 60000000 &> /wdbench-multiple-tps/1-cpus/60000000/run_3/query_469.dat

# Extract values from the last line of docker_result
last_line=$(tail -n 1 /wdbench-multiple-tps/1-cpus/60000000/run_3/query_469.dat)
nb_quantum=$(echo "$last_line" | awk -F'[,:]' '{gsub(/[^0-9.]/,"",$2); print $2}')
nb_results=$(echo "$last_line" | awk -F'[,:]' '{gsub(/[^0-9.]/,"",$4); print $4}')
next_size=$(echo "$last_line" | awk -F'[,:]' '{gsub(/[^0-9.]/,"",$6); print $6}')
execution_time=$(echo "$last_line" | awk -F'[,:]' '{gsub(/[^0-9.]/,"",$8); print $8}')

# Write nb_results to nb_results file
echo "$nb_results" > /wdbench-multiple-tps/1-cpus/60000000/run_3/query_469.nb.dat

# Write nb_quantum, next_size, and execution time to time file
echo "nb_quantum: $nb_quantum" > /wdbench-multiple-tps/1-cpus/60000000/run_3/query_469.time.dat
echo "next_size: $next_size kb" >> /wdbench-multiple-tps/1-cpus/60000000/run_3/query_469.time.dat
echo "execution time: $execution_time sec" >> /wdbench-multiple-tps/1-cpus/60000000/run_3/query_469.time.dat

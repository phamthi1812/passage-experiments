#!/bin/bash
cd /app/apache-jena-5.1.0/bin
./tdb2.tdbquery --loc=/tdb2-wdbench --file=/query_433.sparql --time &> /wdbench-opts/1-cpus/run_2/query_433.dat
nb_lines=$(($(wc -l < /wdbench-opts/1-cpus/run_2/query_433.dat) - 4))
if [ $nb_lines -lt 0 ]; then nb_lines=0; fi
echo $nb_lines > /wdbench-opts/1-cpus/run_2/query_433.nb.dat
tail -n 1 /wdbench-opts/1-cpus/run_2/query_433.dat | grep "Time:" | awk '{print $2}' >> /wdbench-opts/1-cpus/run_2/query_433.time.dat


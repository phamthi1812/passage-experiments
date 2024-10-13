#!/bin/bash
cd /app/apache-jena-5.1.0/bin
export JAVA_OPTIONS="-Xmx52G"
timeout 600s ./tdb2.tdbquery --loc=/tdb2-wdbench --file=/query_469.sparql --time &> /wdbench-multiple-tps/1-cpus/run_1/query_469.dat
exit_status=$?
if [ $exit_status -eq 124 ]; then
    echo "Timeout" > /wdbench-multiple-tps/1-cpus/run_1/query_469.dat
    echo 0 > /wdbench-multiple-tps/1-cpus/run_1/query_469.nb.dat
    echo 0 > /wdbench-multiple-tps/1-cpus/run_1/query_469.time.dat
else
    nb_lines=$(($(wc -l < /wdbench-multiple-tps/1-cpus/run_1/query_469.dat) - 4))
    if [ $nb_lines -lt 0 ]; then nb_lines=0; fi
    echo $nb_lines > /wdbench-multiple-tps/1-cpus/run_1/query_469.nb.dat
    tail -n 1 /wdbench-multiple-tps/1-cpus/run_1/query_469.dat | grep "Time:" | awk '{print $2}' >> /wdbench-multiple-tps/1-cpus/run_1/query_469.time.dat
fi

#!/bin/bash
java -jar -Xmx54G /app/blazegraph-cli.jar --database=/blazegraph.jnl --file=/query_679.sparql &> /wdbench-multiple-tps/1-cpus/run_2/query_679.dat
if grep -q "Query timed out after" /wdbench-multiple-tps/1-cpus/run_2/query_679.dat; then
    timeout_value=$(grep -oP 'Query timed out after \K[0-9]+' /wdbench-multiple-tps/1-cpus/run_2/query_679.dat)
    echo 0 > /wdbench-multiple-tps/1-cpus/run_2/query_679.nb.dat
    echo $timeout_value > /wdbench-multiple-tps/1-cpus/run_2/query_679.time.dat
else
    nb_lines=$(($(wc -l < /wdbench-multiple-tps/1-cpus/run_2/query_679.dat) - 2))
    if [ $nb_lines -lt 0 ]; then nb_lines=0; fi
    echo $nb_lines > /wdbench-multiple-tps/1-cpus/run_2/query_679.nb.dat
    tail -n 1 /wdbench-multiple-tps/1-cpus/run_2/query_679.dat | awk '{print $2}' >> /wdbench-multiple-tps/1-cpus/run_2/query_679.time.dat
fi

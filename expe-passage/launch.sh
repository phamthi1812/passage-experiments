#!/bin/bash
java -jar -Xmx52G /app/passage.jar --database=/blazegraph.jnl --file=/query_52.sparql --timeout=60000 --loop=True --report &> /wdbench-opts/4-cpus/60000/run_2/query_52.dat
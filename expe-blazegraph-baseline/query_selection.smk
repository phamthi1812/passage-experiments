import os
from pathlib import Path
EXEC_DIR = "executables"
DATA_DIR = "datasets"
QUERIES = "queries"
EXPE_BLAZE = "expe-blazegraph-baseline"  # Current folder for this experiment

# Rule to create a Docker image hosting the embedded Blazegraph JAR with dependencies
## TODO: let user do it in README
rule create_blazegraph_embedded_image:
    output:
        out = f"{EXPE_BLAZE}/docker/blazegraph-embedded-latest.tar"
    input:
        jar = f"{EXEC_DIR}/embedded-blazegraph-jar-with-dependencies.jar",
        dockerfile = f"{EXPE_BLAZE}/Dockerfile"
    shell:
        """
        mkdir -p {EXPE_BLAZE}/docker/ &&
        docker build -t blazegraph-embedded:latest -f {input.dockerfile} . &&
        docker save blazegraph-embedded:latest > {output.out}
        """

rule run_blazegraph_embedded_in_docker:
    input:
        data = f"{DATA_DIR}/wdbench-blaze/blazegraph.jnl",
        img = f"{EXPE_BLAZE}/docker/blazegraph-embedded-latest.tar",
        query = f"{QUERIES}/{{query}}.sparql"
    output:
        results = temp(f"{EXPE_BLAZE}/results/{{query}}.dat"),
        nb_results = f"{EXPE_BLAZE}/results/{{query}}.nb.dat",
        time = f"{EXPE_BLAZE}/results/{{query}}.time.dat"
    run:
        query_name = Path(input.query).stem
        subdir = Path(output.results).parent.name
        docker_query_file = f"/{Path(input.query).name}"
        docker_result = f"/results/{subdir}/{query_name}.dat"
        docker_nb_results = f"/results/{subdir}/{query_name}.nb.dat"
        docker_time = f"/results/{subdir}/{query_name}.time.dat"
        launch_file = Path(f"{EXPE_BLAZE}/launch.sh").resolve()
        with open(launch_file, "w") as launch:
            launch.write(f"""#!/bin/bash
java -jar -Xmx54G blazegraph.jar --database=/blazegraph.jnl --file={docker_query_file} --timeout=300000 > {docker_result}
if grep -q "Query timed out after" {docker_result}; then
    timeout_value=$(grep -oP 'Query timed out after \\K[0-9]+' {docker_result})
    echo 0 > {docker_nb_results}
    echo $timeout_value > {docker_time}
else
    nb_lines=$(($(wc -l < {docker_result}) - 2))
    if [ $nb_lines -lt 0 ]; then nb_lines=0; fi
    echo $nb_lines > {docker_nb_results}
    tail -n 1 {docker_result} | awk '{{print $2}}' >> {docker_time}
fi""")
        shell(f"""mkdir -p ./{EXPE_BLAZE}/results/{subdir} && docker load < {input.img} && docker run --rm  --cpuset-cpus='0' --memory="54G" -v ./{input.data}:/blazegraph.jnl -v $PWD/{input.query}:{docker_query_file} -v ./{EXPE_BLAZE}/results/{subdir}:/results/{subdir} -v {launch_file}:/app/launch.sh --name blazegraph-embedded blazegraph-embedded:latest /bin/bash -c "chmod a+x launch.sh && ./launch.sh" """)

#TODO: Decide how many vCPU for running the docker?
# Rule to get the output for each query and process the actual value
rule join_data_for_query_over_blazegraph_embedded:
    input:
        nb_results = f"{EXPE_BLAZE}/results/{{query}}.nb.dat",
        time = f"{EXPE_BLAZE}/results/{{query}}.time.dat"
    output:
        out = f"{EXPE_BLAZE}/results/{{query}}.csv"
    run:
        with open(input.nb_results, "r") as nb_results_file:
            nb_results = int(nb_results_file.read().strip())

        with open(input.time, "r") as time_file:
            time_as_string = time_file.read()

        with open(output.out, "w") as output_file:
            output_file.write("nb_results execution_time(ms)\n")
            output_file.write(f"{nb_results} {time_as_string}\n")

rule all_queries_and_collect_final_csv_embedded:
    params:
        subdir = "wdbench-opts"  # wdbench-multiple-tps
    input:
        expand(f"{EXPE_BLAZE}/results/{{subdir}}/{{query}}.csv",
               subdir=params.subdir,
               query=glob_wildcards(f"{QUERIES}/{{subdir}}/{{query}}.sparql").query)
    output:
        final_csv = f"{EXPE_BLAZE}/final_{{params.subdir}}.csv"
    run:
        csv_files = expand(f"{EXPE_BLAZE}/results/{{subdir}}/{{query}}.csv",
                           subdir=params.subdir,
                           query=glob_wildcards(f"{QUERIES}/{{subdir}}/{{query}}.sparql").query)

        with open(output.final_csv, "w") as final_output:
            final_output.write("query_name nb_results execution_time(ms)\n")
            for csv_file in csv_files:
                if os.path.exists(csv_file):
                    with open(csv_file, "r") as query_csv:
                        lines = query_csv.readlines()
                        if len(lines) > 1:
                            data_line = lines[1].strip()  # Get data from the second line
                            query_name = os.path.basename(csv_file).replace(".csv", "")
                            final_output.write(f"{query_name} {data_line}\n")

        print(f"Final CSV has been collected at {output.final_csv}")
import os
import subprocess
import re
import csv

# Define the list of queries
queries = [
    "query_633", "query_351", "query_547",
    "query_669", "query_272", "query_138", "query_629", "query_518", "query_545", "query_520",
    "query_349", "query_613", "query_255", "query_635", "query_158", "query_168", "query_155",
    "query_638", "query_273", "query_284", "query_604", "query_546", "query_595", "query_614",
    "query_605", "query_600", "query_637", "query_256", "query_407", "query_659", "query_632",
    "query_548", "query_149", "query_147", "query_137"
]

# Path to the Blazegraph jar file and the database
blazegraph_jar = "target/embedded-blazegraph-jar-with-dependencies.jar"
database_path = "/GDD/Thi/wdbench-blaze/blazegraph.jnl"
queries_dir = "queries/wdbench-multiple-tps"
output_csv = "query_results.csv"

def extract_info_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()

            # Get the last line for execution time
            last_line = lines[-1]
            execution_time = re.search(r"Took (\d+) milisecond", last_line)
            execution_time = execution_time.group(1) if execution_time else "heap"

            # Count the number of lines for the number of results
            nb_of_results = len(lines)

            return nb_of_results, execution_time
    except Exception as e:
        return 0, "heap"

# Write results to a CSV
with open(output_csv, 'w', newline='') as csvfile:
    fieldnames = ['query_name', 'nb_of_results', 'execution_time']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    # Write header
    writer.writeheader()

    # Iterate through each query
    for query in queries:
        query_file = os.path.join(queries_dir, f"{query}.sparql")
        output_file = f"test_{query.split('_')[1]}.dat"

        # Run the Blazegraph command
        command = [
            "java", "-jar", "-Xmx54G", blazegraph_jar,
            "--database", database_path,
            "--file", query_file
        ]

        try:
            with open(output_file, 'w') as out:
                result = subprocess.run(command, stdout=out, stderr=subprocess.PIPE, timeout=600)

            # Check if the command was killed or ran into memory issues
            if result.returncode != 0 or 'OutOfMemoryError' in result.stderr.decode():
                nb_of_results, execution_time = 0, "heap"
            else:
                # Extract information from the result file
                nb_of_results, execution_time = extract_info_from_file(output_file)

        except subprocess.TimeoutExpired:
            # Handle case where the process is killed or times out
            nb_of_results, execution_time = 0, "heap"

        # Write the result to CSV
        writer.writerow({
            'query_name': query,
            'nb_of_results': nb_of_results,
            'execution_time': execution_time
        })
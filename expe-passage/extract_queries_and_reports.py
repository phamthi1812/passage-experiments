import os
import re
import csv
import sys
from pathlib import Path

def extract_queries_and_reports(dat_file, query, report_csv, parent_dir, result_subdir):
    query_file_prefix = Path(f"{parent_dir}/expe-passage/{result_subdir}/clean_queries/{query}_")

    Path(query_file_prefix).parent.mkdir(parents=True, exist_ok=True)
    Path(report_csv).parent.mkdir(parents=True, exist_ok=True)
    with open(dat_file, 'r') as f:
        content = f.read()

    query_pattern = re.compile(
            r'To\ continue\ query\ execution,\ use\ the\ following\ query:\x1b\[0m[\s\S]*?\x1b\[0m')
    #query_pattern = re.compile(r'To\ continue\ query\ execution,\ use\ the\ following\ query:\s*(\{[\s\S]*?\})\s*(?=To\ continue|$)')

    block_pattern = re.compile(
        r'\x1b\[1;35mNumber of pause/resume: \x1b\[0m (\d+)\n'
        r'\x1b\[1;35mExecution time: \x1b\[0m (\d+) ms\n'
        r'\x1b\[1;35mNumber of results: \x1b\[0m (\d+)'
    )
    query_matches = query_pattern.findall(content)
    print(f"Found {len(query_matches)} queries")
    for i, query in enumerate(query_matches, 1):
        query_file = f"{query_file_prefix}{i}.sparql"
        clean_query_pattern = re.compile(r'\x1b\[32m(SELECT.*?)(\x1b\[0m|$)', re.DOTALL)
        query = clean_query_pattern.search(query)
        if query:
            query = query.group(1)
            with open(query_file, 'w') as qf:
                qf.write(query + "\n")
        else:
            query = "Query not found"

    with open(report_csv, 'w') as outfile:
        matches = block_pattern.findall(content)
        for match in matches:
            pause_resume, exec_time, num_results = match
            outfile.write(f"Number of pause/resume: {pause_resume}\n")
            outfile.write(f"Execution time: {exec_time} ms\n")
            outfile.write(f"Number of results: {num_results}\n")
            outfile.write("\n")

        # Handle the final block (total)
        total_block_pattern = re.compile(
            r'\x1b\[1;35mTOTAL number of pause/resume: \x1b\[0m (\d+)\n'
            r'\x1b\[1;35mTOTAL execution time: \x1b\[0m (\d+) ms\n'
            r'\x1b\[1;35mTOTAL number of results: \x1b\[0m (\d+)'
        )
        total_match = total_block_pattern.search(content)

        if total_match:
            total_pause_resume, total_exec_time, total_num_results = total_match.groups()
            outfile.write(f"TOTAL number of pause/resume: {total_pause_resume}\n")
            outfile.write(f"TOTAL execution time: {total_exec_time} ms\n")
            outfile.write(f"TOTAL number of results: {total_num_results}\n")


if __name__ == "__main__":
    dat_file = sys.argv[1]
    query = sys.argv[2]
    report_csv = sys.argv[3]
    parent_dir = sys.argv[4]
    result_subdir = sys.argv[5]

    extract_queries_and_reports(dat_file, query, report_csv, parent_dir, result_subdir)

import os
import csv

def calculate_file_size(file_path):
    """Calculate the size of a file in KB."""
    return os.path.getsize(file_path) / 1024  # Convert to KB

def process_queries(original_query, continuation_queries):
    original_size = calculate_file_size(original_query)
    continuation_queries_sizes = [calculate_file_size(query) for query in continuation_queries]
    sum_continuation_size = sum(continuation_queries_sizes)
    nb_continuation_queries = len(continuation_queries)
    individual_ratios = [cq_size / original_size if original_size != 0 else 0 for cq_size in continuation_queries_sizes]
    sum_ratio = sum_continuation_size / original_size if original_size != 0 else 0

    return {
        "original_query_size": original_size,
        "nb_continuation_queries": nb_continuation_queries,
        "sum_continuation_size": sum_continuation_size,
        "individual_ratios": individual_ratios,
        "sum_ratio": sum_ratio,
        "continuation_queries_sizes": continuation_queries_sizes
    }

def process_query_dir(query_dir, continuation_queries_dir):
    """Process all queries in a directory and generate a report."""
    report = []

    # Get all original queries
    original_queries = {
        os.path.splitext(filename)[0]: os.path.join(query_dir, filename)
        for filename in os.listdir(query_dir)
        if os.path.isfile(os.path.join(query_dir, filename))
    }

    # Map continuation queries to their original queries
    continuation_queries = {}
    for filename in os.listdir(continuation_queries_dir):
        file_path = os.path.join(continuation_queries_dir, filename)
        if os.path.isfile(file_path):
            base_name, ext = os.path.splitext(filename)
            # Extract the original query base name
            # Assuming continuation files are named like 'query_1_1.sparql', 'query_1_2.sparql', etc.
            # The original query base name is everything before the last underscore
            if '_' in base_name:
                original_base_name = '_'.join(base_name.split('_')[:-1])
                continuation_queries.setdefault(original_base_name, []).append(file_path)
            else:
                # If there is no underscore, it's not a continuation query
                continue

    # Process each original query with its continuation queries
    for base_name, original_query_path in original_queries.items():
        cqs = continuation_queries.get(base_name, [])
        result = process_queries(original_query_path, cqs)
        report.append({
            'query_name': base_name,
            'original_query': original_query_path,
            'continuation_queries': cqs,
            'result': result
        })

    return report

def generate_csv_report(report_data, output_file='report.csv'):
    """Generate a CSV report from the processed data."""
    fieldnames = [
        'query_name',
        'original_query_size',
        'nb_continuation_queries',
        'sum_continuation_queries_size',
        'sum_ratio'
    ]

    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for entry in report_data:
            result = entry['result']
            writer.writerow({
                'query_name': entry['query_name'],
                'original_query_size': f"{result['original_query_size']:.2f}",
                'nb_continuation_queries': result['nb_continuation_queries'],
                'sum_continuation_queries_size': f"{result['sum_continuation_size']:.2f}",
                'sum_ratio': f"{result['sum_ratio']:.2f}"
            })

    print(f"CSV report generated: {output_file}")
# Example usage:
query_directory = '/GDD/Thi/passage-experiments/selected_queries/wdbench-opts'
continuation_queries_directory = '/GDD/Thi/passage-experiments/expe-passage/wdbench-opts/1-cpus/60000/run_1/clean_queries'
report = process_query_dir(query_directory, continuation_queries_directory)
generate_csv_report(report, output_file='report-wdbench-opt-1-r1.csv')

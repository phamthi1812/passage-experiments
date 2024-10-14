import os
import glob
from pathlib import Path

engine = config.get("engine", "blazegraph-baseline")  # Choose engine: 'blazegraph-baseline', 'jena', 'sage', 'passage'
PARENT_DIR = config.get("parent_dir", "/GDD/Thi/sage-jena-benchmarks")
cpus = config.get("cpus", 1)
query_dirs = config.get("query_dirs", ["wdbench-multiple-tps", "wdbench-opts"])
run_ids = config.get("run_ids", ["run_1", "run_2", "run_3"])
timeouts = config.get("timeouts", [60000, 60000000])

def get_queries(query_dir):
    return [Path(f).stem for f in glob.glob(f"{PARENT_DIR}/selected_queries/{query_dir}/*.sparql")]

def include_engine_snakefile(engine):
    if engine == "blazegraph":
        include: f"{PARENT_DIR}/expe-blazegraph-baseline/Snakefile"
    elif engine == "jena":
        include: f"{PARENT_DIR}/expe-jena/Snakefile"
    elif engine == "sage":
        include: f"{PARENT_DIR}/expe-sage/Snakefile"
    elif engine == "passage":
        include: f"{PARENT_DIR}/expe-passage/Snakefile"
    else:
        raise ValueError(f"Unknown engine: {engine}")


include_engine_snakefile(engine)

rule all:
    input:
        expand(f"{PARENT_DIR}/expe-{engine}/{{query_dir}}/report-{{query_dir}}-{{cpus}}-cpus.csv",
               query_dir=query_dirs, cpus=[cpus])

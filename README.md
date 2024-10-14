# passage-experiments
This repository contains the experiments in the paper "passage: Ensuring Completeness and Responsiveness of PublicSPARQL Endpoints with SPARQL Continuation Queries"

## Organization the repository
- `./datasets`: Contains the WDBench used in the experiments under different formats (TDB2, HDT, and JNL). 
As the dataset is large, we provide a script to download the dataset in jnl. About the TDB2 and HDT formats, you can download and ingest them by yourself.
- `./selected_queries`: Contains the queries used in the experiments, which are a subset of WDBench benchmark that do not contain cartesian products. 
It contains 2 subdirectories: `./selected_queries/wdbench-mulitple-tps` and `./selected_queries/wdbench-opts`. 
The first contains queries that have multiple triple patterns, and the second contains queries that have optional patterns.
Those queries were taken from 1 to 5 minutes for execution ni Blazegraph under single virtual CPU.
- `./passage`: Contains the source code of the PASSAGE implementation. Please refer to the README.md file in this directory for more information.
- `./passage-communica`: PASSAGE x Communica. The extension of Communica smart client for supporting all PASSAGE.
Please also refer to the README.md file in this directory for more information.
- `./expe-blazegraph-baseline`, `./expe-jena`, `./expe-sage`,`./expe-passage`: Contains the scripts used to run the experiments for 
comparing the performance of Blazegraph, Jena, Sage, and PASSAGE. 
- Snakemake file: Contains the workflow for running the experiments.
- `./blazegraph-cli`: For producing blazegraph CLI commands in Blazegraph experiments. You can find the README.md file in this directory for more information.


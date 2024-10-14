# passage-experiments
This repository contains the experiments in the paper "PASSAGE: Ensuring Completeness and Responsiveness of PublicSPARQL Endpoints with SPARQL Continuation Queries"

## Abstract
Being able to query online public knowledge graphs such as Wiki-data or DBPedia is extremely valuable. 
However, these queries can be interrupted due to the fair use policies enforced by SPARQL endpoint providers, 
leading to incomplete results. While these policies help maintain the responsiveness of public SPARQL endpoints,
they compromise the completeness of query results, limiting the feasibility of various downstream tasks.
Ideally, we should not have to choose between completeness and responsiveness. To address this issue, 
we introduce the concept of SPARQL continuation queries.When a SPARQL endpoint interrupts a query, 
it returns partial results along with a SPARQL continuation query to retrieve the remaining results. 
If the continuation query is also interrupted, the process repeats, generating further continuation 
queries until the complete results are obtained. In our experimentation, we show that our continuation
server passage ensures completeness and responsiveness with a high level of performance.

## Organization the repository
- `./datasets`: You can download from the [git repo](https://github.com/MillenniumDB/WDBench) for different formats of the dataset.
We'll need JNL for Blazegraph and PASSAGE experiments, TDB2 for Jena experiments, and HDT for Sage experiments.
- `./selected_queries`: Contains the queries used in the experiments, which are a subset of WDBench benchmark that do not contain cartesian products. 
It contains 2 subdirectories: `./selected_queries/wdbench-mulitple-tps` and `./selected_queries/wdbench-opts`. 
The first contains queries that have multiple triple patterns, and the second contains queries that have optional patterns.
Those queries were taken from 1 to 5 minutes for execution ni Blazegraph under single virtual CPU.
- `./passage`: Contains the source code of the PASSAGE implementation. Please refer to the README.md file in this directory for more information.
- `./passage-communica`: PASSAGE x Communica. The extension of Communica smart client for supporting all SPARQL.
Please also refer to the README.md file in this directory for more information.
- `./expe-blazegraph-baseline`, `./expe-jena`, `./expe-sage`,`./expe-passage`: Contains the scripts used to run the experiments for 
comparing the performance of Blazegraph, Jena, Sage, and PASSAGE. 
- Snakemake file: Contains the workflow for running the experiments.
- `./blazegraph-cli`: For producing blazegraph CLI commands in Blazegraph experiments. You can find the README.md file in this directory for more information.

## Steps to reproduce the experiments

1. Clone the repository
2. Download the dataset in all needed formats and put them in `./datasets`
3. Refer to each README of the Blazegraph, PASSAGE to produce the necessary jar files for the experiments.
4. For Jena, we took the newest version of Jena, which is 5.1.0, already available inside the `./expe-jena` directory.
5. For Sage, we created a docker image that contains the necessary setup for the experiments.

We also provide the docker images for each engine's experiments in their respective directories. 
You can reproduce all the experiments by running the Snakemake file in the root directory of the repository.

```bash 
snakemake -p -s Snakefile -c1
```

You can also run the experiments individually by defining the config as described in Snakefile.






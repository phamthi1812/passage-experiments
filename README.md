# passage-experiments
This repository contains the experiments in the paper "PASSAGE: Ensuring Completeness and Responsiveness of Public SPARQL Endpoints with SPARQL Continuation Queries"

## 📄 Abstract
Being able to query online public knowledge graphs such as Wikidata or DBpedia is extremely valuable. However, these queries can be interrupted due to the fair use policies enforced by SPARQL endpoint providers, leading to incomplete results. While these policies help maintain the responsiveness of public SPARQL endpoints, they compromise the completeness of query results, which limits the feasibility of various downstream tasks. Ideally, we should not have to choose between completeness and responsiveness. To address this issue, we introduce and formalize the concept of SPARQL continuation queries. When a SPARQL endpoint interrupts a query, it returns partial results along with a SPARQL continuation query to retrieve the remaining results. If the continuation query is also interrupted,the process repeats, generating further continuation queries until the complete results are obtained. In our experimentation, we show that our continuation server PASSAGE ensures completeness and responsiveness while delivering high performance.

## 🌐 Live Demo
You can access the live demo of PASSAGE at [Live Demo PASSAGE](https://live-demo-4455226726.europe-west2.run.app/). <i>It might take half a minute to cold start the cloud instance, so please be patient on your first trial.</i>

- We ingested [WatDiv10M dataset](https://dsg.uwaterloo.ca/watdiv/) using the standard [Blazegraph](https://blazegraph.com/) import procedure. 
- We installed this WatDiv10M dataset in the PASSAGE server.
- We updated [Comunica as a smart client](./passage-comunica) to support PASSAGE:
  - The PASSAGE server handles core SPARQL, 
  - Comunica handles the rest of SPARQL operators.
- We provided some WatDiv queries as example queries to execute.
- The timeout of PASSAGE is set to 5 seconds: approximately every 5 seconds, the PASSAGE server will return partial results if any, and a continuation query.
  > Continuation queries appear in the log of Comunica's interface. When the number of partial results is large, transfering and printing the data might take time. In turns, continuations queries are delayed and might appear at slower pace in the log.


> [!TIP]
> Alternatively, you can communicate directly with the PASSAGE server without using Comunica. For example, the following [`curl`](https://curl.se/) command prints partial results and a continuation query in JSON format:
> ```bash
> curl -X POST -d "query=SELECT * WHERE { \
>   ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?c . \
>   ?s ?p ?o }" \
> https://live-demo-4455226726.europe-west2.run.app/watdiv10M.jnl/passage
> ```
> ```js
> { "...": // lot of actual query results
>  {
>    "s": { "type": "uri" , "value": "http://db.uwaterloo.ca/~galuc/wsdbm/Product16004" },
>    "c": { "type": "uri" , "value": "http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory12" },
>    "p": { "type": "uri" , "value": "http://ogp.me/ns#title" },
>    "o": { "type": "literal" , "value": "shenanigan's Popeye's loins poppa's" }
>  }, // then the SPARQL continuation query as metadata
>  "metadata": { "next": "SELECT *\nWHERE { { { SELECT *\nWHERE { BIND(<http://db.uwaterloo.ca/~galuc/wsdbm/Product16004> AS ?s)\nBIND(<http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory12> AS ?c)\n?s ?p ?o }\nOFFSET 3 } } UNION { { SELECT *\nWHERE { ?s a ?c }\nOFFSET 6049 }\n?s ?p ?o } }" }
>}
>```
> > When the number of results is large, transfering and printing the data might slow down the query execution. In such cases, do not hesitate to redirect the standard output to some file.

## 🗂️ Organization of the Repository
- `./datasets`: You can download different formats of the dataset from the [git repo](https://github.com/MillenniumDB/WDBench). We'll need JNL for Blazegraph and PASSAGE, TDB2 for Jena, and HDT for Sage experiments.
- `./selected_queries`: Contains the queries used in the experiments, which are a subset of the WDBench benchmark that do not contain cartesian products. It contains two subdirectories: `./selected_queries/wdbench-multiple-tps` and `./selected_queries/wdbench-opts`. The first contains queries with multiple triple patterns, and the second contains queries with optional patterns. These queries take between 1 to 5 minutes to execute in Blazegraph under a single virtual CPU.
- `./passage`: Contains the source code of the PASSAGE implementation. Please refer to the README.md file in this directory for more information.
- `./passage-communica`: PASSAGE x Comunica. The extension of the Comunica smart client for supporting all SPARQL features. Please refer to the README.md file in this directory for more information.
- `./expe-blazegraph-baseline`, `./expe-jena`, `./expe-sage`, `./expe-passage`: Contains the scripts used to run the experiments for comparing the performance of Blazegraph, Jena, Sage, and PASSAGE.
- Snakemake file: Contains the workflow for running the experiments.
- `./blazegraph-cli`: Contains Blazegraph CLI commands for Blazegraph experiments. Refer to the README.md file in this directory for more information.

## 🧪 Steps to Reproduce the Experiments

### 📂 1. Clone the Repository
### 📥 2. Download and Set Up the Dataset
Download the dataset in all necessary formats(JNL for Blazegraph and PASSAGE, HDT for SaGe and TDB2 for Jena) and place it in the `./datasets` directory.

### 🔧 3. Build Required Components
- **Blazegraph & PASSAGE**: Refer to the README files in each directory to produce the required `.jar` files.
- **Jena**: We use version **5.1.0** of Jena, which is already included in the `./expe-jena` directory.
- **Sage**: A Docker image has been created to set up Sage for the experiments.

### 🐋 4. Docker Images
We provide Docker images for each engine's experiments. You can find these in their respective directories.

### 🚀 5. Run All Experiments
You can reproduce all experiments by running the Snakemake file located in the root directory:
```bash
snakemake -p -s Snakefile -c1
```

### ⚙️ 6. Run Individual Experiments
To run experiments individually, configure and define the appropriate settings as described in the `Snakefile`.

### 📊 7. Analyze the Results
The analysis of the results is done in `./report_analysis.ipynb`. You can find the figures and tables in the paper.

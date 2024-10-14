# Blazegraph CLI

[Blazegraph](https://github.com/blazegraph/database) does not ship
with a CLI. So we create one.

```bash
mvn package

java -jar ./target/blazegraph-cli.jar --database=path/to/journal.jnl --file=path/to/query.sparql
```

```bash
java -jar ./target/blazegraph-cli.jar --help
# Usage: blazegraph-cli [-h] [--database=<path>] [--file=<path>] [--query=<SPARQL>]
#       --database=<path>   The path to your blazegraph database.
#       --file=<path>       The file containing the SPARQL query to execute.
#   -h, --help              Display this help message.
#       --query=<SPARQL>    The SPARQL query to execute.
```

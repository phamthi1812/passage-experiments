package fr.gdd.blazegraph.cli;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;

/**
 * Blazegraph misses a CLI to provide results without starting a server, so
 * here it is.
 */
public class BlazegraphCLI {

    @CommandLine.Option(
            names = "--database",
            paramLabel = "<path>",
            description = "The path to your blazegraph database.")
    String database;

    @CommandLine.Option(
            names = "--query",
            paramLabel = "<SPARQL>",
            description = "The SPARQL query to execute.")
    String queryAsString;

    @CommandLine.Option(
            names = "--file",
            paramLabel = "<path>",
            description = "The file containing the SPARQL query to execute.")
    String queryFile;

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "Display this help message.")
    boolean usageHelpRequested;

    /* **************************************************************** */

    public static void main(String[] args) throws RepositoryException, SailException {
        BlazegraphCLI serverOptions = new BlazegraphCLI();
        try {
            new CommandLine(serverOptions).parseArgs(args);
        } catch (Exception e) {
            CommandLine.usage(new BlazegraphCLI(), System.out);
            System.exit(CommandLine.ExitCode.USAGE);
        }

        if (serverOptions.usageHelpRequested ||
                Objects.isNull(serverOptions.database) ||
                (Objects.isNull(serverOptions.queryAsString) && Objects.isNull(serverOptions.queryFile))) {
            CommandLine.usage(new BlazegraphCLI(), System.out);
            System.exit(CommandLine.ExitCode.USAGE);
        }

        if (Objects.nonNull(serverOptions.queryFile)) {
            Path queryPath = Path.of(serverOptions.queryFile);
            try {
                serverOptions.queryAsString = Files.readString(queryPath);
            } catch (IOException e) {
                System.err.printf("Error: could not read %s.%n", queryPath);
                System.exit(CommandLine.ExitCode.USAGE);
            }
        }

        System.setProperty("com.bigdata.Banner.quiet", "true"); // shut the banner
        Properties props = new Properties();
        props.put(com.bigdata.journal.Options.FILE, serverOptions.database);
        BigdataSail sail = new BigdataSail(props);
        BigdataSailRepository repository = new BigdataSailRepository(sail);

        // TupleQueryResultWriter writer = QueryResultIO.createWriter(TupleQueryResultFormat.CSV, new PrintStream(System.out));
        // TupleQueryResultWriter writer = new SPARQLResultsCSVWriter(new PrintStream(System.out));

        try {
            repository.initialize();

            RepositoryConnection connection = repository.getReadOnlyConnection();
            TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, serverOptions.queryAsString);
            tupleQuery.setIncludeInferred(true /* includeInferred */);
            connection.close();

            long nbResults = 0;
            long start = System.currentTimeMillis();
            TupleQueryResult result = tupleQuery.evaluate();
            try {
                while (result.hasNext()) {
                    BindingSet bindings = result.next();
                    System.out.println(bindings.toString());
                    nbResults += 1;
                }
            } finally {
                result.close();
            }
            long elapsed = System.currentTimeMillis() - start;
            System.err.printf("Took %s ms to process %s results.%n", elapsed, nbResults);
            System.exit(CommandLine.ExitCode.OK);
        } catch (Exception e) {
            System.err.printf("Error while executing the query: %s%n", e);
            System.exit(CommandLine.ExitCode.SOFTWARE);
        } finally {
            // Actually capital to avoid concurrency issues
            // don't know why though
            repository.shutDown();
        }
    }

}
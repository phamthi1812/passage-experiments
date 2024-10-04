package fr.gdd.sage;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Execute a SPARQL query with or without preemption, and measure
 * the execution time, and count the number of results.
 */
public class SageCLI {

    @CommandLine.Option(
            names = "--database",
            paramLabel = "<path>",
            description = "The path to your blazegraph database.")
    String database;

    @CommandLine.Option(
            names = "--executions",
            paramLabel = "1",
            description = "Number of times that it executes the query in sequence.")
    Integer numberOfExecutions = 1;

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

    @CommandLine.Option(
            names = "--timeout",
            paramLabel = "<ms>",
            description = "Timeout before the query is stopped (Blazegraph) or pause/resumed (Sage).")
    Long timeout = Long.MAX_VALUE;

    @CommandLine.Option(
            names = "--engine",
            paramLabel = "{sage|blazegraph}",
            description = "Set the SPARQL query engine for the execution (Sage, or Blazegraph).")
    String engine;

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "Display this help message.")
    boolean usageHelpRequested;

    /**
     * Aims to be the main of a benchmarking snakemake command.
     */
    public static void main(String[] args) throws RepositoryException, SailException {
        SageCLI serverOptions = new SageCLI();
        new CommandLine(serverOptions).parseArgs(args);

        if (serverOptions.usageHelpRequested ||
                Objects.isNull(serverOptions.database) ||
                (Objects.isNull(serverOptions.queryAsString) && Objects.isNull(serverOptions.queryFile)) ||
                Objects.isNull(serverOptions.engine)) {
            CommandLine.usage(new SageCLI(), System.out);
            return;
        }

        if (!serverOptions.engine.trim().equalsIgnoreCase("sage") &&
                !serverOptions.engine.trim().equalsIgnoreCase("blazegraph")) {
            System.out.println("SPARQL query engine unknown: " + serverOptions.engine +".");
            CommandLine.usage(new SageCLI(), System.out);
            return;
        }

        if (Objects.nonNull(serverOptions.queryFile)) {
            Path queryPath = Path.of(serverOptions.queryFile);
            try {
                serverOptions.queryAsString = Files.readString(queryPath);
            } catch (IOException e) {
                System.out.println("Error: could not read " + queryPath.toString() + ".");
            }
        }

        if (Objects.isNull(serverOptions.numberOfExecutions)) {
            serverOptions.numberOfExecutions = 1;
        }

        if (Objects.isNull(serverOptions.timeout)) {
            serverOptions.timeout = Long.MAX_VALUE;
        }

        BlazegraphBackend backend = new BlazegraphBackend(serverOptions.database);

        System.out.println(serverOptions.queryAsString);

        System.out.println("run time results preemptions");
        for (int i = 0; i < serverOptions.numberOfExecutions; ++i) {
            System.gc(); // no guarantee but still
            long start = System.currentTimeMillis();
            Pair<Long, Long> results = runWithBackend(backend, serverOptions.engine, serverOptions.queryAsString, serverOptions.timeout);
            long elapsed = System.currentTimeMillis() - start;
            System.out.printf("%s %s %s %s%n", i, elapsed, results.getLeft(), results.getRight());
        }
    }

    public static Pair<Long, Long> runWithBackend(BlazegraphBackend backend, String engine, String queryAsString, Long timeout) {
        return switch (engine.trim().toLowerCase()) {
            case "sage" -> {
                long nbResults = 0;
                long nbPreempt = -1; // first execution is normal
                while (Objects.nonNull(queryAsString)) {
                    Pair<Integer, String> nbResultsAndResume = SageExecutor.executeQueryWithTimeout(queryAsString, backend, timeout);
                    nbResults += nbResultsAndResume.getLeft();
                    queryAsString = nbResultsAndResume.getRight();
                    nbPreempt += 1;
                }
                yield new ImmutablePair<>(nbResults, nbPreempt);
            }
            case "blazegraph" -> new ImmutablePair<>(
                    SageExecutor.executeWithBlazegraph(queryAsString, backend, timeout),
                    0L // no preemption, ofc
            );
            default -> throw new UnsupportedOperationException("Unkown engine: " + engine + ".");
        };
    }
}

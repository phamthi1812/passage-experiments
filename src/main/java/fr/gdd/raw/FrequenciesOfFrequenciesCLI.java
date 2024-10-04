package fr.gdd.raw;

import com.google.common.collect.Multiset;
import fr.gdd.sage.blazegraph.BlazegraphBackend;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Processes the frequencies of frequencies of a SPARQL query.
 * This is important to assess the efficiency of CRAWD, and the inefficiency
 * of state-of-the-art count-distinct approaches.
 */
public class FrequenciesOfFrequenciesCLI {

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

    @CommandLine.Option(
            names = "--variable",
            paramLabel = "<name>",
            description = "The variable name on which count-distinct is performed, e.g. ?s"
    )
    String varName;

    @CommandLine.Option(
            names = "--slow",
            description = "Slower execution time, but should be better in terms of memory."
    )
    Boolean slower = false;

    @CommandLine.Option(
            names = "--medium",
            paramLabel = "<nbthreads>",
            description = "Distinct are fast, but then count for each and every, so it's slower."
    )
    Integer medium = 1;


    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "Display this help message.")
    boolean usageHelpRequested;

    /* **************************************************************** */

    public static void main(String[] args) throws RepositoryException, SailException {
        FrequenciesOfFrequenciesCLI serverOptions = new FrequenciesOfFrequenciesCLI();
        new CommandLine(serverOptions).parseArgs(args);

        if (serverOptions.usageHelpRequested ||
                Objects.isNull(serverOptions.database) ||
                (Objects.isNull(serverOptions.queryAsString) && Objects.isNull(serverOptions.queryFile)) ||
                Objects.isNull(serverOptions.varName)) {
            CommandLine.usage(new FrequenciesOfFrequenciesCLI(), System.out);
            System.exit(CommandLine.ExitCode.USAGE);
        }

        if (Objects.nonNull(serverOptions.queryFile)) {
            Path queryPath = Path.of(serverOptions.queryFile);
            try {
                serverOptions.queryAsString = Files.readString(queryPath);
            } catch (IOException e) {
                System.out.printf("Error: could not read %s.%n", queryPath);
                System.exit(CommandLine.ExitCode.USAGE);
            }
        }

        BlazegraphBackend backend = new BlazegraphBackend(serverOptions.database);

        long start = System.currentTimeMillis();
        if (serverOptions.slower) {
            slowExecution(backend, serverOptions.queryAsString, serverOptions.varName, serverOptions.medium);
        }

        if (serverOptions.medium > 1) {
            slowExecution(backend, serverOptions.queryAsString, serverOptions.varName, serverOptions.medium);
        }

        if (!(serverOptions.medium > 1) && !serverOptions.slower) {
            // TODO better transformation of query to avoid processing the query inside the query
            serverOptions.queryAsString = String.format("""
                            SELECT (COUNT(*) AS ?f) ?c WHERE {
                                SELECT (COUNT(*) AS ?c) %s WHERE {%s} GROUP BY %s
                            } GROUP BY ?c
                            """,
                    serverOptions.varName,
                    serverOptions.queryAsString, // if no optimization are done, this is highly unefficient
                    serverOptions.varName
            );
            System.out.println(serverOptions.queryAsString);
            fastExecution(backend, serverOptions.queryAsString);
        }

        long elapsed = System.currentTimeMillis() - start;
        System.out.printf("Took %s ms to process.%n", elapsed);
        System.exit(CommandLine.ExitCode.OK);
    }

    /**
     * Actually normal execution, but faster than the other one.
     * @param backend The blazegraph backend to execute the query.
     * @param queryAsString The SPARQL query to execute.
     */
    public static void slowExecution(BlazegraphBackend backend, String queryAsString, String varName, Integer medium) {
        String queryDistinct = String.format("""
                SELECT DISTINCT %s WHERE {
                    %s
                }""", varName, queryAsString);

        System.out.println(queryDistinct);

        ConcurrentHashMap<Integer, Long> f2cConcu = new ConcurrentHashMap<>();
        try {
            Iterator<BindingSet> distincts = medium > 1 ?
                    backend.executeQuery(queryDistinct).iterator() : // all distinct at once
                    backend.executeQueryToIterator(queryDistinct); // one by one

            try (var pool = Executors.newFixedThreadPool(medium)){
                List<Future<Integer>> futureCounts = new ArrayList<>();

                while (distincts.hasNext()) {
                    BindingSet bs = distincts.next();
                    final Value distinctAsValue = bs.getValue(varName.replace("?", ""));
                    var futureCount = pool.submit(() -> {
                        String distinct = switch (distinctAsValue) { // ugly but w/e for now
                            case Literal l -> "\"" + l.toString() + "\"";
                            case URI u -> "<" + u.toString() + ">";
                            default -> throw new UnsupportedOperationException("Distinct cast");
                        };

                        // System.out.printf("%s%n",distinct);

                        String countQueryAsString = String.format("""
                                    SELECT (COUNT(*) AS ?c) WHERE {%s}
                                """, queryAsString.replace(varName, distinct));

                        Iterator<BindingSet> counts = backend.executeQueryToIterator(countQueryAsString);
                        if (!counts.hasNext()) {
                            throw new RuntimeException("weird");
                        }
                        Value f = counts.next().getBinding("c").getValue();

                        Integer frequency = ((Literal) f).intValue();
                        f2cConcu.putIfAbsent(frequency, 0L);
                        f2cConcu.compute(frequency, (k, v) -> v + 1L);
                        return CommandLine.ExitCode.OK;
                    });
                    futureCounts.add(futureCount);
                }

                futureCounts.forEach(future -> {
                    try {
                        future.get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                });

            }
        } catch (Exception e) {
            System.out.printf("Error while executing the distinct query: %s%n", e);
            System.exit(CommandLine.ExitCode.SOFTWARE);
        }

        System.out.println("# f c");
        for (Map.Entry<Integer,Long> fc : f2cConcu.entrySet()) {
            System.out.printf("%s %s%n", fc.getValue(), fc.getKey());
        }
    }

    /**
     * Actually normal execution, but faster than the other one.
     * @param backend The blazegraph backend to execute the query.
     * @param queryAsString The SPARQL query to execute.
     */
    public static void fastExecution(BlazegraphBackend backend, String queryAsString) {
        Multiset<BindingSet> results = null;
        try {
            // Probably do not need to perform the optimisation where
            // the result is output as soon as it's produced, since the count
            // must explore all results anyway.
            results = backend.executeQuery(queryAsString);
        } catch (Exception e) {
            System.out.printf("Error while executing the query: %s%n", e);
            System.exit(CommandLine.ExitCode.SOFTWARE);
        }

        if (Objects.nonNull(results)) {
            System.out.println("# f c");
            for (BindingSet bs : results.elementSet()) {
                Literal f = (Literal) bs.getBinding("f").getValue();
                int fAsInt = f.intValue();
                Literal c = (Literal) bs.getBinding("c").getValue();
                int cAsInt = c.intValue();
                System.out.printf("%s %s%n", fAsInt, cAsInt);
            }
        }
    }

}
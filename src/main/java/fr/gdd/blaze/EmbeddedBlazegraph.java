package fr.gdd.blaze;

import com.google.common.collect.Multiset;
import fr.gdd.sage.blazegraph.BlazegraphBackend;
import org.openrdf.query.BindingSet;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;


/**
 * Processes the frequencies of frequencies of a SPARQL query.
 * This is important to assess the efficiency of CRAWD, and the inefficiency
 * of state-of-the-art count-distinct approaches.
 */
public class EmbeddedBlazegraph {

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
            names = "--timeout",
            paramLabel = "<milliseconds>",
            description = "Timeout for the SPARQL query execution in milliseconds.")
    Long timeout = Long.MAX_VALUE;

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "Display this help message.")
    boolean usageHelpRequested;

    /* **************************************************************** */

    public static void main(String[] args) throws RepositoryException, SailException {
        EmbeddedBlazegraph serverOptions = new EmbeddedBlazegraph();
        new CommandLine(serverOptions).parseArgs(args);

        if (serverOptions.usageHelpRequested ||
                Objects.isNull(serverOptions.database) ||
                (Objects.isNull(serverOptions.queryAsString) && Objects.isNull(serverOptions.queryFile)) ||
                (Objects.isNull(serverOptions.timeout))) {
            CommandLine.usage(new EmbeddedBlazegraph(), System.out);
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
        if (Objects.isNull(serverOptions.timeout)) {
            serverOptions.timeout = Long.MAX_VALUE;
        }

        BlazegraphBackend backend = new BlazegraphBackend(serverOptions.database);

        long start = System.currentTimeMillis();
        Multiset<BindingSet> results = null;

        try {
            results = backend.executeQuery(serverOptions.queryAsString);
            long elapsed = System.currentTimeMillis() - start;

            if (elapsed > serverOptions.timeout) {
                System.exit(CommandLine.ExitCode.SOFTWARE);
            }
        } catch (Exception e) {
            System.out.printf("Error while executing the query: %s%n", e);
            System.exit(CommandLine.ExitCode.SOFTWARE);
        }

        if (Objects.nonNull(results)) {
            for (BindingSet bs : results.elementSet()) {
                System.out.printf("%s %n", bs);
            }
        }

        long totalElapsed = System.currentTimeMillis() - start;
        System.out.printf("Took %s ms to process.%n", totalElapsed);
        System.exit(CommandLine.ExitCode.OK);
    }
}
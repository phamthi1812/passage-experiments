package fr.gdd.utils;

import fr.gdd.sage.SageCLI;
import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.sager.optimizers.CardinalityJoinOrdering;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class CheckCartesianProductCLI {

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


    /**
     * Aims to be the main of a benchmarking snakemake command.
     */
    public static void main(String[] args) {
        CheckCartesianProductCLI serverOptions = new CheckCartesianProductCLI();
        new CommandLine(serverOptions).parseArgs(args);

        if (Objects.isNull(serverOptions.queryFile) && Objects.isNull(serverOptions.queryAsString)) {
            CommandLine.usage(new CheckCartesianProductCLI(), System.out);
            System.exit(CommandLine.ExitCode.USAGE);
        }

        if (Objects.nonNull(serverOptions.queryFile)) {
            Path queryPath = Path.of(serverOptions.queryFile);
            try {
                serverOptions.queryAsString = Files.readString(queryPath);
            } catch (IOException e) {
                System.out.println("Error: could not read " + queryPath.toString() + ".");
                System.exit(CommandLine.ExitCode.SOFTWARE);
            }
        }

        Op queryAsOp = Algebra.compile(QueryFactory.create(serverOptions.queryAsString));

        BlazegraphBackend empty = null;
        try {
            empty = new BlazegraphBackend();
        } catch (SailException | RepositoryException e) {
            throw new RuntimeException(e);
        }

        CardinalityJoinOrdering<?, ?> isCartesian = new CardinalityJoinOrdering<>(empty);
        isCartesian.visit(queryAsOp);

        if (isCartesian.hasCartesianProduct()) {
            System.out.println("True");
        } else {
            System.out.println("False");
        }

        System.exit(CommandLine.ExitCode.OK);
    }

}

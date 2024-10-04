package org.apache.jena;

import org.apache.jena.query.*;
import org.apache.jena.tdb2.TDB2Factory;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class JenaCli {
    @CommandLine.Option(
            names = "--database",
            paramLabel = "<path>",
            description = "The path to your TDB2 database.")
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

    public static void main(String[] args) {
        JenaCli serverOptions = new JenaCli();
        new CommandLine(serverOptions).parseArgs(args);

        if (serverOptions.usageHelpRequested ||
                Objects.isNull(serverOptions.database) ||
                (Objects.isNull(serverOptions.queryAsString) && Objects.isNull(serverOptions.queryFile))) {
            CommandLine.usage(new JenaCli(), System.out);
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

        Dataset dataset = TDB2Factory.connectDataset(serverOptions.database);
        long startTime = System.currentTimeMillis();
        try {
            dataset.begin(ReadWrite.READ);
            try {
                Query query = QueryFactory.create(serverOptions.queryAsString);
                try (QueryExecution qExec = QueryExecutionFactory.create(query, dataset)) {
                    ResultSet results = qExec.execSelect();
                    ResultSetFormatter.out(System.out, results, query);
                }
                dataset.commit();
            } catch (Exception e) {
                dataset.abort();
                throw e;
            } finally {
                dataset.end();
            }
        } catch (Exception e) {
            System.err.println("Error executing query: " + e.getMessage());
            e.printStackTrace();
            System.exit(CommandLine.ExitCode.SOFTWARE);
        } finally {
            dataset.close();
        }
        long endTime = System.currentTimeMillis();
        System.out.printf("Took %s ms to process.%n", endTime - startTime);
        System.exit(CommandLine.ExitCode.OK);
    }
}

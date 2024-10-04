package fr.gdd.sage;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.SagerOpExecutor;
import fr.gdd.sage.sager.resume.BGP2Triples;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.base.Sys;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Set of static functions that execute the query depending on the configuration.
 */
public class SageExecutor {

    private static final Logger log = LoggerFactory.getLogger(SageExecutor.class);

    public static long executeWithBlazegraph(String queryAsString, BlazegraphBackend backend, Long timeout) {
        try {
            return backend.countQuery(queryAsString, timeout);
        } catch (Exception e) {
            return -1; // error of evaluation, or repository, i.e, not about timeout
        }
    };

    /**
     * @param queryAsString The SPARQL query to execute.
     * @param backend The backend to execute on.
     * @return The preempted query after one result.
     */
    public static <ID, VALUE> Pair<Integer, String> executeQuery(String queryAsString, Backend<ID, VALUE, ?> backend) {
        return executeQueryWithLimit(queryAsString, backend, 1L);
    }

    /**
     * @param queryAsString The SPARQL query to execute.
     * @param backend The backend to execute on.
     * @param limit The number of actual results mappings before pausing.
     * @return The preempted query after one result.
     */
    public static <ID, VALUE> Pair<Integer, String> executeQueryWithLimit(String queryAsString, Backend<ID, VALUE, ?> backend, Long limit) {
        ARQ.enableOptimizer(false);

        Op query = Algebra.compile(QueryFactory.create(queryAsString));
        query = ReturningOpVisitorRouter.visit(new BGP2Triples(), query);

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(SagerConstants.BACKEND, backend);

        SagerOpExecutor<ID, VALUE> executor = new SagerOpExecutor<ID,VALUE>(ec).setLimit(limit);

        Iterator<BackendBindings<ID, VALUE>> iterator = executor.execute(query);
        int nbResults = 0;
        while (iterator.hasNext()) {
            log.debug("{}", iterator.next());
            nbResults += 1;
        };

        return new ImmutablePair<>(nbResults, executor.pauseAsString());
    }

    public static <ID, VALUE> Pair<Integer, String> executeQueryWithTimeout(String queryAsString, Backend<ID, VALUE, Long> backend, Long timeout) {
        ARQ.enableOptimizer(false);

        Op query = Algebra.compile(QueryFactory.create(queryAsString));
        query = ReturningOpVisitorRouter.visit(new BGP2Triples(), query);

        SagerOpExecutor<ID, VALUE> executor = new SagerOpExecutor<ID,VALUE>().setTimeout(timeout).setBackend(backend);

        Iterator<BackendBindings<ID, VALUE>> iterator = executor.execute(query);

        int nbResults = 0;
        while (iterator.hasNext()){
            log.debug("{}", iterator.next());
            ++nbResults;
        }

        return new ImmutablePair<>(nbResults, executor.pauseAsString());
    }

}


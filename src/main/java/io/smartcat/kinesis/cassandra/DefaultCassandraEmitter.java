package io.smartcat.kinesis.cassandra;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

/**
 * This class in a basic implementation of {@link CassandraEmitter}. It connects to a Cassandra
 * cluster using the given {@link CassandraKinesisConnectorConfiguration} properties. All the
 * given {@link CassandraRecord}s in {@link CassandraEmitter#emit(UnmodifiableBuffer)} are stored
 * into the respective Cassandra table executing {@link Session#executeAsync(String)} with
 * {@link CassandraRecord#toCqlStatement()}.
 */
public class DefaultCassandraEmitter implements CassandraEmitter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCassandraEmitter.class);

    private CassandraKinesisConnectorConfiguration config;
    private Cluster cluster;
    private Session session;

    @Override
    public void init(CassandraKinesisConnectorConfiguration config) {
        this.config = config;

        LOGGER.info("Connecting to cluster with contact points: {} and port: {}",
                config.cassandraContactPoints, config.cassandraPort);

        final String[] nodes = config.cassandraContactPoints.split(",");

        final QueryOptions queryOptions = new QueryOptions();
        queryOptions.setConsistencyLevel(config.cassandraConsistencyLevel);

        cluster = Cluster.builder()
                .addContactPoints(nodes)
                .withPort(config.cassandraPort)
                .withCredentials(config.cassandraUsername, config.cassandraPassword)
                .withQueryOptions(queryOptions)
                .build();
        session = cluster.connect();
    }

    @Override
    public List<List<CassandraRecord>> emit(UnmodifiableBuffer<List<CassandraRecord>> buffer) throws IOException {
        if (config == null || !cluster.isClosed()) {
            throw new IllegalStateException("Emitter has to be initialized first.");
        }
        insert(buffer.getRecords());

        return Collections.emptyList();
    }

    @Override
    public void fail(List<List<CassandraRecord>> records) {
        for (List<CassandraRecord> cassRecords : records) {
            for (CassandraRecord rec : cassRecords) {
                LOGGER.error("Could not emit record: " + rec);
            }
        }
    }

    @Override
    public void shutdown() {
        LOGGER.info("Disconnecting from cluster.");
        if (cluster != null && !cluster.isClosed()) {
            cluster.close();
            cluster = null;
        }
    }

    private void insert(List<List<CassandraRecord>> records) {
        for (List<CassandraRecord> cassRecords : records) {
            for (CassandraRecord record : cassRecords) {
                executeInsertsAsync(record.toCqlStatement());
            }
        }
    }

    private void executeInsertsAsync(Statement statement) {
        LOGGER.debug("Inserting {}", statement.toString());
        session.executeAsync(statement);
    }
}

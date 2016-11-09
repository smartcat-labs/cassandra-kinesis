package io.smartcat.kinesis.cassandra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

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
                config.CASSANDRA_CONTACT_POINTS, config.CASSANDRA_PORT);

        final String[] nodes = config.CASSANDRA_CONTACT_POINTS.split(",");

        cluster = Cluster.builder()
                .addContactPoints(nodes)
                .withPort(config.CASSANDRA_PORT)
                .withCredentials(config.CASSANDRA_USERNAME, config.CASSANDRA_PASSWORD)
                .build();
        session = cluster.connect();
    }

    @Override
    public List<List<CassandraRecord>> emit(UnmodifiableBuffer<List<CassandraRecord>> buffer) throws IOException {
        if (config == null) {
            throw new IllegalStateException("Emitter has to be initialized first.");
        }
        List<String> inserts = prepareInserts(buffer.getRecords());
        executeInsertsAsync(inserts);
        return new ArrayList<>();
    }

    @Override
    public void fail(List<List<CassandraRecord>> records) {
        for (List<CassandraRecord> cassRecords : records) {
            for (CassandraRecord rec: cassRecords) {
                LOGGER.error("Could not emit record: " + rec);
            }
        }
    }

    @Override
    public void shutdown() {
        LOGGER.info("Disconnecting from cluster.");
        if (cluster != null) {
            cluster.close();
        }
    }

    private List<String> prepareInserts(List<List<CassandraRecord>> records) {
        final List<String> inserts = new ArrayList<>();
        for (List<CassandraRecord> cassRecords : records) {
            for (CassandraRecord record : cassRecords) {
                inserts.add(record.toCqlStatement());
            }
        }
        return inserts;
    }

    private void executeInsertsAsync(List<String> inserts) {
        for (String insert : inserts) {
            LOGGER.info("Inserting {}", insert);
            session.executeAsync(insert);
        }
    }
}

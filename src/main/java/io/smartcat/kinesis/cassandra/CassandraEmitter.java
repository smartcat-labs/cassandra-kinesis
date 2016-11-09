package io.smartcat.kinesis.cassandra;

import java.util.List;

import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;

/**
 * An implementation of this interface is responsible for storing {@link CassandraRecord}s
 * into the configured Cassandra database.
 *
 * @see IEmitter
 */
public interface CassandraEmitter extends IEmitter<List<CassandraRecord>> {
    /**
     * This method is called after an instance construction and before any other
     * action in order to complete any necessary initialization steps.
     *
     * @param config actual configuration
     */
    void init(CassandraKinesisConnectorConfiguration config);
}

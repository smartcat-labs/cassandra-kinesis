package io.smartcat.kinesis.cassandra;

import java.util.List;

/**
 * An implementation of this interface is responsible for transforming a record coming
 * from Kinesis stream (<code>byte[]</code>) into a list of {@link CassandraRecord}s.
 */
public interface CassandraTransformer {
    /**
     * This method is called after an instance construction and before any other
     * action in order to complete any necessary initialization steps.
     *
     * @param config actual configuration
     */
    void init(CassandraKinesisConnectorConfiguration config);

    /**
     * Converts a Kinesis input record into one or more {@link CassandraRecord}s.
     *
     * @param record a record coming from Kinesis stream
     * @return one or more cassandra records
     */
    List<CassandraRecord> transform(byte[] record);
}

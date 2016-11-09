package io.smartcat.kinesis.cassandra;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorExecutorBase;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;
import com.amazonaws.services.kinesis.connectors.impl.AllPassFilter;
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformerBase;
import com.amazonaws.services.kinesis.model.Record;

/**
 * An implementation of {@link KinesisConnectorExecutorBase} for Cassandra. It uses {@link CassandraTransformer} and
 * {@link CassandraEmitter} specified by the given {@link CassandraKinesisConnectorConfiguration} as
 * {@link ITransformerBase} and {@link IEmitter} respectively. A simple {@link BasicMemoryBuffer} is used as
 * {@link IBuffer}, and {@link AllPassFilter} as {@link IFilter}.
 */
public class CassandraKinesisConnectorExecutor extends KinesisConnectorExecutorBase<byte[], List<CassandraRecord>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraKinesisConnectorExecutor.class);

    private CassandraKinesisConnectorConfiguration config;

    /**
     * Constructor.
     * @param config the given configuration
     */
    public CassandraKinesisConnectorExecutor(CassandraKinesisConnectorConfiguration config) {
        super();
        this.config = config;
        initialize(config);
    }

    @Override
    public KinesisConnectorRecordProcessorFactory<byte[], List<CassandraRecord>>
            getKinesisConnectorRecordProcessorFactory() {
        return new KinesisConnectorRecordProcessorFactory<>(
                new IKinesisConnectorPipeline<byte[], List<CassandraRecord>>() {
            @Override
            public IEmitter<List<CassandraRecord>> getEmitter(KinesisConnectorConfiguration configuration) {
                LOGGER.info("Creating CassandraEmitter for class name {}", config.CASSANDRA_EMITTER_CLASS);
                CassandraEmitter emitter = null;
                try {
                    emitter = (CassandraEmitter) Class.forName(config.CASSANDRA_EMITTER_CLASS).newInstance();
                    emitter.init(config);
                } catch (Exception e) {
                    LOGGER.error("Failed to create CassandraEmitter by class name", e);
                }
                return emitter;
            }

            @Override
            public IBuffer<byte[]> getBuffer(KinesisConnectorConfiguration configuration) {
                return new BasicMemoryBuffer<>(configuration);
            }

            @Override
            public ITransformerBase<byte[], List<CassandraRecord>> getTransformer(
                    KinesisConnectorConfiguration configuration) {
                LOGGER.info("Creating CassandraTransformer for class name {}", config.CASSANDRA_TRANSFORMER_CLASS);
                CassandraTransformer transformer;
                try {
                    transformer =
                            (CassandraTransformer) Class.forName(config.CASSANDRA_TRANSFORMER_CLASS).newInstance();
                    transformer.init(config);
                } catch (Exception e) {
                    LOGGER.error("Failed to create CassandraTransformer by class name", e);
                    throw new IllegalStateException(e);
                }

                return new ITransformer<byte[], List<CassandraRecord>>() {

                    @Override
                    public List<CassandraRecord> fromClass(byte[] record) throws IOException {
                        return transformer.transform(record);
                    }

                    @Override
                    public byte[] toClass(Record record) throws IOException {
                        return record.getData().array();
                    }

                };
            }

            @Override
            public IFilter<byte[]> getFilter(KinesisConnectorConfiguration configuration) {
                return new AllPassFilter<>();
            }
        }, config);
    }
}

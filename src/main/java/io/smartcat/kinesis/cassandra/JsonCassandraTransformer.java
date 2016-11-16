package io.smartcat.kinesis.cassandra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class implements {@link CassandraTransformer} where input records are expected to
 * be JSON objects (strings). Every JSON top-level property is treated as a Cassandra table
 * column. Only JSON properties of type string, boolean or number are processed. {@link CassandraRecord}
 * keyspace and table are set using the
 * {@link CassandraKinesisConnectorConfiguration#cassandraKeyspace} and
 * {@link CassandraKinesisConnectorConfiguration#cassandraTable}.
 */
public class JsonCassandraTransformer implements CassandraTransformer {
    private static final Log LOGGER = LogFactory.getLog(JsonCassandraTransformer.class);

    private CassandraKinesisConnectorConfiguration config;

    @Override
    public void init(CassandraKinesisConnectorConfiguration config) {
        this.config = config;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<CassandraRecord> transform(byte[] record) {
        final ObjectMapper mapper = new ObjectMapper();
        final String input = new String(record);
        final List<CassandraRecord> output = new ArrayList<>();

        Map<String, Object> map = null;
        try {
            map = mapper.readValue(input, Map.class);
            CassandraRecord outRecord = new CassandraRecord(config.cassandraKeyspace, config.cassandraTable);
            for (String prop : map.keySet()) {
                Object value = map.get(prop);
                if (value instanceof String || value instanceof Boolean || value instanceof Integer
                        || value instanceof Long || value instanceof Double) {
                    outRecord.setValue(prop, value);
                }
            }
            output.add(outRecord);
        } catch (IOException e) {
            LOGGER.warn("Unable to parse as JSON object: " + input);
        }

        return output;
    }
}

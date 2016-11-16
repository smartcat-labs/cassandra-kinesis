package io.smartcat.kinesis.cassandra;

import static org.assertj.core.api.Assertions.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonCassandraTransformerTest {

    @Test
    public void test_transform() throws Exception {
        Properties props = new Properties();
        props.put(CassandraKinesisConnectorConfiguration.ConfigurationOptions.PROP_CASSANDRA_KEYSPACE, "spacekey");
        props.put(CassandraKinesisConnectorConfiguration.ConfigurationOptions.PROP_CASSANDRA_TABLE, "sink");
        CassandraKinesisConnectorConfiguration config = new CassandraKinesisConnectorConfiguration(props,
                new DefaultAWSCredentialsProviderChain());

        CassandraTransformer transformer = new JsonCassandraTransformer();
        transformer.init(config);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> inputObj = new HashMap<>();
        inputObj.put("id", "123");
        inputObj.put("time", 123456);
        inputObj.put("value", 4.5);
        String input = mapper.writeValueAsString(inputObj);
        List<CassandraRecord> res = transformer.transform(input.getBytes());
        assertThat(res.get(0).getKeyspace()).isEqualTo("spacekey");
        assertThat(res.get(0).getTable()).isEqualTo("sink");
        assertThat(res.get(0).getValue("id")).isEqualTo("123");
        assertThat(res.get(0).getValue("time")).isEqualTo(123456);
        assertThat(res.get(0).getValue("value")).isEqualTo(4.5);
    }

}

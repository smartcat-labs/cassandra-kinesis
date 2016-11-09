package io.smartcat.kinesis.cassandra;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;

/**
 * This class is an extension of {@link KinesisConnectorConfiguration} that adds constants used
 * to configure Kinesis Cassandra connector.
 *
 * @see KinesisConnectorConfiguration
 */
public class CassandraKinesisConnectorConfiguration extends KinesisConnectorConfiguration {
    private static final Log LOG = LogFactory.getLog(CassandraKinesisConnectorConfiguration.class);

    /**
     * Configuration property name for specifying {@link CassandraTransformer} implementation.
     */
    public static final String PROP_CASSANDRA_TRANSFORMER_CLASS = "cassandraTransformer";

    /**
     * Configuration property name for specifying {@link CassandraEmitter} implementation.
     */
    public static final String PROP_CASSANDRA_EMITTER_CLASS = "cassandraEmitter";

    /**
     * Configuration property name used by {@link DefaultCassandraEmitter} for specifying
     * Cassandra cluster contact points.
     */
    public static final String PROP_CASSANDRA_CONTACT_POINTS = "cassandraContactPoints";

    /**
     * Configuration property name used by {@link DefaultCassandraEmitter} for specifying
     * Cassandra native CQL port.
     */
    public static final String PROP_CASSANDRA_PORT = "cassandraPort";

    /**
     * Configuration property name used by {@link DefaultCassandraEmitter} for specifying
     * Cassandra username.
     */
    public static final String PROP_CASSANDRA_USERNAME = "cassandraUsername";

    /**
     * Configuration property name used by {@link DefaultCassandraEmitter} for specifying
     * Cassandra password.
     */
    public static final String PROP_CASSANDRA_PASSWORD = "cassandraPassword";

    /**
     * Configuration property name used by {@link DefaultCassandraEmitter} for specifying
     * Cassandra keyspace.
     */
    public static final String PROP_CASSANDRA_KEYSPACE = "cassandraKeyspace";

    /**
     * Configuration property name used by {@link DefaultCassandraEmitter} for specifying
     * Cassandra table name.
     */
    public static final String PROP_CASSANDRA_TABLE = "cassandraTable";

    /**
     * Configuration property name used by {@link DefaultCassandraEmitter} for specifying
     * write consistency level.
     */
    public static final String PROP_CASSANDRA_CONSISTENCY_LEVEL = "cassandraConsistencyLevel";

    /**
     * Default value for {@link CassandraKinesisConnectorConfiguration#CASSANDRA_TRANSFORMER_CLASS}.
     */
    public static final String DEFAULT_CASSANDRA_TRANSFORMER_CLASS = JsonCassandraTransformer.class.getName();

    /**
     * Default value for {@link CassandraKinesisConnectorConfiguration#CASSANDRA_EMITTER_CLASS}.
     */
    public static final String DEFAULT_CASSANDRA_EMITTER_CLASS = DefaultCassandraEmitter.class.getName();

    /**
     * Default value for {@link CassandraKinesisConnectorConfiguration#CASSANDRA_CONTACT_POINTS}.
     */
    public static final String DEFAULT_CASSANDRA_CONTACT_POINTS = "127.0.0.1";

    /**
     * Default value for {@link CassandraKinesisConnectorConfiguration#CASSANDRA_PORT}.
     */
    public static final int DEFAULT_CASSANDRA_PORT = 9042;

    /**
     * Default value for {@link CassandraKinesisConnectorConfiguration#CASSANDRA_USERNAME}.
     */
    public static final String DEFAULT_CASSANDRA_USERNAME = null;

    /**
     * Default value for {@link CassandraKinesisConnectorConfiguration#CASSANDRA_PASSWORD}.
     */
    public static final String DEFAULT_CASSANDRA_PASSWORD = null;

    /**
     * Default value for {@link CassandraKinesisConnectorConfiguration#CASSANDRA_KEYSPACE}.
     */
    public static final String DEFAULT_CASSANDRA_KEYSPACE = "test";

    /**
     * Default value for {@link CassandraKinesisConnectorConfiguration#CASSANDRA_TABLE}.
     */
    public static final String DEFAULT_CASSANDRA_TABLE = "kinesis";

    /**
     * Default value for {@link CassandraKinesisConnectorConfiguration#CASSANDRA_CONSISTENCY_LEVEL}.
     */
    public static final String DEFAULT_CASSANDRA_CONSISTENCY_LEVEL = "ONE";

    /**
     * FQDN of a Java class that implements {@link Transformer}. It transforms input Kinesis record
     * (<code>byte[]</code>) into a list of {@link CassandraRecord}. The default value is
     * {@link io.smartcat.kinesis.cassandra.JsonCassandraTransformer}.
     */
    public final String CASSANDRA_TRANSFORMER_CLASS;

    /**
     * FQDN of a Java class that implements {@link CassandraEmitter}. It is responsible for storing
     * {@link CassandraRecord} instances into Cassandra.
     */
    public final String CASSANDRA_EMITTER_CLASS;

    /**
     * List of Cassandra cluster nodes' IP addresses (comma separated list).
     */
    public final String CASSANDRA_CONTACT_POINTS;

    /**
     * Cassandra native CQL port.
     */
    public final int CASSANDRA_PORT;

    /**
     * Cassandra authentication username.
     */
    public final String CASSANDRA_USERNAME;

    /**
     * Cassandra authentication password.
     */
    public final String CASSANDRA_PASSWORD;

    /**
     * Cassandra keyspace to use.
     */
    public final String CASSANDRA_KEYSPACE;

    /**
     * Cassandra table name to use as the target.
     */
    public final String CASSANDRA_TABLE;

    /**
     * Cassandra write consistency level.
     */
    public final String CASSANDRA_CONSISTENCY_LEVEL;

    /**
     * Configuration constructor.
     *
     * @param properties user-defined properties
     * @param credentialsProvider AWS credentials provider
     *
     * @see {@link KinesisConnectorConfiguration}
     */
    public CassandraKinesisConnectorConfiguration(Properties properties,
            AWSCredentialsProvider credentialsProvider) {
        super(properties, credentialsProvider);

        CASSANDRA_CONTACT_POINTS =
                properties.getProperty(PROP_CASSANDRA_CONTACT_POINTS, DEFAULT_CASSANDRA_CONTACT_POINTS);
        CASSANDRA_PORT =
                getIntegerProperty(PROP_CASSANDRA_PORT, DEFAULT_CASSANDRA_PORT, properties);
        CASSANDRA_USERNAME =
                properties.getProperty(PROP_CASSANDRA_USERNAME, DEFAULT_CASSANDRA_USERNAME);
        CASSANDRA_PASSWORD =
                properties.getProperty(PROP_CASSANDRA_PASSWORD, DEFAULT_CASSANDRA_PASSWORD);
        CASSANDRA_KEYSPACE =
                properties.getProperty(PROP_CASSANDRA_KEYSPACE, DEFAULT_CASSANDRA_KEYSPACE);
        CASSANDRA_TABLE =
                properties.getProperty(PROP_CASSANDRA_TABLE, DEFAULT_CASSANDRA_TABLE);
        CASSANDRA_CONSISTENCY_LEVEL =
                properties.getProperty(PROP_CASSANDRA_CONSISTENCY_LEVEL, DEFAULT_CASSANDRA_CONSISTENCY_LEVEL);
        CASSANDRA_TRANSFORMER_CLASS =
                properties.getProperty(PROP_CASSANDRA_TRANSFORMER_CLASS, DEFAULT_CASSANDRA_TRANSFORMER_CLASS);
        CASSANDRA_EMITTER_CLASS =
                properties.getProperty(PROP_CASSANDRA_EMITTER_CLASS, DEFAULT_CASSANDRA_EMITTER_CLASS);
    }

    private int getIntegerProperty(String property, int defaultValue, Properties properties) {
        String propertyValue = properties.getProperty(property, Integer.toString(defaultValue));
        try {
            return Integer.parseInt(propertyValue.trim());
        } catch (NumberFormatException e) {
            LOG.error(e);
            return defaultValue;
        }
    }
}

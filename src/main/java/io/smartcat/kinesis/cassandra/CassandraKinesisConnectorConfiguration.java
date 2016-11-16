package io.smartcat.kinesis.cassandra;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.datastax.driver.core.ConsistencyLevel;

/**
 * This class is an extension of {@link KinesisConnectorConfiguration} that adds constants used
 * to configure Kinesis Cassandra connector.
 *
 * @see KinesisConnectorConfiguration
 */
public class CassandraKinesisConnectorConfiguration extends KinesisConnectorConfiguration {

    private static final Log LOG = LogFactory.getLog(CassandraKinesisConnectorConfiguration.class);

    /**
     * FQDN of a Java class that implements {@link javax.xml.transform.Transformer}. It transforms input Kinesis record
     * (<code>byte[]</code>) into a list of {@link CassandraRecord}. The default value is
     * {@link io.smartcat.kinesis.cassandra.JsonCassandraTransformer}.
     */
    public final String cassandraTransformerClass;

    /**
     * FQDN of a Java class that implements {@link CassandraEmitter}. It is responsible for storing
     * {@link CassandraRecord} instances into Cassandra.
     */
    public final String cassandraEmitterClass;

    /**
     * List of Cassandra cluster nodes' IP addresses (comma separated list).
     */
    public final String cassandraContactPoints;

    /**
     * Cassandra native CQL port.
     */
    public final int cassandraPort;

    /**
     * Cassandra authentication username.
     */
    public final String cassandraUsername;

    /**
     * Cassandra authentication password.
     */
    public final String cassandraPassword;

    /**
     * Cassandra keyspace to use.
     */
    public final String cassandraKeyspace;

    /**
     * Cassandra table name to use as the target.
     */
    public final String cassandraTable;

    /**
     * Cassandra write consistency level.
     */
    public final ConsistencyLevel cassandraConsistencyLevel;

    /**
     * Configuration constructor.
     *
     * @param properties          user-defined properties
     * @param credentialsProvider AWS credentials provider
     * @see {@link KinesisConnectorConfiguration}
     */
    public CassandraKinesisConnectorConfiguration(Properties properties, AWSCredentialsProvider credentialsProvider) {
        super(properties, credentialsProvider);

        cassandraContactPoints = properties.getProperty(ConfigurationOptions.PROP_CASSANDRA_CONTACT_POINTS,
                ConfigurationOptions.DEFAULT_CASSANDRA_CONTACT_POINTS);
        cassandraPort = getIntegerProperty(ConfigurationOptions.PROP_CASSANDRA_PORT,
                ConfigurationOptions.DEFAULT_CASSANDRA_PORT, properties);
        cassandraUsername = properties.getProperty(ConfigurationOptions.PROP_CASSANDRA_USERNAME,
                ConfigurationOptions.DEFAULT_CASSANDRA_USERNAME);
        cassandraPassword = properties.getProperty(ConfigurationOptions.PROP_CASSANDRA_PASSWORD,
                ConfigurationOptions.DEFAULT_CASSANDRA_PASSWORD);
        cassandraKeyspace = properties.getProperty(ConfigurationOptions.PROP_CASSANDRA_KEYSPACE,
                ConfigurationOptions.DEFAULT_CASSANDRA_KEYSPACE);
        cassandraTable = properties
                .getProperty(ConfigurationOptions.PROP_CASSANDRA_TABLE, ConfigurationOptions.DEFAULT_CASSANDRA_TABLE);
        cassandraConsistencyLevel = ConsistencyLevel.valueOf(properties
                .getProperty(ConfigurationOptions.PROP_CASSANDRA_CONSISTENCY_LEVEL,
                        ConfigurationOptions.DEFAULT_CASSANDRA_CONSISTENCY_LEVEL));
        cassandraTransformerClass = properties.getProperty(ConfigurationOptions.PROP_CASSANDRA_TRANSFORMER_CLASS,
                ConfigurationOptions.DEFAULT_CASSANDRA_TRANSFORMER_CLASS);
        cassandraEmitterClass = properties.getProperty(ConfigurationOptions.PROP_CASSANDRA_EMITTER_CLASS,
                ConfigurationOptions.DEFAULT_CASSANDRA_EMITTER_CLASS);
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

    /**
     * Cassandra kinesis connector configuration options containing property keys and defaults.
     */
    public static class ConfigurationOptions {
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
         * Default value for {@link CassandraKinesisConnectorConfiguration#cassandraTransformerClass}.
         */
        public static final String DEFAULT_CASSANDRA_TRANSFORMER_CLASS = JsonCassandraTransformer.class.getName();

        /**
         * Default value for {@link CassandraKinesisConnectorConfiguration#cassandraEmitterClass}.
         */
        public static final String DEFAULT_CASSANDRA_EMITTER_CLASS = DefaultCassandraEmitter.class.getName();

        /**
         * Default value for {@link CassandraKinesisConnectorConfiguration#cassandraContactPoints}.
         */
        public static final String DEFAULT_CASSANDRA_CONTACT_POINTS = "127.0.0.1";

        /**
         * Default value for {@link CassandraKinesisConnectorConfiguration#cassandraPort}.
         */
        public static final int DEFAULT_CASSANDRA_PORT = 9042;

        /**
         * Default value for {@link CassandraKinesisConnectorConfiguration#cassandraUsername}.
         */
        public static final String DEFAULT_CASSANDRA_USERNAME = null;

        /**
         * Default value for {@link CassandraKinesisConnectorConfiguration#cassandraPassword}.
         */
        public static final String DEFAULT_CASSANDRA_PASSWORD = null;

        /**
         * Default value for {@link CassandraKinesisConnectorConfiguration#cassandraKeyspace}.
         */
        public static final String DEFAULT_CASSANDRA_KEYSPACE = "test";

        /**
         * Default value for {@link CassandraKinesisConnectorConfiguration#cassandraTable}.
         */
        public static final String DEFAULT_CASSANDRA_TABLE = "kinesis";

        /**
         * Default value for {@link CassandraKinesisConnectorConfiguration#cassandraConsistencyLevel}.
         */
        public static final String DEFAULT_CASSANDRA_CONSISTENCY_LEVEL = "ONE";
    }

}

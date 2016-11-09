package io.smartcat.kinesis.cassandra;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

/**
 * Cassandra Kinesis connector based on {@link CassandraKinesisConnectorExecutor}. Connector properties
 * are load from <code>cassandra-kinesis-connector.properties</code> if present. AWS
 * credentials are provided using {@link DefaultAWSCredentialsProviderChain}.
 */
public class CassandraKinesisConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraKinesisConnector.class);

    private static final String PROPERTIES = "cassandra-kinesis-connector.properties";

    private CassandraKinesisConnector() {
        // prevent class instantiation
    }

    /**
     * CassandraKinesisConnector entry point.
     *
     * @param args command line parameters
     */
    public static void main(String[] args) {
        LOGGER.info("Starting Cassandra Kinesis connector.");

        Properties props = new Properties();
        try {
            InputStream in = new FileInputStream(PROPERTIES);
            props.load(in);
        } catch (IOException e) {
            LOGGER.warn("Unable to load properties file " + PROPERTIES + ". Using default values.", e);
        }

        CassandraKinesisConnectorConfiguration conf = new CassandraKinesisConnectorConfiguration(props,
                new DefaultAWSCredentialsProviderChain());
        CassandraKinesisConnectorExecutor executor = new CassandraKinesisConnectorExecutor(conf);

        (new Thread(executor)).start();
    }
}

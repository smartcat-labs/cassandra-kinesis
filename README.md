# Cassandra Kinesis Connector

Cassandra connector for AWS Kinesis that allows Kinesis records to be stored into Cassandra.  

## Introduction

Cassandra Kinesis connector is based on [Amazon Kinesis Connector Library](https://github.com/awslabs/amazon-kinesis-connectors) project. It extends the library with Cassandra-specific transformer and emitter (`ITransformer` and `IEmitter`, that is, `CassandraTransformer` and `CassandraEmitter`) and provides basic implementations for those. It also includes a connector executor implementation that allows the connector to be run as a stand-alone Java process.

The provided default implementations of `CassandraTransformer` and `CassandraEmitter` are [JsonCassandraTransformer](src/main/java/io/smartcat/kinesis/cassandra/JsonCassandraTransformer.java) and [DefaultCassandraEmitter](src/main/java/io/smartcat/kinesis/cassandra/DefaultCassandraEmitter.java).

_JsonCassandraTransformer_ interprets the input records from Kinesis stream as JSON strings and converts them into _CassandraRecord_ instances. Every JSON top-level property is treated as a Cassandra table column. Only JSON properties of type string, boolean or number are processed. The rest is silently ignored.

_DefaultCassandraEmitter_ stores _CassandraRecord_ instances (produced by _JsonCassandraTransformer_) into the respective Cassandra table (specified by the configuration).
 
## Configuration

Cassandra Kinesis Connector extends Amazon Kinesis Connector Library's [configuration](https://github.com/awslabs/amazon-kinesis-connectors/blob/master/src/main/java/com/amazonaws/services/kinesis/connectors/KinesisConnectorConfiguration.java) and adds additional [Cassandra-specific properties](src/main/java/io/smartcat/kinesis/cassandra/CassandraKinesisConnectorConfiguration.java).

All default property values can be replaced using an external properties file - `cassandra-kinesis-connector.properties`.

## Usage

Cassandra Kinesis Conector can be either embedded as a library into an application or used as a out-of-the-box connector application.

For embedding, see [_CassandraKinesisConnectorExecutor_](src/main/java/io/smartcat/kinesis/cassandra/CassandraKinesisConnectorExecutor.java) implementation.

As a stand-alone, out-of-the-box connector application, Cassandra Kinesis Connector executable JAR (`cassandra-kinesis-VERSION-all.jar`) should be used. When started, the connector tries to load external properties file `cassandra-kinesis-connector.properties` from the working directory.

## License and development

Cassandra Kinesis is licensed under the liberal and business-friendly [Apache Licence, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) and is freely available on GitHub. Cassandra Kinesis is further released to the repositories of Maven Central and on JCenter. The project is built using [Maven](http://maven.apache.org/). From your shell, cloning and building the project would go something like this:

```
git clone https://github.com/smartcat-labs/cassandra-kinesis.git
cd cassandra-kinesis
mvn package
```
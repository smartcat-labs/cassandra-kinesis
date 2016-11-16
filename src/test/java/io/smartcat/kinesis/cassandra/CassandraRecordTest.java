package io.smartcat.kinesis.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.datastax.driver.core.Statement;

public class CassandraRecordTest {

    @Test
    public void test_toCqlStatement() {
        CassandraRecord record = new CassandraRecord("spacekey", "sink");
        record.setValue("id", "123");
        record.setValue("time", 123456);
        record.setValue("value", 4.5);

        Statement cql = record.toCqlStatement();
        assertThat(cql.toString()).isEqualTo("INSERT INTO spacekey.sink (id,time,value) VALUES ('123',123456,4.5);");
    }

}

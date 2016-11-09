package io.smartcat.kinesis.cassandra;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;

public class CassandraRecordTest {

    @Test
    public void test_toCqlStatement() {
        CassandraRecord record = new CassandraRecord("spacekey", "sink");
        record.setValue("id", "123");
        record.setValue("time", 123456);
        record.setValue("value", 4.5);

        String cql = record.toCqlStatement();
        assertThat(cql).isEqualTo("INSERT INTO spacekey.sink (id,time,value) VALUES ('123',123456,4.5)");
    }

}

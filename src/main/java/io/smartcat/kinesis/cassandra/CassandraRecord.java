package io.smartcat.kinesis.cassandra;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a data container for a single Cassandra insert record.
 * It consists of a map with column names and respective values for
 * the specified table and keyspace.
 */
public class CassandraRecord {
    private String keyspace;
    private String table;
    private Map<String, Object> values;

    /**
     * @return the keyspace
     */
    public String getKeyspace() {
        return keyspace;
    }

    /**
     * @param keyspace the keyspace to set
     */
    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    /**
     * @return the table
     */
    public String getTable() {
        return table;
    }

    /**
     * @param table the table to set
     */
    public void setTable(String table) {
        this.table = table;
    }

    /**
     * Returns value for the given name.
     * @param name the column name
     * @return the respective value or null
     */
    public Object getValue(String name) {
        return values.get(name);
    }

    /**
     * Sets a value for the given column name.
     * @param name the column name
     * @param value the value to set
     */
    public void setValue(String name, Object value) {
        this.values.put(name, value);
    }

    /**
     * @return the values
     */
    public Map<String, Object> getValues() {
        return values;
    }

    /**
     * @param values the values to set
     */
    public void setValues(Map<String, Object> values) {
        this.values = values;
    }

    /**
     * Constructor.
     * @param keyspace the keyspace name
     * @param table the table name
     */
    public CassandraRecord(String keyspace, String table) {
        this.keyspace = keyspace;
        this.table = table;
        this.values = new HashMap<>();
    }

    @Override
    public String toString() {
        return keyspace + "." + table + ":" + values.toString();
    }

    /**
     * Generates a CQL INSERT statement for this record.
     * @return the CQL INSERT statement
     */
    public String toCqlStatement() {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(keyspace);
        sb.append('.');
        sb.append(table);

        boolean first = true;
        for (String key : values.keySet()) {
            if (first) {
                first = false;
                sb.append(" (");
            } else {
                sb.append(',');
            }
            sb.append(key);
        }

        sb.append(") VALUES ");
        first = true;
        for (String key : values.keySet()) {
            if (first) {
                first = false;
                sb.append("(");
            } else {
                sb.append(',');
            }
            Object value = values.get(key);
            if (value instanceof String) {
                sb.append("'");
                sb.append(value);
                sb.append("'");
            } else {
                sb.append(value);
            }
        }
        sb.append(")");

        return sb.toString();
    }
}

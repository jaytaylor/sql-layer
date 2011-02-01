package com.akiban.cserver.service.d_l;

public interface DStarLMXBean {
    String getUsingSchema();
    void setUsingSchema(String schema);

    void createTable(String schema, String ddl);
    void createTable(String ddl);

    void writeRow(String schema, String table, String fields);
    void writeRow(String table, String fields);
}

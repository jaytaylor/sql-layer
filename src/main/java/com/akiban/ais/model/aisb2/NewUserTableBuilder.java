package com.akiban.ais.model.aisb2;

public interface NewUserTableBuilder {
    /**
     * Joins this table to another one, using the default schema
     * @param table the table to join to
     * @return a builder that will create the new join
     */
    NewAkibanJoinBuilder joinTo(String table);

    /**
     * Joins this table to another one.
     * @param schema the schema of the table to join to
     * @param table the table name of the table to join to
     * @return a builder that will create the new join
     */
    NewAkibanJoinBuilder joinTo(String schema, String table);

    NewUserTableBuilder colLong(String name);
    NewUserTableBuilder colString(String name, int length);

    NewUserTableBuilder pk(String... columns);
    NewUserTableBuilder uniqueKey(String indexName, String... columns);
    NewUserTableBuilder key(String indexName, String... columns);
}

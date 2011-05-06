package com.akiban.ais.model.aisb2;

public interface NewUserTableBuilder {
    NewAkibanJoinBuilder joinTo(String table);
    NewAkibanJoinBuilder joinTo(String schema, String table);

    NewUserTableBuilder colLong(String name);
    NewUserTableBuilder colString(String name, int length);

    NewUserTableBuilder pk(String... columns);
    NewUserTableBuilder uniqueKey(String indexName, String... columns);
    NewUserTableBuilder key(String indexName, String... columns);
}

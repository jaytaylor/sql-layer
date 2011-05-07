package com.akiban.ais.model.aisb2;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;

public class AISBBasedBuilder
    implements NewAISBuilder, NewUserTableBuilder, NewAkibanJoinBuilder
{
    // NewAISBuilder interface

    @Override
    public AkibanInformationSchema ais() {
        return aisb.akibanInformationSchema();
    }

    @Override
    public NewAISBuilder defaultSchema(String schema) {
        this.schema = schema;
        return this;
    }

    @Override
    public NewUserTableBuilder userTable(String table) {
        return userTable(schema, table); // TODO
    }

    @Override
    public NewUserTableBuilder userTable(String schema, String table) {
        return this; // TODO
    }

    // NewuserTableBuilder interface

    @Override
    public NewAkibanJoinBuilder joinTo(String table) {
        return joinTo(schema, table);
    }

    @Override
    public NewAkibanJoinBuilder joinTo(String schema, String table) {
        return this; // TODO
    }

    @Override
    public NewUserTableBuilder colLong(String name) {
        return this; // TODO
    }

    @Override
    public NewUserTableBuilder colString(String name, int length) {
        return this; // TODO
    }

    @Override
    public NewUserTableBuilder pk(String... columns) {
        return this; // TODO
    }

    @Override
    public NewUserTableBuilder uniqueKey(String indexName, String... columns) {
        return this; // TODO
    }

    @Override
    public NewUserTableBuilder key(String indexName, String... columns) {
        return this; // TODO
    }

    // NewAkibanJoinBuilder

    @Override
    public NewAkibanJoinBuilder on(String childColumn, String parentColumn) {
        return this; // TODO
    }

    @Override
    public NewAkibanJoinBuilder and(String childColumn, String parentColumn) {
        return this; // TODO
    }

    // AISBBasedBuilder interface

    public AISBBasedBuilder() {
        aisb = new AISBuilder();
    }

    // object state

    private final AISBuilder aisb;
    private String schema;
}

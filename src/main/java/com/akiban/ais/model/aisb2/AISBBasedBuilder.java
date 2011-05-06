package com.akiban.ais.model.aisb2;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;

public class AISBBasedBuilder
    implements NewAISBuilder, NewUserTableBuilder, NewAkibanJoinBuilder
{
    // NewAISBuilder interface

    @Override
    public AkibanInformationSchema ais() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NewAISBuilder defaultSchema(String schema) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NewUserTableBuilder userTable(String table) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NewUserTableBuilder userTable(String schema, String table) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    // NewuserTableBuilder interface

    @Override
    public NewAkibanJoinBuilder joinTo(String table) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NewAkibanJoinBuilder joinTo(String schema, String table) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NewUserTableBuilder colLong(String name) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NewUserTableBuilder colString(String name, int length) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NewUserTableBuilder pk(String... columns) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NewUserTableBuilder uniqueKey(String indexName, String... columns) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NewUserTableBuilder key(String indexName, String... columns) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    // NewAkibanJoinBuilder

    @Override
    public NewAkibanJoinBuilder on(String childColumn, String parentColumn) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NewAkibanJoinBuilder and(String childColumn, String parentColumn) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    // AISBBasedBuilder interface

    public AISBBasedBuilder() {
        aisb = new AISBuilder();
    }

    // object state

    private final AISBuilder aisb;
}

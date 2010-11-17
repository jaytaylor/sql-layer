package com.akiban.cserver.api.common;

import com.akiban.ais.model.TableName;
import com.akiban.cserver.api.dml.NoSuchTableException;

public final class IdResolverStub implements IdResolver {
    private static final IdResolverStub instance = new IdResolverStub();

    public static IdResolverStub instance() {
        return instance;
    }

    private IdResolverStub() {

    }

    @Override
    public int tableId(TableName tableName) throws NoSuchTableException {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableName tableName(int id) throws NoSuchTableException {
        throw new UnsupportedOperationException();
    }
}

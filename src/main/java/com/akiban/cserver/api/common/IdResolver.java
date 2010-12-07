package com.akiban.cserver.api.common;

import com.akiban.ais.model.TableName;
import com.akiban.cserver.RowDef;

public interface IdResolver {
    int tableId(TableName tableName) throws NoSuchTableException;

    TableName tableName(int id) throws NoSuchTableException;

    RowDef getRowDef(TableId id) throws NoSuchTableException;
}

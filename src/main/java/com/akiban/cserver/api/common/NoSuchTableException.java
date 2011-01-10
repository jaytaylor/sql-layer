package com.akiban.cserver.api.common;

import com.akiban.ais.model.TableName;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.dml.DMLException;
import com.akiban.cserver.util.RowDefNotFoundException;
import com.akiban.message.ErrorCode;

public final class NoSuchTableException extends DMLException {
    public NoSuchTableException(InvalidOperationException e) {
        super(e);
    }

    public NoSuchTableException(TableId tableId, RowDefNotFoundException e) {
        super(ErrorCode.NO_SUCH_TABLE, "TableId not found: " + tableId, e);
    }

    public NoSuchTableException(int id) {
        super(ErrorCode.NO_SUCH_TABLE, "No table with id %d", id);
    }

    public NoSuchTableException(TableName name) {
        super(ErrorCode.NO_SUCH_TABLE, "No table with name %s", name);
    }
}

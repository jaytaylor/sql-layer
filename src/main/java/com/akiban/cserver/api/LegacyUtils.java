package com.akiban.cserver.api;

import com.akiban.cserver.RowData;
import com.akiban.cserver.api.dml.TableDefinitionMismatchException;

public final class LegacyUtils {
    public static Integer matchRowDatas(RowData one, RowData two) throws TableDefinitionMismatchException {
        if (one == null) {
            return (two == null) ? null : two.getRowDefId();
        }
        if (two == null) {
            return one.getRowDefId();
        }
        if (one.getRowDefId() == two.getRowDefId()) {
            return one.getRowDefId();
        }
        throw new TableDefinitionMismatchException("Mismatched table ids: %d != %d",
                one.getRowDefId(), two.getRowDefId());
    }
}

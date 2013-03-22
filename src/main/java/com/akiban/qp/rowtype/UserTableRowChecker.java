
package com.akiban.qp.rowtype;

import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.Row;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.NotNullViolationException;

import java.util.BitSet;

public class UserTableRowChecker implements ConstraintChecker
{
    @Override
    public void checkConstraints(Row row, boolean usePValues) throws InvalidOperationException
    {
        for (int f = 0; f < fields; f++) {
            if (notNull.get(f) && isNull(row, f, usePValues)) {
                // if this column is an identity column, null is allowed
                // 
                if (f == identityColumn) {
                    continue;
                }
                TableName tableName = table.getName();
                throw new NotNullViolationException(tableName.getSchemaName(),
                                                    tableName.getTableName(),
                                                    table.getColumnsIncludingInternal().get(f).getName());
            }
        }
    }

    private boolean isNull(Row row, int f, boolean usePValues) {
        return usePValues
                ? row.pvalue(f).isNull()
                : row.eval(f).isNull();
    }

    public UserTableRowChecker(RowType rowType)
    {
        assert rowType.hasUserTable() : rowType;
        fields = rowType.nFields();
        table = rowType.userTable();
        notNull = table.notNull();
        identityColumn = table.getIdentityColumn() != null ? table.getIdentityColumn().getPosition().intValue() : -1;
    }

    public UserTableRowChecker(UserTable userTable)
    {
        fields = userTable.getColumnsIncludingInternal().size();
        table = userTable;
        notNull = table.notNull();
        identityColumn = table.getIdentityColumn() != null ? table.getIdentityColumn().getPosition().intValue() : -1;
    }

    private final int identityColumn;
    private final int fields;
    private final UserTable table;
    private final BitSet notNull;
}

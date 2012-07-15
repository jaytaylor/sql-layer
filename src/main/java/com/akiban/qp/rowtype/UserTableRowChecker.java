/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
    public void checkConstraints(Row row) throws InvalidOperationException
    {
        for (int f = 0; f < fields; f++) {
            if (notNull.get(f) && row.eval(f).isNull()) {
                // if this column is an identity column, null is allowed
                // 
                //if (f == identityColumn) {
                //    continue;
                //}
                TableName tableName = table.getName();
                throw new NotNullViolationException(tableName.getSchemaName(),
                                                    tableName.getTableName(),
                                                    table.getColumnsIncludingInternal().get(f).getName());
            }
        }

    }

    public UserTableRowChecker(RowType rowType)
    {
        assert rowType.hasUserTable() : rowType;
        fields = rowType.nFields();
        table = rowType.userTable();
        notNull = table.notNull();
        identityColumn = table.getIdentityColumn() != null ? table.getIdentityColumn().getPosition().intValue() : -1;
        
    }

    private final int identityColumn;
    private final int fields;
    private final UserTable table;
    private final BitSet notNull;
}

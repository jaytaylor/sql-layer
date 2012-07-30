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

package com.akiban.sql.pg;

import com.akiban.server.service.dxl.DXLFunctionsHook;
import com.akiban.server.service.dxl.DXLReadWriteLockHook;
import com.akiban.server.service.session.Session;
import com.akiban.util.tap.InOutTap;

import java.io.IOException;
import java.util.List;

/**
 * An ordinary SQL statement.
 */
public abstract class PostgresBaseStatement implements PostgresStatement
{
    private List<String> columnNames;
    private List<PostgresType> columnTypes;
    private PostgresType[] parameterTypes;
    private boolean usesPValues;

    protected PostgresBaseStatement(PostgresType[] parameterTypes, boolean usesPValues) {
        this.parameterTypes = parameterTypes;
        this.usesPValues = usesPValues;
    }

    protected PostgresBaseStatement(List<String> columnNames, 
                                    List<PostgresType> columnTypes,
                                    PostgresType[] parameterTypes,
                                    boolean usesPValues) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.parameterTypes = parameterTypes;
        this.usesPValues = usesPValues;
    }

    public boolean usesPValues() {
        return usesPValues;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<PostgresType> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public PostgresType[] getParameterTypes() {
        return parameterTypes;
    }

    @Override
    public void sendDescription(PostgresQueryContext context, boolean always) 
            throws IOException {
        PostgresServerSession server = context.getServer();
        PostgresMessenger messenger = server.getMessenger();
        List<PostgresType> columnTypes = getColumnTypes();
        if (columnTypes == null) {
            if (!always) return;
            messenger.beginMessage(PostgresMessages.NO_DATA_TYPE.code());
        }
        else {
            messenger.beginMessage(PostgresMessages.ROW_DESCRIPTION_TYPE.code());
            List<String> columnNames = getColumnNames();
            int ncols = columnTypes.size();
            messenger.writeShort(ncols);
            for (int i = 0; i < ncols; i++) {
                PostgresType type = columnTypes.get(i);
                messenger.writeString(columnNames.get(i)); // attname
                messenger.writeInt(0);    // attrelid
                messenger.writeShort(0);  // attnum
                messenger.writeInt(type.getOid()); // atttypid
                messenger.writeShort(type.getLength()); // attlen
                messenger.writeInt(type.getModifier()); // atttypmod
                messenger.writeShort(context.isColumnBinary(i) ? 1 : 0);
            }
        }
        messenger.sendMessage();
    }

    protected abstract InOutTap executeTap();
    protected abstract InOutTap acquireLockTap();

    protected void lock(Session session, DXLFunctionsHook.DXLFunction operationType)
    {
        acquireLockTap().in();
        executeTap().in();
        try {
            DXLReadWriteLockHook.only().hookFunctionIn(session, operationType);
        } finally {
            acquireLockTap().out();
        }
    }

    protected void unlock(Session session, DXLFunctionsHook.DXLFunction operationType)
    {
        DXLReadWriteLockHook.only().hookFunctionFinally(session, operationType, null);
        executeTap().out();
    }
}

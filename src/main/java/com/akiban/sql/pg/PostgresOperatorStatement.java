/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.pg;

import com.akiban.server.service.session.Session;
import com.akiban.qp.operator.*;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.util.Tap;

import static com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction.*;

import java.util.*;
import java.io.IOException;

/**
 * An SQL SELECT transformed into an operator tree
 * @see PostgresOperatorCompiler
 */
public class PostgresOperatorStatement extends PostgresBaseStatement
{
    private Operator resultOperator;
    private RowType resultRowType;

    private static final Tap.InOutTap EXECUTE_TAP = Tap.createTimer("PostgresOperatorStatement: execute shared");
    private static final Tap.InOutTap ACQUIRE_LOCK_TAP = Tap.createTimer("PostgresOperatorStatement: acquire shared lock");

    public PostgresOperatorStatement(Operator resultOperator,
                                     RowType resultRowType,
                                     List<String> columnNames,
                                     List<PostgresType> columnTypes,
                                     PostgresType[] parameterTypes) {
        super(columnNames, columnTypes, parameterTypes);
        this.resultOperator = resultOperator;
        this.resultRowType = resultRowType;
    }
    
    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.READ;
    }

    @Override
    public int execute(PostgresQueryContext context, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        PostgresMessenger messenger = server.getMessenger();
        Session session = server.getSession();
        int nrows = 0;
        Cursor cursor = null;
        try {
            lock(session, UNSPECIFIED_DML_READ);
            cursor = API.cursor(resultOperator, context);
            cursor.open();
            PostgresRowOutputter outputter = new PostgresRowOutputter(messenger, context, this);
            Row row;
            while ((row = cursor.next()) != null) {
                assert resultRowType == null || (row.rowType() == resultRowType) : row;
                outputter.output(row);
                nrows++;
                if ((maxrows > 0) && (nrows >= maxrows))
                    break;
            }
        }
        finally {
            if (cursor != null) {
                cursor.close();
            }
            unlock(session, UNSPECIFIED_DML_READ);
        }
        {        
            messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
            messenger.writeString("SELECT " + nrows);
            messenger.sendMessage();
        }
        return nrows;
    }

    @Override
    protected Tap.InOutTap executeTap()
    {
        return EXECUTE_TAP;
    }

    @Override
    protected Tap.InOutTap acquireLockTap()
    {
        return ACQUIRE_LOCK_TAP;
    }

}

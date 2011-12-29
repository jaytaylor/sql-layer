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
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.util.Tap;

import com.akiban.server.expression.EnvironmentExpressionSetting;

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
                                     PostgresType[] parameterTypes,
                                     List<EnvironmentExpressionSetting> environmentSettings) {
        super(columnNames, columnTypes, parameterTypes, environmentSettings);
        this.resultOperator = resultOperator;
        this.resultRowType = resultRowType;
    }
    
    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.READ;
    }

    @Override
    public int execute(PostgresServerSession server, int maxrows)
        throws IOException {
        PostgresMessenger messenger = server.getMessenger();
        Bindings bindings = getBindings();
        Session session = server.getSession();
        int nrows = 0;
        Cursor cursor = null;
        try {
            lock(session, UNSPECIFIED_DML_READ);
            setEnvironmentBindings(server, bindings);
            cursor = API.cursor(resultOperator, server.getStore());
            cursor.open(bindings);
            PostgresOutputter<Row> outputter = getRowOutputter(messenger);
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

    protected PostgresOutputter<Row> getRowOutputter(PostgresMessenger messenger) {
        return new PostgresRowOutputter(messenger, this);
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

    /** Only needed in the case where a statement has parameters or the client
     * specifies that some results should be in binary. */
    static class BoundStatement extends PostgresOperatorStatement {
        private Bindings bindings;
        private int nparams;
        private boolean[] columnBinary; // Is this column binary format?
        private boolean defaultColumnBinary;

        public BoundStatement(Operator resultOperator,
                              RowType resultRowType,
                              List<String> columnNames,
                              List<PostgresType> columnTypes,
                              Bindings bindings, int nparams,
                              boolean[] columnBinary, boolean defaultColumnBinary,
                              List<EnvironmentExpressionSetting> environmentSettings) {
            super(resultOperator, resultRowType, columnNames, columnTypes, 
                  null, environmentSettings);
            this.bindings = bindings;
            this.nparams = nparams;
            this.columnBinary = columnBinary;
            this.defaultColumnBinary = defaultColumnBinary;
        }

        @Override
        public Bindings getBindings() {
            return bindings;
        }

        @Override
        protected int getNParameters() {
            return nparams;
        }

        @Override
        public boolean isColumnBinary(int i) {
            if ((columnBinary != null) && (i < columnBinary.length))
                return columnBinary[i];
            else
                return defaultColumnBinary;
        }
    }

    /** Get a bound version of a predicate by applying given parameters
     * and requested result formats. */
    @Override
    public PostgresStatement getBoundStatement(Object[] parameters,
                                               boolean[] columnBinary, 
                                               boolean defaultColumnBinary)  {
        if ((parameters == null) && 
            (columnBinary == null) && (defaultColumnBinary == false))
            return this;        // Can be reused.

        Bindings bindings = getBindings();
        int nparams = getNParameters();
        if (parameters != null) {
            bindings = getParameterBindings(parameters);
            nparams = parameters.length;
        }
        return new BoundStatement(resultOperator, resultRowType,
                                  getColumnNames(), getColumnTypes(),
                                  bindings, nparams,
                                  columnBinary, defaultColumnBinary,
                                  getEnvironmentSettings());
    }
}

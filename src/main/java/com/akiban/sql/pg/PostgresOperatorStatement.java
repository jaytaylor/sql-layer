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
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.util.Tap;

import static com.akiban.server.expression.std.EnvironmentExpression.EnvironmentValue;

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
    private int offset = 0;
    private int limit = -1;

    private static final Tap.InOutTap EXECUTE_TAP = Tap.createTimer("PostgresOperatorStatement: execute shared");
    private static final Tap.InOutTap ACQUIRE_LOCK_TAP = Tap.createTimer("PostgresOperatorStatement: acquire shared lock");

    public PostgresOperatorStatement(Operator resultOperator,
                                     RowType resultRowType,
                                     List<String> columnNames,
                                     List<PostgresType> columnTypes,
                                     PostgresType[] parameterTypes,
                                     List<EnvironmentValue> environmentValues,
                                     int offset,
                                     int limit) {
        super(columnNames, columnTypes, parameterTypes, environmentValues);
        this.resultOperator = resultOperator;
        this.resultRowType = resultRowType;
        this.offset = offset;
        this.limit = limit;
    }
    
    public int execute(PostgresServerSession server, int maxrows)
        throws IOException {
        PostgresMessenger messenger = server.getMessenger();
        Bindings bindings = getBindings();
        Session session = server.getSession();
        int nskip = offset;
        if (limit > 0) {
            if ((maxrows <= 0) || (maxrows > limit))
                maxrows = limit;
        }
        int nrows = 0;
        Cursor cursor = null;
        try {
            lock(session, UNSPECIFIED_DML_READ);
            setEnvironmentBindings(server, bindings);
            cursor = API.cursor(resultOperator, server.getStore());
            cursor.open(bindings);
            List<PostgresType> columnTypes = getColumnTypes();
            int ncols = columnTypes.size();
            Row row;
            ToObjectValueTarget target = new ToObjectValueTarget();
            while ((row = cursor.next()) != null) {
                assert resultRowType == null || (row.rowType() == resultRowType) : row;
                if (nskip > 0) {
                    nskip--;
                    continue;
                }
                messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
                messenger.writeShort(ncols);
                for (int i = 0; i < ncols; i++) {
                    Object field = target.convertFromSource(row.eval(i));
                    PostgresType type = columnTypes.get(i);
                    byte[] value = type.encodeValue(field,
                                                    messenger.getEncoding(),
                                                    isColumnBinary(i));
                    if (value == null) {
                        messenger.writeInt(-1);
                    }
                    else {
                        messenger.writeInt(value.length);
                        messenger.write(value);
                    }
                }
                messenger.sendMessage();
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
                              int offset, int limit,
                              Bindings bindings, int nparams,
                              boolean[] columnBinary, boolean defaultColumnBinary,
                              List<EnvironmentValue> environmentValues) {
            super(resultOperator, resultRowType, columnNames, columnTypes, 
                  null, environmentValues, offset, limit);
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
    public PostgresStatement getBoundStatement(String[] parameters,
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
                                  offset, limit, bindings, nparams,
                                  columnBinary, defaultColumnBinary,
                                  getEnvironmentValues());
    }
}

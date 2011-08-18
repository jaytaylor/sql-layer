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

import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.sql.StandardException;

import com.akiban.qp.physicaloperator.API;
import com.akiban.qp.physicaloperator.BindingNotSetException;
import com.akiban.qp.physicaloperator.Bindings;
import com.akiban.qp.physicaloperator.Cursor;
import com.akiban.qp.physicaloperator.IncompatibleRowException;
import com.akiban.qp.physicaloperator.PhysicalOperator;
import com.akiban.qp.physicaloperator.StoreAdapterRuntimeException;
import com.akiban.qp.physicaloperator.UndefBindings;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;

import java.util.*;
import java.io.IOException;

/**
 * An SQL SELECT transformed into an operator tree
 * @see PostgresOperatorCompiler
 */
public class PostgresOperatorStatement extends PostgresBaseStatement
{
    private PhysicalOperator resultOperator;
    private int offset = 0;
    private int limit = -1;
        
    public PostgresOperatorStatement(PhysicalOperator resultOperator,
                                     List<String> columnNames,
                                     List<PostgresType> columnTypes,
                                     PostgresType[] parameterTypes,
                                     int offset,
                                     int limit) {
        super(columnNames, columnTypes, parameterTypes);
        this.resultOperator = resultOperator;
        this.offset = offset;
        this.limit = limit;
    }
    
    public int execute(PostgresServerSession server, int maxrows)
        throws IOException, StandardException {
        PostgresMessenger messenger = server.getMessenger();
        Bindings bindings = getBindings();
        RowType resultRowType = resultOperator.rowType();
        Cursor cursor = API.cursor(resultOperator, server.getStore());
        int nskip = offset;
        if (limit > 0) {
            if ((maxrows <= 0) || (maxrows > limit))
                maxrows = limit;
        }
        int nrows = 0;
        try {
            cursor.open(bindings);
            List<PostgresType> columnTypes = getColumnTypes();
            int ncols = columnTypes.size();
            Row row;
            ToObjectValueTarget target = new ToObjectValueTarget();
            while ((row = cursor.next()) != null) {
                assert (row.rowType() == resultRowType) : row;
                if (nskip > 0) {
                    nskip--;
                    continue;
                }
                messenger.beginMessage(PostgresMessenger.DATA_ROW_TYPE);
                messenger.writeShort(ncols);
                for (int i = 0; i < ncols; i++) {
                    Object field = target.convertFromSource(row.bindSource(i, bindings));
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
        catch (BindingNotSetException ex) {
            throw new StandardException(ex);
        }
        catch (IncompatibleRowException ex) {
            throw new StandardException(ex);
        }
        catch (StoreAdapterRuntimeException ex) {
            throw new StandardException(ex);
        }
        finally {
            cursor.close();
        }
        {        
            messenger.beginMessage(PostgresMessenger.COMMAND_COMPLETE_TYPE);
            messenger.writeString("SELECT " + nrows);
            messenger.sendMessage();
        }
        return nrows;
    }

    protected Bindings getBindings() {
        return UndefBindings.only();
    }

    /** Only needed in the case where a statement has parameters or the client
     * specifies that some results should be in binary. */
    static class BoundStatement extends PostgresOperatorStatement {
        private Bindings bindings;
        private boolean[] columnBinary; // Is this column binary format?
        private boolean defaultColumnBinary;

        public BoundStatement(PhysicalOperator resultOperator,
                              List<String> columnNames,
                              List<PostgresType> columnTypes,
                              int offset, int limit,
                              Bindings bindings,
                              boolean[] columnBinary, boolean defaultColumnBinary) {
            super(resultOperator, columnNames, columnTypes, 
                  null, offset, limit);
            this.bindings = bindings;
            this.columnBinary = columnBinary;
            this.defaultColumnBinary = defaultColumnBinary;
        }

        @Override
        public Bindings getBindings() {
            return bindings;
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
                                               boolean defaultColumnBinary) 
            throws StandardException {
        if ((parameters == null) && 
            (columnBinary == null) && (defaultColumnBinary == false))
            return this;        // Can be reused.

        Bindings bindings = getBindings();
        if (parameters != null)
            bindings = getParameterBindings(parameters);
        return new BoundStatement(resultOperator,
                                  getColumnNames(), getColumnTypes(),
                                  offset, limit, bindings, 
                                  columnBinary, defaultColumnBinary);
    }

}

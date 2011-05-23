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

import com.akiban.sql.StandardException;

import com.akiban.ais.model.Column;
import com.akiban.qp.physicaloperator.API;
import com.akiban.qp.physicaloperator.ArrayBindings;
import com.akiban.qp.physicaloperator.Bindings;
import com.akiban.qp.physicaloperator.Cursor;
import com.akiban.qp.physicaloperator.PhysicalOperator;
import com.akiban.qp.physicaloperator.StoreAdapter;
import com.akiban.qp.physicaloperator.UndefBindings;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.service.session.Session;

import java.util.*;
import java.io.IOException;

/**
 * An SQL SELECT transformed into an operator tree
 * @see PostgresOperatorCompiler
 */
public class PostgresOperatorStatement extends PostgresBaseStatement
{
    private StoreAdapter store;
    private PhysicalOperator resultOperator;
    private RowType resultRowType;
    private int[] resultColumnOffsets;
    private int offset = 0;
    private int limit = -1;
        
    public PostgresOperatorStatement(StoreAdapter store,
                                     PhysicalOperator resultOperator,
                                     RowType resultRowType,
                                     List<Column> resultColumns,
                                     int[] resultColumnOffsets,
                                     int offset,
                                     int limit) {
        super(resultColumns);
        this.store = store;
        this.resultOperator = resultOperator;
        this.resultRowType = resultRowType;
        this.resultColumnOffsets = resultColumnOffsets;
        this.offset = offset;
        this.limit = limit;
    }
    
    public void execute(PostgresServerSession server, int maxrows)
        throws IOException, StandardException {
        PostgresMessenger messenger = server.getMessenger();
        Bindings bindings = getBindings();
        Cursor cursor = API.cursor(resultOperator, store);
        int nskip = offset;
        if (limit > 0) {
            if ((maxrows <= 0) || (maxrows > limit))
                maxrows = limit;
        }
        int nrows = 0;
        try {
            cursor.open(bindings);
            List<Column> columns = getColumns();
            List<PostgresType> types = getTypes();
            int ncols = columns.size();
            while (cursor.next()) {
                Row row = cursor.currentRow();
                if (row.rowType() == resultRowType) {
                    if (nskip > 0) {
                        nskip--;
                        continue;
                    }
                    messenger.beginMessage(PostgresMessenger.DATA_ROW_TYPE);
                    messenger.writeShort(ncols);
                    for (int i = 0; i < ncols; i++) {
                        Column column = columns.get(i);
                        Object field = row.field(resultColumnOffsets[i], bindings);
                        PostgresType type = types.get(i);
                        byte[] value = type.encodeValue(field, column, 
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
        } 
        finally {
            cursor.close();
        }
        {        
            messenger.beginMessage(PostgresMessenger.COMMAND_COMPLETE_TYPE);
            messenger.writeString("SELECT " + nrows);
            messenger.sendMessage();
        }
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

        public BoundStatement(StoreAdapter store,
                              PhysicalOperator resultOperator,
                              RowType resultRowType,
                              List<Column> resultColumns,
                              int[] resultColumnOffsets,
                              int offset, int limit,
                              Bindings bindings,
                              boolean[] columnBinary, boolean defaultColumnBinary) {
            super(store, 
                  resultOperator, resultRowType, resultColumns, resultColumnOffsets,
                  offset, limit);
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
                                               boolean defaultColumnBinary) {
        if ((parameters == null) && 
            (columnBinary == null) && (defaultColumnBinary == false))
            return this;        // Can be reused.

        Bindings bindings = getBindings();
        if (parameters != null) {
            ArrayBindings ab = new ArrayBindings(parameters.length);
            for (int i = 0; i < parameters.length; i++)
                ab.set(i, parameters[i]);
            bindings = ab;
        }
        return new BoundStatement(store, resultOperator, resultRowType, 
                                  getColumns(), resultColumnOffsets,
                                  offset, limit, bindings, 
                                  columnBinary, defaultColumnBinary);
    }

}

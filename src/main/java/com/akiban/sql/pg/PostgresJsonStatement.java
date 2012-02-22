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

import static com.akiban.sql.pg.PostgresJsonCompiler.JsonResultColumn;

import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.AkType;

import java.util.*;

public class PostgresJsonStatement extends PostgresOperatorStatement
{
    private List<JsonResultColumn> resultColumns;

    public PostgresJsonStatement(Operator resultOperator, RowType resultRowType,
                                 List<JsonResultColumn> resultColumns,
                                 PostgresType[] parameterTypes) {
        super(resultOperator, resultRowType,
              // Looks like just one unlimited VARCHAR to the client.
              Collections.singletonList("JSON"),
              Collections.singletonList(new PostgresType(PostgresType.VARCHAR_TYPE_OID,
                                                         (short)-1, -1, AkType.VARCHAR)),
              parameterTypes);
        this.resultColumns = resultColumns;
    }

    @Override
    protected PostgresOutputter<Row> getRowOutputter(PostgresQueryContext context) {
        return new PostgresJsonOutputter(context, this, 
                                         resultColumns, getColumnTypes().get(0));
    }
    
}

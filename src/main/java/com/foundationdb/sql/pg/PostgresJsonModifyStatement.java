/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.akiban.sql.pg;

import com.akiban.sql.optimizer.plan.CostEstimate;
import static com.akiban.sql.pg.PostgresJsonCompiler.JsonResultColumn;
import static com.akiban.sql.pg.PostgresJsonStatement.jsonColumnNames;
import static com.akiban.sql.pg.PostgresJsonStatement.jsonColumnTypes;

import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.mcompat.mtypes.MString;

import java.util.*;

public class PostgresJsonModifyStatement extends PostgresModifyOperatorStatement
{
    private List<JsonResultColumn> resultColumns;

    public PostgresJsonModifyStatement(PostgresOperatorCompiler compiler) {
        super(compiler);
    }

    public void init(String statementType, Operator resultOperator, RowType resultRowType,
                     List<JsonResultColumn> resultColumns,
                     PostgresType[] parameterTypes,
                     CostEstimate costEstimate,
                     boolean usePValues,
                     boolean requireStepIsolation,
                     boolean putInCache) {
        super.init(statementType, resultOperator, resultRowType,
                   // Looks like just one unlimited VARCHAR to the client.
                   jsonColumnNames(), jsonColumnTypes(),
                   parameterTypes, costEstimate, usePValues, 
                   requireStepIsolation, putInCache);
        this.resultColumns = resultColumns;
    }

    @Override
    protected PostgresOutputter<Row> getRowOutputter(PostgresQueryContext context) {
        return new PostgresJsonOutputter(context, this, 
                                         resultColumns, getColumnTypes().get(0));
    }
    
}

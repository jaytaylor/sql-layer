/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.sql.optimizer.plan;

import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.format.DefaultFormatter;
import com.foundationdb.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/** Physical INSERT/UPDATE/DELETE statement */
public class PhysicalUpdate extends BasePlannable
{
    private boolean returning;
    private boolean putInCache;

    public PhysicalUpdate(Operator resultsOperator, 
                          ParameterType[] parameterTypes,
                          RowType rowType, 
                          List<PhysicalResultColumn> resultColumns,
                          boolean returning, 
                          boolean putInCache,
                          CostEstimate costEstimate,
                          Set<Table> affectedTables) {
        super (resultsOperator, parameterTypes, rowType, resultColumns, costEstimate, affectedTables);
        this.returning = returning;
        this.putInCache = putInCache;
    }
    
    public boolean isReturning() {
        return returning;
    }

    public boolean putInCache() {
        return putInCache;
    }

    @Override
    public boolean isUpdate() {
        return true;
    }

    @Override
    protected String withIndentedExplain(StringBuilder str, ExplainContext context, String defaultSchemaName, DefaultFormatter.LevelOfDetail levelOfDetail) {
        if (getParameterTypes() != null)
            str.append(Arrays.toString(getParameterTypes()));
        if (!putInCache)
            str.append("/NO_CACHE");
        return super.withIndentedExplain(str, context, defaultSchemaName, levelOfDetail);
    }

}

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

package com.foundationdb.sql.optimizer.plan;

import com.foundationdb.ais.model.UserTable;
import com.foundationdb.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.foundationdb.sql.types.DataTypeDescriptor;

import com.foundationdb.qp.exec.UpdatePlannable;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.ExplainContext;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/** Physical INSERT/UPDATE/DELETE statement */
public class PhysicalUpdate extends BasePlannable
{
    private boolean requireStepIsolation;
    private boolean returning;
    private boolean putInCache;

    public PhysicalUpdate(Operator resultsOperator, 
                          DataTypeDescriptor[] paramterTypes,
                          RowType rowType, 
                          List<PhysicalResultColumn> resultColumns,
                          boolean returning, 
                          boolean requireStepIsolation,
                          boolean putInCache,
                          CostEstimate costEstimate,
                          Set<UserTable> affectedTables) {
        super (resultsOperator, paramterTypes, rowType, resultColumns, costEstimate, affectedTables);
        this.requireStepIsolation = requireStepIsolation;
        this.returning = returning;
        this.putInCache = putInCache;
    }
    
    public UpdatePlannable getUpdatePlannable() {
        return (UpdatePlannable)getPlannable();
    }

    public boolean isRequireStepIsolation() {
        return requireStepIsolation;
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
    protected String withIndentedExplain(StringBuilder str, ExplainContext context, String defaultSchemaName) {
        if (getParameterTypes() != null)
            str.append(Arrays.toString(getParameterTypes()));
        if (requireStepIsolation)
            str.append("/STEP_ISOLATE");
        if (!putInCache)
            str.append("/NO_CACHE");
        return super.withIndentedExplain(str, context, defaultSchemaName);
    }

}

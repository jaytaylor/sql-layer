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
import com.foundationdb.qp.exec.Plannable;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.format.DefaultFormatter;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.foundationdb.sql.types.DataTypeDescriptor;

import java.util.*;

/** Physical operator plan */
public abstract class BasePlannable extends BasePlanNode
{
    public static class ParameterType {
        private final DataTypeDescriptor sqlType;
        private final TInstance type;

        public ParameterType(DataTypeDescriptor sqlType, TInstance type) {
            this.sqlType = sqlType;
            this.type = type;
        }

        public DataTypeDescriptor getSQLType() {
            return sqlType;
        }

        public TInstance getType() {
            return type;
        }

        @Override
        public String toString() {
            if (type != null)
                return type.toStringConcise(true);
            else
                return Objects.toString(sqlType);
        }
    }

    private Plannable plannable;
    private ParameterType[] parameterTypes;
    private List<PhysicalResultColumn> resultColumns;
    private RowType rowType;
    private CostEstimate costEstimate;
    private Set<Table> affectedTables;

    protected BasePlannable(Plannable plannable,
                            ParameterType[] parameterTypes,
                            RowType rowType,
                            List<PhysicalResultColumn> resultColumns,
                            CostEstimate costEstimate,
                            Set<Table> affectedTables) {
        this.plannable = plannable;
        this.parameterTypes = parameterTypes;
        this.rowType = rowType;
        this.resultColumns = resultColumns;
        this.costEstimate = costEstimate;
        this.affectedTables = affectedTables;
    }

    public Plannable getPlannable() {
        return plannable;
    }
    public ParameterType[] getParameterTypes() {
        return parameterTypes;
    }

    public RowType getResultRowType() {
        return rowType;
    }

    public List<PhysicalResultColumn> getResultColumns() {
        return resultColumns;
    }

    public CostEstimate getCostEstimate() {
        return costEstimate;
    }

    public Set<Table> getAffectedTables() {
        return affectedTables;
    }

    public abstract boolean isUpdate();

    @Override
    public boolean accept(PlanVisitor v) {
        return v.visit(this);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        // Do not copy operators.
    }
    
    public String explainToString(ExplainContext context, String defaultSchemaName, DefaultFormatter.LevelOfDetail levelOfDetail) {
        return withIndentedExplain(new StringBuilder(getClass().getSimpleName()), context, defaultSchemaName, levelOfDetail);
    }

    @Override
    public String planString(SummaryConfiguration configuration) {
        // Similar to above, but with @hash for consistency and verbose
        return withIndentedExplain(new StringBuilder(super.summaryString(configuration)), null, null, DefaultFormatter.LevelOfDetail.VERBOSE_WITHOUT_COST);
    }

    protected String withIndentedExplain(StringBuilder str, ExplainContext context, String defaultSchemaName, DefaultFormatter.LevelOfDetail levelOfDetail) {
        if (context == null)
            context = new ExplainContext(); // Empty
        DefaultFormatter f = new DefaultFormatter(defaultSchemaName, levelOfDetail);
        for (String operator : f.format(plannable.getExplainer(context))) {
            str.append("\n  ");
            str.append(operator);
        }
        return str.toString();
    }

}

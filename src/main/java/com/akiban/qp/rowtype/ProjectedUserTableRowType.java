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

package com.akiban.qp.rowtype;

import java.util.List;

import com.akiban.ais.model.HKey;
import com.akiban.ais.model.UserTable;
import com.akiban.server.explain.*;
import com.akiban.server.expression.Expression;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.texpressions.TPreparedExpression;

public class ProjectedUserTableRowType extends ProjectedRowType {

    public ProjectedUserTableRowType(DerivedTypesSchema schema, UserTable table, List<? extends Expression> projections, List<? extends TPreparedExpression> tExprs) {
        this(schema, table, projections, tExprs, false);
    }

    public ProjectedUserTableRowType(DerivedTypesSchema schema, UserTable table, List<? extends Expression> projections, List<? extends TPreparedExpression> tExprs,
                                     boolean includeInternalColumn) {
        super(schema, table.getTableId(), projections, tExprs);
        this.nFields = includeInternalColumn ? table.getColumnsIncludingInternal().size() : table.getColumns().size();
        this.table = table;
        this.constraintChecker = new UserTableRowChecker(this);
    }

    @Override
    public UserTable userTable() {
        return table;
    }

    @Override
    public boolean hasUserTable() {
        return table != null;
    }
    
    @Override
    public int nFields()
    {
        return nFields;
    }

    @Override
    public ConstraintChecker constraintChecker()
    {
        return constraintChecker;
    }

    @Override
    public HKey hKey()
    {
        return table.hKey();
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer explainer = super.getExplainer(context);
        explainer.addAttribute(Label.TABLE_SCHEMA, PrimitiveExplainer.getInstance(table.getName().getSchemaName()));
        explainer.addAttribute(Label.TABLE_NAME, PrimitiveExplainer.getInstance(table.getName().getTableName()));
        return explainer;
    }

    @Override
    public String toString()
    {
        return String.format("%s: %s", super.toString(), table);
    }

    private final int nFields;
    private final UserTable table;
    private final ConstraintChecker constraintChecker;
}

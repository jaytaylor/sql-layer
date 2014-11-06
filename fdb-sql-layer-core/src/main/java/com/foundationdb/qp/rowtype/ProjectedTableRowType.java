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

package com.foundationdb.qp.rowtype;

import java.util.List;

import com.foundationdb.ais.model.HKey;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.texpressions.TPreparedExpression;

public class ProjectedTableRowType extends ProjectedRowType {

    public ProjectedTableRowType(Schema schema, Table table, List<? extends TPreparedExpression> tExprs) {
        this(schema, table, tExprs, false);
    }

    public ProjectedTableRowType(Schema schema, Table table, List<? extends TPreparedExpression> tExprs,
                                 boolean includeInternalColumn) {
        super(schema, table.getTableId(), tExprs);
        this.nFields = includeInternalColumn ? table.getColumnsIncludingInternal().size() : table.getColumns().size();
        this.table = table;
    }

    @Override
    public Table table() {
        return table;
    }

    @Override
    public boolean hasTable() {
        return table != null;
    }
    
    @Override
    public int nFields()
    {
        return nFields;
    }

    @Override
    public HKey hKey()
    {
        return table.hKey();
    }

    public List<? extends TPreparedExpression> getProjections() {
        return super.getExpressions();
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
    private final Table table;
}

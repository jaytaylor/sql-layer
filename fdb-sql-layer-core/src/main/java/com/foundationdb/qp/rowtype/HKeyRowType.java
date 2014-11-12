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

import com.foundationdb.ais.model.HKey;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.TInstance;

public class HKeyRowType extends DerivedRowType
{
    // Object interface

    @Override
    public String toString()
    {
        return "HKey";
    }

    // RowType interface

    @Override
    public int nFields()
    {
        return nFields;
    }

    @Override
    public TInstance typeAt(int index) {
        return hKey().column(index).getType();
    }

    @Override
    public HKey hKey()
    {
        return hKey;
    }
    
    @Override
    public Table table() {
        return hKey.table();
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer explainer = super.getExplainer(context);
        TableName tableName = hKey.table().getName();
        explainer.addAttribute(Label.TABLE_SCHEMA, PrimitiveExplainer.getInstance(tableName.getSchemaName()));
        explainer.addAttribute(Label.TABLE_NAME, PrimitiveExplainer.getInstance(tableName.getTableName()));
        return explainer;
    }

    // HKeyRowType interface
    
    public HKeyRowType(Schema schema, int typeId, HKey hKey)
    {
        super(schema, typeId);
        this.hKey = hKey;
        this.nFields = hKey.nColumns();
    }

    // Object state

    private final int nFields;
    private HKey hKey;
}

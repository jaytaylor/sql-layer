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

import com.foundationdb.ais.model.*;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.TInstance;

public abstract class IndexRowType extends AisRowType
{
    // Object interface

    @Override
    public String toString()
    {
        return index.toString();
    }

    // RowType interface

    @Override
    public TInstance typeAt(int i) {
        return index.getAllColumns().get(i).getColumn().getType();
    }

    @Override
    public int nFields()
    {
        return index.getAllColumns().size();
    }

    @Override
    public HKey hKey()
    {
        return tableType.hKey();
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer explainer = super.getExplainer(context);
        IndexName indexName = index.getIndexName();
        explainer.addAttribute(Label.TABLE_SCHEMA, PrimitiveExplainer.getInstance(indexName.getSchemaName()));
        explainer.addAttribute(Label.TABLE_NAME, PrimitiveExplainer.getInstance(indexName.getTableName()));
        explainer.addAttribute(Label.INDEX_NAME, PrimitiveExplainer.getInstance(indexName.getName()));
        return explainer;
    }

    // IndexRowType interface
    
    public int declaredFields()
    {
        return index().getKeyColumns().size();
    }

    public TableRowType tableType()
    {
        return tableType;
    }

    public Index index()
    {
        return index;
    }

    public IndexRowType physicalRowType()
    {
        return this;
    }

    public static IndexRowType createIndexRowType(Schema schema, int typeId, TableRowType tableType, Index index)
    {
        return new Conventional(schema, typeId, tableType, index);
    }

    // For use by subclasses

    protected IndexRowType(Schema schema, int typeId, TableRowType tableType, Index index)
    {
        super(schema, typeId);
        if (index.isGroupIndex()) {
            GroupIndex groupIndex = (GroupIndex) index;
            assert groupIndex.leafMostTable() == tableType.table();
        }
        this.tableType = tableType;
        this.index = index;
    }

    // Object state

    // If index is a GroupIndex, then tableType.table() is the leafmost table of the GroupIndex.
    private final TableRowType tableType;
    private final Index index;

    // Inner classes

    private static class Conventional extends IndexRowType
    {
        @Override
        public IndexRowType physicalRowType()
        {
            return spatialIndexRowType == null ? this : spatialIndexRowType;
        }

        public Conventional(Schema schema, int typeId, TableRowType tableType, Index index)
        {
            super(schema, typeId, tableType, index);
            spatialIndexRowType = index.isSpatial() ? new Spatial(schema, typeId, tableType, index) : null;
        }

        // For a spatial index, the IndexRowType reflects the declared columns. physicalRowType reflects the
        // stored index, which replaces the declared columns by a z-value column.
        private final IndexRowType spatialIndexRowType;
    }

    private static class Spatial extends IndexRowType
    {
        @Override
        public IndexRowType physicalRowType()
        {
            assert false;
            return null;
        }

        public Spatial(Schema schema, int typeId, TableRowType tableType, Index index)
        {
            super(schema, typeId, tableType, index);
        }

        @Override
        public int nFields()
        {
            return index().getAllColumns().size() - index().dimensions() + 1;
        }

        @Override
        public TInstance typeAt(int i) {
            int firstSpatial = index().firstSpatialArgument();
            if (i < firstSpatial)
                return super.typeAt(i);
            else if (i == firstSpatial)
                return InternalIndexTypes.LONG.instance(false);
            else
                return super.typeAt(i + index().dimensions() - 1);
        }
    }
}

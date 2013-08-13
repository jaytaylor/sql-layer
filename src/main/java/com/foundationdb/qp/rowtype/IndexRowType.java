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

import com.akiban.ais.model.*;
import com.akiban.server.explain.*;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;

import java.util.*;

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
    public TInstance typeInstanceAt(int i) {
        return index.getAllColumns().get(i).getColumn().tInstance();
    }

    @Override
    public int nFields()
    {
        return akTypes.length;
    }

    @Override
    public AkType typeAt(int index)
    {
        return akTypes[index];
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

    public UserTableRowType tableType()
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

    public static IndexRowType createIndexRowType(Schema schema, UserTableRowType tableType, Index index)
    {
        return new Conventional(schema, tableType, index);
    }

    // For use by subclasses

    protected IndexRowType(Schema schema, UserTableRowType tableType, Index index, int nFields)
    {
        super(schema, schema.nextTypeId());
        if (index.isGroupIndex()) {
            GroupIndex groupIndex = (GroupIndex) index;
            assert groupIndex.leafMostTable() == tableType.userTable();
        }
        this.tableType = tableType;
        this.index = index;
        this.akTypes = new AkType[nFields];
    }

    // Object state

    // If index is a GroupIndex, then tableType.userTable() is the leafmost table of the GroupIndex.
    private final UserTableRowType tableType;
    private final Index index;
    protected final AkType[] akTypes;

    // Inner classes

    private static class Conventional extends IndexRowType
    {
        @Override
        public IndexRowType physicalRowType()
        {
            return spatialIndexRowType == null ? this : spatialIndexRowType;
        }

        public Conventional(Schema schema, UserTableRowType tableType, Index index)
        {
            super(schema, tableType, index, index.getAllColumns().size());
            List<IndexColumn> indexColumns = index.getAllColumns();
            for (int i = 0; i < indexColumns.size(); i++) {
                Column column = indexColumns.get(i).getColumn();
                akTypes[i] = column.getType().akType();
            }
            spatialIndexRowType = index.isSpatial() ? new Spatial(schema, tableType, index) : null;
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

        public Spatial(Schema schema, UserTableRowType tableType, Index index)
        {
            super(schema, tableType, index, index.getAllColumns().size() - index.dimensions() + 1);
            List<IndexColumn> indexColumns = index.getAllColumns();
            int i = 0, t = 0;
            while (i < indexColumns.size()) {
                if (i == index.firstSpatialArgument()) {
                    akTypes[t++] = AkType.LONG;
                    i += index.dimensions();
                }
                else {
                    Column column = indexColumns.get(i++).getColumn();
                    akTypes[t++] = column.getType().akType();
                }
            }
        }

        @Override
        public TInstance typeInstanceAt(int i) {
            int firstSpatial = index().firstSpatialArgument();
            if (i < firstSpatial)
                return super.typeInstanceAt(i);
            else if (i == firstSpatial)
                return MNumeric.BIGINT.instance(false);
            else
                return super.typeInstanceAt(i + index().dimensions() - 1);
        }
    }
}

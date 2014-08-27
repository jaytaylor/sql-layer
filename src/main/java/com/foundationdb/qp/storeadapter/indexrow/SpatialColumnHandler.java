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

package com.foundationdb.qp.storeadapter.indexrow;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.server.geophile.Space;
import com.foundationdb.server.geophile.SpaceLatLon;
import com.foundationdb.server.rowdata.FieldDef;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDataSource;
import com.foundationdb.server.rowdata.RowDataValueSource;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;

public class SpatialColumnHandler
{
    public SpatialColumnHandler(Index index)
    {
        space = index.space();
        dimensions = space.dimensions();
        assert index.dimensions() == dimensions;
        tinstances = new TInstance[dimensions];
        fieldDefs = new FieldDef[dimensions];
        coords = new long[dimensions];
        rowDataSource = new RowDataValueSource();
        firstSpatialField = index.firstSpatialArgument();
        lastSpatialField = firstSpatialField + dimensions - 1;
        for (int d = 0; d < dimensions; d++) {
            IndexColumn indexColumn = index.getKeyColumns().get(firstSpatialField + d);
            Column column = indexColumn.getColumn();
            tinstances[d] = column.getType();
            fieldDefs[d] = column.getFieldDef();
        }
    }

    public boolean handleSpatialColumn(PersistitIndexRowBuffer persistitIndexRowBuffer, int indexField, long zValue)
    {
        assert zValue >= 0;
        if (indexField == firstSpatialField) {
            persistitIndexRowBuffer.pKey().append(zValue);
        }
        return indexField >= firstSpatialField && indexField <= lastSpatialField;
    }

    public long zValue(RowData rowData)
    {
        bind(rowData);
        return space.shuffle(coords);
    }

    private void bind(RowData rowData)
    {
        for (int d = 0; d < dimensions; d++) {
            rowDataSource.bind(fieldDefs[d], rowData);

            RowDataValueSource rowDataValueSource = (RowDataValueSource)rowDataSource;
            TClass tclass = tinstances[d].typeClass();
            if (tclass == MNumeric.DECIMAL) {
                BigDecimalWrapper wrapper = TBigDecimal.getWrapper(rowDataValueSource, tinstances[d]);
                coords[d] =
                    d == 0
                    ? SpaceLatLon.scaleLat(wrapper.asBigDecimal())
                    : SpaceLatLon.scaleLon(wrapper.asBigDecimal());
            }
            else if (tclass == MNumeric.BIGINT) {
                coords[d] = rowDataValueSource.getInt64();
            }
            else if (tclass == MNumeric.INT) {
                coords[d] = rowDataValueSource.getInt32();
            }
            else {
                assert false : fieldDefs[d].column();
            }
        }
    }

    private final Space space;
    private final int dimensions;
    private final TInstance[] tinstances;
    private final FieldDef[] fieldDefs;
    private final long[] coords;
    private final RowDataSource rowDataSource;
    private final int firstSpatialField;
    private final int lastSpatialField;
}

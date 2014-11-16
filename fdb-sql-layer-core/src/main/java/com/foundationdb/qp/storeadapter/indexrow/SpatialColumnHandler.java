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
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.WriteIndexRow;
import com.foundationdb.server.rowdata.FieldDef;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDataSource;
import com.foundationdb.server.rowdata.RowDataValueSource;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.spatial.Spatial;
import com.geophile.z.Space;

public class SpatialColumnHandler
{
    public SpatialColumnHandler(Index index)
    {
        space = index.space();
        dimensions = space.dimensions();
        assert index.dimensions() == dimensions;
        tinstances = new TInstance[dimensions];
        fieldDefs = new FieldDef[dimensions];
        coords = new double[dimensions];
        positions = new int[dimensions];
        
        rowDataSource = new RowDataValueSource();
        firstSpatialField = index.firstSpatialArgument();
        lastSpatialField = index.lastSpatialArgument();
        int spatialColumns = lastSpatialField - firstSpatialField + 1;
        for (int c = 0; c < spatialColumns; c++) {
            IndexColumn indexColumn = index.getKeyColumns().get(firstSpatialField + c);
            Column column = indexColumn.getColumn();
            tinstances[c] = column.getType();
            fieldDefs[c] = column.getFieldDef();
            positions[c] = column.getPosition().intValue();
        }
    }

    public boolean handleSpatialColumn(WriteIndexRow writeIndexRow, int indexField, long zValue)
    {
        assert zValue >= 0;
        if (indexField == firstSpatialField) {
            writeIndexRow.pKey().append(zValue);
        }
        return indexField >= firstSpatialField && indexField <= lastSpatialField;
    }

    public long zValue (Row row) 
    {
        bind (row);
        return Spatial.shuffle(space, coords[0], coords[1]);
    }
    
    public long zValue(RowData rowData)
    {
        bind(rowData);
        return Spatial.shuffle(space, coords[0], coords[1]);
    }

    private void bind (Row row) {
        for (int d = 0; d < dimensions; d++) {
            ValueSource source = row.value(positions[d]);
            TClass tclass = source.getType().typeClass();
            if (tclass == MNumeric.DECIMAL) {
                BigDecimalWrapper wrapper = TBigDecimal.getWrapper(source, tinstances[d]);
                coords[d] = wrapper.asBigDecimal().doubleValue();
            }
            else if (tclass == MNumeric.BIGINT) {
                coords[d] = source.getInt64();
            }
            else if (tclass == MNumeric.INT) {
                coords[d] = source.getInt32();
            }
            else {
                assert false : row.rowType().table().getColumn(positions[d]);
            }
        }
    }
    
    private void bind(RowData rowData)
    {
        for (int d = 0; d < dimensions; d++) {
            rowDataSource.bind(fieldDefs[d], rowData);

            RowDataValueSource rowDataValueSource = (RowDataValueSource)rowDataSource;
            TClass tclass = tinstances[d].typeClass();
            if (tclass == MNumeric.DECIMAL) {
                BigDecimalWrapper wrapper = TBigDecimal.getWrapper(rowDataValueSource, tinstances[d]);
                coords[d] = wrapper.asBigDecimal().doubleValue();
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
    private final int[] positions;
    
    private final TInstance[] tinstances;
    private final FieldDef[] fieldDefs;
    private final double[] coords;
    private final RowDataSource rowDataSource;
    private final int firstSpatialField;
    private final int lastSpatialField;
}

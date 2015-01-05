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
import com.foundationdb.server.error.InvalidSpatialObjectException;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.mcompat.mtypes.MBinary;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.spatial.Spatial;
import com.geophile.z.Space;
import com.geophile.z.SpatialObject;
import com.geophile.z.spatialobject.d2.Point;
import com.vividsolutions.jts.io.ParseException;

public class SpatialColumnHandler
{
    public SpatialColumnHandler(Index index)
    {
        space = index.space();
        dimensions = space.dimensions();
        assert dimensions == 2;
        assert dimensions == index.dimensions();
        firstSpatialField = index.firstSpatialArgument();
        lastSpatialField = index.lastSpatialArgument();
        int spatialColumns = lastSpatialField - firstSpatialField + 1;
        tinstances = new TInstance[spatialColumns];
        positions = new int[spatialColumns];
        for (int c = 0; c < spatialColumns; c++) {
            IndexColumn indexColumn = index.getKeyColumns().get(firstSpatialField + c);
            Column column = indexColumn.getColumn();
            tinstances[c] = column.getType();
            positions[c] = column.getPosition().intValue();
        }
        coords = new double[dimensions];
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
    

    public void processSpatialObject(Row rowData, Operation operation)
    {
        bind(rowData);
        long[] zs = zArray();
        Spatial.shuffle(space, spatialObject, zs);
        for (int i = 0; i < zs.length && zs[i] != Space.Z_NULL; i++) {
            operation.handleZValue(zs[i]);
        }
    }

    private void bind (Row row) {
        if (lastSpatialField > firstSpatialField) {
            // Point coordinates stored in two columns
            assert dimensions == 2 : dimensions;
            double coord = Double.NaN;
            double x = Double.NaN;
            double y = Double.NaN;
            for (int d = 0; d < dimensions; d++) {
                ValueSource source = row.value(positions[d]);
                TClass tclass = source.getType().typeClass();
                if (tclass == MNumeric.DECIMAL) {
                    BigDecimalWrapper wrapper = TBigDecimal.getWrapper(source, tinstances[d]);
                    coord = wrapper.asBigDecimal().doubleValue();
                }
                else if (tclass == MNumeric.BIGINT) {
                    coord = source.getInt64();
                }
                else if (tclass == MNumeric.INT) {
                    coord = source.getInt32();
                }
                else {
                    assert false : row.rowType().table().getColumn(positions[d]);
                }
                if (d == 0) {
                    x = coord;
                } else {
                    y = coord;
                }
                coords[d] = coord;
            }
            spatialObject = new Point(x, y);
        } else {
            ValueSource source = row.value(positions[0]);
            TClass tclass = source.getType().typeClass();
            assert tclass == MBinary.BLOB : tclass;
            byte[] spatialObjectBytes = source.getBytes();
            try {
                spatialObject = Spatial.deserialize(space, spatialObjectBytes);
            } catch (ParseException e) {
                throw new InvalidSpatialObjectException();
            }
        }
    }

    private long[] zArray()
    {
        assert spatialObject != null;
        int maxZ = spatialObject.maxZ();
        if (zs == null || maxZ > zs.length) {
            zs = new long[maxZ];
        }
        return zs;
    }

    private final Space space;
    private final int dimensions;
    private final int[] positions;    
    private final TInstance[] tinstances;
    private final int firstSpatialField;
    private final int lastSpatialField;
    private SpatialObject spatialObject;
    private long[] zs;
    private final double[] coords;

    // Inner classes

    public static abstract class Operation
    {
        public abstract void handleZValue(long z);
    }
}

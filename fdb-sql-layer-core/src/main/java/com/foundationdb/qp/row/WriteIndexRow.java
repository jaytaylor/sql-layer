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

package com.foundationdb.qp.row;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexRowComposition;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.storeadapter.indexcursor.SortKeyAdapter;
import com.foundationdb.qp.storeadapter.indexcursor.SortKeyTarget;
import com.foundationdb.qp.storeadapter.indexcursor.ValueSortKeyAdapter;
import com.foundationdb.qp.storeadapter.indexrow.SpatialColumnHandler;
import com.foundationdb.qp.util.PersistitKey;
import com.foundationdb.server.rowdata.FieldDef;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDataSource;
import com.foundationdb.server.rowdata.RowDataValueSource;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.ArgumentValidation;
import com.persistit.Key;
import com.persistit.Value;

public class WriteIndexRow extends AbstractRow {

    @Override
    public RowType rowType() {
        return indexRowType;
    }

    public void initialize (Row row, Key hKey, SpatialColumnHandler spatialColumnHandler, long zValue) {
        pKeyAppends = 0;
        int indexField = 0;
        IndexRowComposition indexRowComp = index.indexRowComposition();
        while (indexField < indexRowComp.getLength()) {
            // handleSpatialColumn will increment pKeyAppends once for all spatial columns
            if (spatialColumnHandler != null && spatialColumnHandler.handleSpatialColumn(this, indexField, zValue)) {
                if (indexField == index.firstSpatialArgument()) {
                    pKeyAppends++;
                }
            } else {
                if (indexRowComp.isInRowData(indexField)) {
                    int position = indexRowComp.getFieldPosition(indexField);
                    Column column = row.rowType().table().getColumnsIncludingInternal().get(position);
                    ValueSource source = row.value(column.getPosition());
                    pKeyTarget().append(source, column.getType());
                } else if (indexRowComp.isInHKey(indexField)) {
                    PersistitKey.appendFieldFromKey(pKey(), hKey, indexRowComp.getHKeyPosition(indexField), index
                        .getIndexName());
                } else {
                    throw new IllegalStateException("Invalid IndexRowComposition: " + indexRowComp);
                }
                pKeyAppends++;
            }
            indexField++;
        }
        
    }

    public void initialize(RowData rowData, Key hKey, SpatialColumnHandler spatialColumnHandler, long zValue) {
        pKeyAppends = 0;
        int indexField = 0;
        IndexRowComposition indexRowComp = index.indexRowComposition();
        FieldDef[] fieldDefs = index.leafMostTable().rowDef().getFieldDefs();
        RowDataSource rowDataValueSource = new RowDataValueSource();
        while (indexField < indexRowComp.getLength()) {
            // handleSpatialColumn will increment pKeyAppends once for all spatial columns
            if (spatialColumnHandler != null && spatialColumnHandler.handleSpatialColumn(this, indexField, zValue)) {
                if (indexField == index.firstSpatialArgument()) {
                    pKeyAppends++;
                }
            } else {
                if (indexRowComp.isInRowData(indexField)) {
                    FieldDef fieldDef = fieldDefs[indexRowComp.getFieldPosition(indexField)];
                    Column column = fieldDef.column();
                    rowDataValueSource.bind(fieldDef, rowData);
                    pKeyTarget().append(rowDataValueSource,
                                        column.getType());
                } else if (indexRowComp.isInHKey(indexField)) {
                    PersistitKey.appendFieldFromKey(pKey(), hKey, indexRowComp.getHKeyPosition(indexField), index
                        .getIndexName());
                } else {
                    throw new IllegalStateException("Invalid IndexRowComposition: " + indexRowComp);
                }
                pKeyAppends++;
            }
            indexField++;
        }
    }

    
    public void close(Session session, Store store, boolean forInsert) {
        //If we've written too many fields to the key (iKey), make sure to put the extra fields
        // into the value (iValue) for writing. 
        if (pValueTarget != null) {
            iValue.clear();
            iValue.putByteArray(iKeyExtended.getEncodedBytes(), 0, iKeyExtended.getEncodedSize());
        }
    }

    public Key pKey()
    {
        if (pKeyAppends < pKeyFields) {
            return iKey;
        }
        return iKeyExtended;
    }

    @SuppressWarnings("unchecked")
    private <S> SortKeyTarget<S> pKeyTarget()
    {
        if (pKeyAppends < pKeyFields) {
            return pKeyTarget;
        }
        return pValueTarget;
    }

    public void resetForWrite(Index index, Key createKey) {
        resetForWrite(index, createKey, null);
    }
    
    public void resetForWrite(Index index, Key createKey, Value value) {
        this.index = index;
        this.iKey = createKey;
        this.iValue = value;
        this.index = index;
        if (index.isSpatial()) {
            this.pKeyFields = index.getAllColumns().size() - index.dimensions() + 1;
        } else {
            this.pKeyFields = index.getAllColumns().size();
        }
        if (this.pKeyTarget == null) {
            this.pKeyTarget = SORT_KEY_ADAPTER.createTarget(index.getIndexName());
        }
        this.pKeyTarget.attach(createKey);
    }

    // Group Index Row only - table bitmap stored in index value
    public void tableBitmap(long bitmap) {
        iValue.put(bitmap);
    }

    public long tableBitmap() {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    public <S> void append(S source, TInstance type) {
        pKeyTarget.append(source, type);
    }

    public WriteIndexRow (KeyCreator keyCreator) {
        ArgumentValidation.notNull("keyCreator", keyCreator);
    }
    
    private Index index;
    private Key iKey;
    private Key iKeyExtended;
    private Value iValue;
    private IndexRowType indexRowType;
    private SortKeyTarget pKeyTarget;
    private SortKeyTarget pValueTarget;
    private int pKeyAppends = 0;
    private int pKeyFields;

    private final SortKeyAdapter<ValueSource, TPreparedExpression> SORT_KEY_ADAPTER = ValueSortKeyAdapter.INSTANCE;
    
    @Override
    public HKey hKey() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    protected ValueSource uncheckedValue(int i) {
        throw new UnsupportedOperationException();
    }

}

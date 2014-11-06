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

package com.foundationdb.server.store;

import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.storeadapter.RowDataCreator;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.api.dml.scan.NiceRow;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.store.format.FDBStorageDescription;
import com.foundationdb.tuple.ByteArrayUtil;
import com.foundationdb.tuple.Tuple2;
import com.persistit.Key;

import java.util.Arrays;

public class FDBStoreDataHelper
{
    private FDBStoreDataHelper() {
    }

    public static byte[] prefixBytes(FDBStoreData storeData) {
        return prefixBytes(storeData.storageDescription);
    }

    public static byte[] prefixBytes(HasStorage object) {
        return prefixBytes((FDBStorageDescription)object.getStorageDescription());
    }

    public static byte[] prefixBytes(FDBStorageDescription storageDescription) {
        return storageDescription.getPrefixBytes();
    }

    public static void unpackKey(FDBStoreData storeData) {
        unpackTuple(storeData.storageDescription, storeData.persistitKey,
                    storeData.rawKey);
    }

    public static void unpackTuple(HasStorage object, Key key, byte[] tupleBytes) {
        unpackTuple((FDBStorageDescription)object.getStorageDescription(),
                    key, tupleBytes);
    }

    public static void unpackTuple(FDBStorageDescription storageDescription, Key key, byte[] tupleBytes) {
        byte[] treeBytes = prefixBytes(storageDescription);
        // TODO: Use fromBytes(byte[],int,int) when available.
        Tuple2 tuple = Tuple2.fromBytes(Arrays.copyOfRange(tupleBytes, treeBytes.length, tupleBytes.length));
        storageDescription.getTupleKey(tuple, key);
    }

    public static byte[] packKey(FDBStoreData storeData) {
        return packKey(storeData, null);
    }

    public static byte[] packKey(FDBStoreData storeData, Key.EdgeValue edge) {
        storeData.rawKey = packedTuple(storeData.storageDescription, 
                                       storeData.persistitKey, edge, storeData.nudgeDir);
        return storeData.rawKey;
    }

    public static byte[] packedTuple(HasStorage object, Key key) {
        return packedTuple(object, key, null);
    }

    public static byte[] packedTuple(HasStorage object, Key key, Key.EdgeValue edge) {
        return packedTuple((FDBStorageDescription)object.getStorageDescription(), key, edge, null);
    }

    public static byte[] packedTuple(FDBStorageDescription storageDescription, Key key) {
        return packedTuple(storageDescription, key, null, null);
    }

    public static byte[] packedTuple(FDBStorageDescription storageDescription, Key key, Key.EdgeValue edge, FDBStoreData.NudgeDir nudged) {
        byte[] treeBytes = prefixBytes(storageDescription);
        if (edge != null) {
            // TODO: Could eliminate new key if callers didn't rely on key state outside getEncodedSize()
            // (see checkUniqueness() in FDBStore).
            Key nkey = new Key(null, key.getEncodedSize() + 1);
            key.copyTo(nkey);
            key = nkey;
            key.append(edge);
        }
        byte[] keyBytes = storageDescription.getKeyBytes(key, nudged);
        return ByteArrayUtil.join(treeBytes, keyBytes);
    }

    public static void unpackValue(FDBStoreData storeData) {
        storeData.persistitValue.clear();
        storeData.persistitValue.putEncodedBytes(storeData.rawValue, 0, storeData.rawValue.length);
    }

    public static void expandRow (Row row, FDBStoreData storeData) {
        throw new UnsupportedOperationException();
    }
    
    public static void expandRowData(RowData rowData, FDBStoreData storeData, boolean copyBytes) {
        expandRowData(rowData, storeData.rawValue, copyBytes);
    }

    public static void expandRowData(RowData rowData, byte[] value, boolean copyBytes) {
        if(copyBytes) {
            byte[] rowBytes = rowData.getBytes();
            if((rowBytes == null) || (rowBytes.length < value.length)) {
                rowBytes = Arrays.copyOf(value, value.length);
                rowData.reset(rowBytes);
            } else {
                System.arraycopy(value, 0, rowBytes, 0, value.length);
                rowData.reset(0, value.length);
            }
        } else {
            rowData.reset(value);
        }
        rowData.prepareRow(0);
    }

    public static void packRowData(RowData rowData, FDBStoreData storeData) {
        storeData.rawValue = Arrays.copyOfRange(rowData.getBytes(), rowData.getRowStart(), rowData.getRowEnd());
    }
    
    public static void packRow(Row row, FDBStoreData storeData) {
        
        RowDef rowDef = row.rowType().table().rowDef();
        RowDataCreator creator = new RowDataCreator();
        NewRow niceRow = new NiceRow(rowDef.getRowDefId(), rowDef);
        int fields = rowDef.table().getColumnsIncludingInternal().size();
        for(int i = 0; i < fields; ++i) {
            creator.put(row.value(i), niceRow, i);
        }
        RowData rowData = niceRow.toRowData();
        storeData.rawValue = Arrays.copyOfRange(rowData.getBytes(), rowData.getRowStart(), rowData.getRowEnd());
    }

}

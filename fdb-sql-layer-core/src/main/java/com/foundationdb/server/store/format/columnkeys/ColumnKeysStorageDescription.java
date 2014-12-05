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

package com.foundationdb.server.store.format.columnkeys;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.validation.AISValidationFailure;
import com.foundationdb.ais.model.validation.AISValidationOutput;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.FDBProtobuf;
import com.foundationdb.ais.protobuf.FDBProtobuf.ColumnKeys;
import com.foundationdb.ais.protobuf.FDBProtobuf.TupleUsage;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.error.StorageDescriptionInvalidException;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.FDBStore;
import com.foundationdb.server.store.FDBStoreData;
import com.foundationdb.server.store.FDBTransactionService.TransactionState;
import com.foundationdb.server.store.format.FDBStorageDescription;
import com.foundationdb.server.store.format.tuple.TupleRowDataConverter;
import com.foundationdb.server.store.format.tuple.TupleStorageDescription;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.tuple.ByteArrayUtil;
import com.foundationdb.tuple.Tuple2;
import com.persistit.Key;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.foundationdb.server.store.FDBStoreDataHelper.*;

/**
 * Encode a row as separate key/value pairs, one for each column.
 * The key is the same as for {@link TupleStorageDescription}, plus an
 * element of the column name string.
 * The value is a single <code>Tuple</code> element.
 * Child rows follow immediately after the last parent column, due to
 * the tuple encoding using 02 for column name strings and 0C-1C for
 * ordinal integers.
 */
public class ColumnKeysStorageDescription extends FDBStorageDescription
{
    protected static final byte[] FIRST_NUMERIC = { 0x0C };

    public ColumnKeysStorageDescription(HasStorage forObject, String storageFormat) {
        super(forObject, storageFormat);

    }

    public ColumnKeysStorageDescription(HasStorage forObject, ColumnKeysStorageDescription other, String storageFormat) {
        super(forObject, other, storageFormat);
    }

    @Override
    public StorageDescription cloneForObject(HasStorage forObject) {
        return new ColumnKeysStorageDescription(forObject, this, storageFormat);
    }
    
    @Override
    public StorageDescription cloneForObjectWithoutState(HasStorage forObject) {
        return new ColumnKeysStorageDescription(forObject, storageFormat);
    }

    @Override
    public void writeProtobuf(Storage.Builder builder) {
        super.writeProtobuf(builder);
        builder.setExtension(FDBProtobuf.columnKeys, ColumnKeys.YES); // no options yet
        writeUnknownFields(builder);
    }

    @Override
    public void validate(AISValidationOutput output) {
        super.validate(output);
        if (!(object instanceof Group)) {
            output.reportFailure(new AISValidationFailure(new StorageDescriptionInvalidException(object, "is not a Group")));
            return;
        }
        List<String> illegal = TupleRowDataConverter.checkTypes((Group)object, TupleUsage.KEY_AND_ROW);
        if (!illegal.isEmpty()) {
            output.reportFailure(new AISValidationFailure(new StorageDescriptionInvalidException(object, "has some types that cannot be stored in a Tuple: " + illegal)));
        }
    }

    @Override
    public byte[] getKeyBytes(Key key, FDBStoreData.NudgeDir nudged) {
        assert nudged == null : "Nudge only expected during mixed mode index iteration";
        return getKeyBytes(key);
    }
    
    @Override
    public byte[] getKeyBytes(Key key ) {
        return TupleStorageDescription.getKeyBytesInternal(key, null);
    }
        
    @Override
    public void getTupleKey(Tuple2 t, Key key) {
        key.clear();
        TupleStorageDescription.appendHKeySegments(t, key, ((Group)object));
    }

    @Override
    public void packRow (FDBStore store, Session session, 
                        FDBStoreData storeData, Row row) {
        int nfields = row.rowType().nFields();
        Map<String,Object> value = new HashMap<>(nfields); // Intermediate form of value.
        for (int i = 0; i < nfields; i++) {
            value.put(row.rowType().table().getColumn(i).getName(), ValueSources.toObject(row.value(i)));
        }
        storeData.otherValue = value;
    }
    
    @Override 
    @SuppressWarnings("unchecked")
    public Row expandRow(FDBStore store, Session session, 
                            FDBStoreData storeData, Schema schema) {
        Map<String,Object> value = (Map<String,Object>)storeData.otherValue;
        
        Table table = TupleStorageDescription.tableFromOrdinals((Group)object, storeData.persistitKey);
        RowType rowType = schema.tableRowType(table);
        int nfields = rowType.nFields();
        Object[] objects = new Object[nfields];
        for (int i = 0; i < nfields; i++) {
            objects[i] = value.get(rowType.fieldColumn(i).getName());
        }
        Row row = new ValuesHolderRow(rowType, objects);
        return row;
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public void store(FDBStore store, Session session, FDBStoreData storeData) {
        TransactionState txn = store.getTransaction(session, storeData);
        // Erase all previous column values, in case not present in Map.
        txn.clearRange(storeData.rawKey, ByteArrayUtil.join(storeData.rawKey, FIRST_NUMERIC));
        Map<String,Object> value = (Map<String,Object>)storeData.otherValue;
        for (Map.Entry<String,Object> entry : value.entrySet()) {
            txn.setBytes(ByteArrayUtil.join(storeData.rawKey,
                                            Tuple2.from(entry.getKey()).pack()),
                         Tuple2.from(entry.getValue()).pack());
        }
    }

    @Override
    public boolean fetch(FDBStore store, Session session, FDBStoreData storeData) {
        // Cannot get in a single fetch.
        try {
            groupIterator(store, session, storeData,
                          FDBStore.GroupIteratorBoundary.KEY,
                          FDBStore.GroupIteratorBoundary.FIRST_DESCENDANT,
                          1, false);
            return storeData.next();
        }
        finally {
            storeData.closeIterator();
        }
    }

    @Override
    public void clear(FDBStore store, Session session, FDBStoreData storeData) {
        TransactionState txn = store.getTransaction(session, storeData);
        byte[] begin = storeData.rawKey;
        byte[] end = ByteArrayUtil.join(begin, FIRST_NUMERIC);
        txn.clearRange(begin, end);
    }

    @Override
    public void groupIterator(FDBStore store, Session session, FDBStoreData storeData,
                              FDBStore.GroupIteratorBoundary left, FDBStore.GroupIteratorBoundary right,
                              int limit, boolean snapshot) {
        byte[] begin, end;
        switch (left) {
        case START:
            begin = prefixBytes(storeData);
            break;
        case KEY:
            begin = packKey(storeData);
            break;
        case NEXT_KEY:
            // Meaning possibly descendants.
        case FIRST_DESCENDANT:
            begin = ByteArrayUtil.join(packKey(storeData), FIRST_NUMERIC);
            break;
        default:
            throw new IllegalArgumentException(left.toString());
        }
        switch (right) {
        case END:
            end = ByteArrayUtil.strinc(prefixBytes(storeData));
            break;
        case NEXT_KEY:
        case FIRST_DESCENDANT:
            end = ByteArrayUtil.join(packKey(storeData), FIRST_NUMERIC);
            break;
        case LAST_DESCENDANT:
            end = packKey(storeData, Key.AFTER);
            break;
        default:
            throw new IllegalArgumentException(right.toString());
        }
        storeData.iterator = 
            new ColumnKeysStorageIterator(storeData,
                                          store.getTransaction(session, storeData)
                                          .getRangeIterator(begin, end),
                                          limit);
    }

    public void indexIterator(FDBStore store, Session session, FDBStoreData storeData,
                              boolean key, boolean inclusive, boolean reverse) {
        throw new UnsupportedOperationException();
    }

}

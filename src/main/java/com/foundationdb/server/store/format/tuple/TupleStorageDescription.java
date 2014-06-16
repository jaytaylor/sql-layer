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

package com.foundationdb.server.store.format.tuple;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.validation.AISValidationFailure;
import com.foundationdb.ais.model.validation.AISValidationOutput;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.FDBProtobuf.TupleUsage;
import com.foundationdb.ais.protobuf.FDBProtobuf;
import com.foundationdb.server.error.StorageDescriptionInvalidException;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.FDBStore;
import com.foundationdb.server.store.FDBStoreData;
import com.foundationdb.server.store.format.FDBStorageDescription;
import com.foundationdb.tuple.ByteArrayUtil;
import com.foundationdb.tuple.Tuple2;

import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.persistit.Key;
import com.persistit.KeyShim;

import java.util.List;

public class TupleStorageDescription extends FDBStorageDescription
{
    private TupleUsage usage;

    public TupleStorageDescription(HasStorage forObject) {
        super(forObject);
    }

    public TupleStorageDescription(HasStorage forObject, TupleStorageDescription other) {
        super(forObject, other);
        this.usage = other.usage;
    }

    @Override
    public StorageDescription cloneForObject(HasStorage forObject) {
        return new TupleStorageDescription(forObject, this);
    }

    public TupleUsage getUsage() {
        return usage;
    }
    public void setUsage(TupleUsage usage) {
        this.usage = usage;
    }

    @Override
    public void writeProtobuf(Storage.Builder builder) {
        super.writeProtobuf(builder);
        if (usage != null) {
            builder.setExtension(FDBProtobuf.tupleUsage, usage);
        }
        writeUnknownFields(builder);
    }

    @Override
    public void validate(AISValidationOutput output) {
        super.validate(output);
        if (usage == null) {
            return;
        }
        if (usage == TupleUsage.KEY_AND_ROW) {
            if (!(object instanceof Group)) {
                output.reportFailure(new AISValidationFailure(new StorageDescriptionInvalidException(object, "is not a Group and has no row")));
                return;
            }
            if (!((Group)object).getRoot().getChildJoins().isEmpty()) {
                output.reportFailure(new AISValidationFailure(new StorageDescriptionInvalidException(object, "has more than one table")));
                return;
            }
        }
        List<String> illegal;
        if (object instanceof Group) {
            illegal = TupleRowDataConverter.checkTypes((Group)object, usage);
        }
        else if (object instanceof Index) {
            illegal = TupleRowDataConverter.checkTypes((Index)object, usage);
        }
        else {
            output.reportFailure(new AISValidationFailure(new StorageDescriptionInvalidException(object, "is not a Group or Index and cannot use Tuples")));
            return;
        }
        if (!illegal.isEmpty()) {
            output.reportFailure(new AISValidationFailure(new StorageDescriptionInvalidException(object, "has some types that cannot be stored in a Tuple: " + illegal)));
        }
    }

    @Override
    public byte[] getKeyBytes(Key key) {
        if (usage != null) {
            // If the Key is encoded as a single component Tuple, you
            // need to apply the edge before encoding. But with
            // multiple components, it wants to do it after packing.
            // For a Key {1}, whose bytes are 258100, and as a Tuple
            // 01258100FF00, strinc would be 01258100FF01, whereas
            // {1,{after}} would be 258100FE, so 01258100FFFE00.
            // So, take edge out and do below.
            Key.EdgeValue edge = null;
            int nkeys = key.getDepth();
            if (KeyShim.isBefore(key)) {
                edge = Key.BEFORE;
                nkeys--;
            }
            else if (KeyShim.isAfter(key)) {
                edge = Key.AFTER;
                nkeys--;
            }
            Object[] keys = new Object[nkeys];
            key.reset();
            for (int i = 0; i < nkeys; i++) {
                keys[i] = key.decode();
            }
            byte[] bytes = Tuple2.from(keys).pack();
            if (edge == Key.BEFORE) {
                return ByteArrayUtil.join(bytes, new byte[1]);
            }
            else if (edge == Key.AFTER) {
                if (nkeys == 0) {
                    return new byte[] { (byte)0xFF };
                }
                else {
                    return ByteArrayUtil.strinc(bytes);
                }
            }
            else {
                return bytes;
            }
        }
        else {
            return super.getKeyBytes(key);
        }
    }

    @Override
    public void getTupleKey(Tuple2 t, Key key) {
        if (usage != null) {
            key.clear();
            if (object instanceof Group) {
                appendHKeySegments(t, key, ((Group)object));
            }
            else {
                for (Object seg : t) {
                    key.append(seg);
                }
            }
        }
        else {
            super.getTupleKey(t, key);
        }
    }
    
    /** <code>Tuple</code> does not distinguish integer types. This is
     * mostly not a problem, since they are all encoded as longs in
     * Persistit.  Except for ordinals, which are ints.
     */
    public static void appendHKeySegments(Tuple2 t, Key key, Group group) {
        Table table = null;
        int nextOrdinalIndex = 0;
        for (int i = 0; i < t.size(); i++) {
            Object seg = t.get(i);
            if ((i == nextOrdinalIndex) &&
                (seg instanceof Long)) {
                int ordinal = ((Long)seg).intValue();
                boolean found = false;
                if (i == 0) {
                    table = group.getRoot();
                    found = (table.getOrdinal() == ordinal);
                }
                else {
                    for (Join join : table.getChildJoins()) {
                        table = join.getChild();
                        if (table.getOrdinal() == ordinal) {
                            found = true;
                            break;
                        }
                    }
                }
                if (found) {
                    nextOrdinalIndex = i + 1 + table.getPrimaryKey().getColumns().size();
                    seg = ordinal;
                }
            }
            key.append(seg);
        }
    }

    @Override
    public void packRowData(FDBStore store, Session session,
                            FDBStoreData storeData, RowData rowData) {
        if (usage == TupleUsage.KEY_AND_ROW) {
            RowDef rowDef = ((Group)object).getRoot().rowDef();
            assert (rowDef.getRowDefId() == rowData.getRowDefId()) : rowData;
            Tuple2 t = TupleRowDataConverter.tupleFromRowData(rowDef, rowData);
            storeData.rawValue = t.pack();
        }
        else {
            super.packRowData(store, session, storeData, rowData);
        }
    }

    @Override
    public void expandRowData(FDBStore store, Session session,
                              FDBStoreData storeData, RowData rowData) {
        if (usage == TupleUsage.KEY_AND_ROW) {
            Tuple2 t = Tuple2.fromBytes(storeData.rawValue);
            RowDef rowDef = ((Group)object).getRoot().rowDef();
            TupleRowDataConverter.tupleToRowData(t, rowDef, rowData);
        }
        else {
            super.expandRowData(store, session, storeData, rowData);
        }
    }

}

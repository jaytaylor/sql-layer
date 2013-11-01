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
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.validation.AISValidationFailure;
import com.foundationdb.ais.model.validation.AISValidationOutput;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.FDBProtobuf.TupleUsage;
import com.foundationdb.ais.protobuf.FDBProtobuf;
import com.foundationdb.server.error.StorageDescriptionInvalidException;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.store.FDBStore;
import com.foundationdb.server.store.FDBStoreData;
import com.foundationdb.server.store.format.FDBStorageDescription;
import com.foundationdb.tuple.ByteArrayUtil;
import com.foundationdb.tuple.Tuple;

import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.persistit.Key;

import java.util.List;

public class TupleStorageDescription extends FDBStorageDescription
{
    private TupleUsage usage;

    public TupleStorageDescription(HasStorage forObject) {
        super(forObject);
    }

    public TupleStorageDescription(HasStorage forObject, TupleStorageDescription other) {
        super(forObject);
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
    public byte[] getKeyBytes(Key key, Key.EdgeValue edge) {
        if (usage != null) {
            Object[] keys = new Object[key.getDepth()];
            key.reset();
            for (int i = 0; i < keys.length; i++) {
                keys[i] = key.decode();
            }
            byte[] bytes = Tuple.from(keys).pack();
            if (edge == Key.BEFORE) {
                return ByteArrayUtil.join(bytes, new byte[1]);
            }
            else if (edge == Key.AFTER) {
                return ByteArrayUtil.strinc(bytes);
            }
            else {
                return bytes;
            }
        }
        else {
            return super.getKeyBytes(key, edge);
        }
    }

    @Override
    public void getTupleKey(Tuple t, Key key) {
        if (usage != null) {
            key.clear();
            for (Object seg : t) {
                key.append(seg);
            }
        }
        else {
            super.getTupleKey(t, key);
        }
    }

    @Override
    public void packRowData(FDBStore store, FDBStoreData storeData, RowData rowData) {
        if (usage == TupleUsage.KEY_AND_ROW) {
            RowDef rowDef = ((Group)object).getRoot().rowDef();
            assert (rowDef.getRowDefId() == rowData.getRowDefId()) : rowData;
            Tuple t = TupleRowDataConverter.tupleFromRowData(rowDef, rowData);
            storeData.value = t.pack();
        }
        else {
            super.packRowData(store, storeData, rowData);
        }
    }

    @Override
    public void expandRowData(FDBStore store, FDBStoreData storeData, RowData rowData) {
        if (usage == TupleUsage.KEY_AND_ROW) {
            Tuple t = Tuple.fromBytes(storeData.value);
            RowDef rowDef = ((Group)object).getRoot().rowDef();
            TupleRowDataConverter.tupleToRowData(t, rowDef, rowData);
        }
        else {
            super.expandRowData(store, storeData, rowData);
        }
    }

}

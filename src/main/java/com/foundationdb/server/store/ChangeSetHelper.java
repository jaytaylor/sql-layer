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

import com.foundationdb.ais.util.TableChange;
import com.foundationdb.ais.util.TableChange.ChangeType;
import com.foundationdb.server.store.TableChanges.Change;
import com.foundationdb.server.store.TableChanges.ChangeSet;
import com.foundationdb.server.store.TableChanges.IndexChange;
import com.foundationdb.util.ArgumentValidation;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.io.IOException;

public class ChangeSetHelper
{
    public static byte[] save(ChangeSet changeSet) {
        ArgumentValidation.notNull("changeSet", changeSet);
        checkFields(changeSet);
        int size = changeSet.getSerializedSize();
        byte[] buffer = new byte[size];
        CodedOutputStream stream = CodedOutputStream.newInstance(buffer);
        try {
            changeSet.writeTo(stream);
        } catch(IOException e) {
            // Only throws OutOfSpace, which shouldn't happen
            throw new IllegalStateException(e);
        }
        return buffer;
    }

    public static ChangeSet load(byte[] buffer) {
        ArgumentValidation.notNull("buffer", buffer);
        ChangeSet.Builder builder = ChangeSet.newBuilder();
        try {
            builder.mergeFrom(buffer);
        } catch(InvalidProtocolBufferException e) {
            throw new IllegalStateException(e);
        }
        ChangeSet changeSet = builder.build();
        checkFields(changeSet);
        return changeSet;
    }

    public static Change createAddChange(String newName) {
        return Change.newBuilder().setChangeType(ChangeType.ADD.name()).setNewName(newName).build();
    }

    public static Change createDropChange(String oldName) {
        return Change.newBuilder().setChangeType(ChangeType.DROP.name()).setOldName(oldName).build();
    }

    public static Change createModifyChange(String oldName, String newName) {
        return Change.newBuilder().setChangeType(ChangeType.MODIFY.name()).setOldName(oldName).setNewName(newName).build();
    }

    public static Change createFromTableChange(TableChange tableChange) {
        switch(tableChange.getChangeType()) {
            case ADD:
                return ChangeSetHelper.createAddChange(tableChange.getNewName());
            case MODIFY:
                return ChangeSetHelper.createModifyChange(tableChange.getOldName(), tableChange.getNewName());
            case DROP:
                return ChangeSetHelper.createDropChange(tableChange.getOldName());
            default:
                throw new IllegalStateException(tableChange.getChangeType().toString());
        }
    }

    //
    // Internal
    //

    private static void checkFields(Change change) {
        requiredFields(change, Change.CHANGE_TYPE_FIELD_NUMBER);
        switch(ChangeType.valueOf(change.getChangeType())) {
            case ADD:
                requiredFields(change, Change.NEW_NAME_FIELD_NUMBER);
            break;
            case DROP:
                requiredFields(change, Change.OLD_NAME_FIELD_NUMBER);
            break;
            case MODIFY:
                requiredFields(change, Change.OLD_NAME_FIELD_NUMBER, Change.NEW_NAME_FIELD_NUMBER);
            break;
        }
    }

    private static void checkFields(IndexChange indexChange) {
        requiredFields(indexChange, IndexChange.INDEX_TYPE_FIELD_NUMBER, IndexChange.CHANGE_FIELD_NUMBER);
        checkFields(indexChange.getChange());
    }

    private static void checkFields(ChangeSet changeSet) {
        requiredFields(changeSet,
                       ChangeSet.CHANGE_LEVEL_FIELD_NUMBER,
                       ChangeSet.TABLE_ID_FIELD_NUMBER,
                       ChangeSet.OLD_SCHEMA_FIELD_NUMBER,
                       ChangeSet.OLD_NAME_FIELD_NUMBER);
        for(Change c : changeSet.getColumnChangeList()) {
            checkFields(c);
        }
        for(IndexChange c : changeSet.getIndexChangeList()) {
            checkFields(c);
        }
    }

    private static void requiredFields(Message msg, int... fields) {
        for(int fieldNumber : fields) {
            FieldDescriptor field = msg.getDescriptorForType().findFieldByNumber(fieldNumber);
            if(!msg.hasField(field)) {
                throw new IllegalArgumentException("Missing field: " + field.getName());
            }
        }
    }
}

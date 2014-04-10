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

package com.foundationdb.server.types.service;

import com.foundationdb.server.error.UnsupportedColumnDataTypeException;
import com.foundationdb.server.types.TBundleID;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TName;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.StringFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

// TODO: Does not do anything with bundles at present. Type namespace is flat.
public class TypesRegistry
{
    private final Map<String,TBundleID> typeBundleIDByName =
        new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private final Map<String,TClass> typeClassByName =
        new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    public TypesRegistry(Collection<? extends TClass> tClasses) {
        for (TClass tClass : tClasses) {
            TName tName = tClass.name();
            typeBundleIDByName.put(tName.bundleId().name(), tName.bundleId());
            TClass prev = typeClassByName.put(tName.unqualifiedName(), tClass);
            if (prev != null) {
                throw new IllegalStateException("Duplicate classes: " + prev +
                                                " and " + tClass);
            }
        }
    }

    public Collection<? extends TBundleID> getTypeBundleIDs() {
        return Collections.unmodifiableCollection(typeBundleIDByName.values());
    }

    public Collection<? extends TClass> getTypeClasses() {
        return Collections.unmodifiableCollection(typeClassByName.values());
    }

    public TClass getTypeClass(String name) {
        return typeClassByName.get(name);
    }

    /**
     * Get the type instance with the given name and parameters.
     * Table and column name are supplied for the sake of the error message.
     */
    public TInstance getType(String typeName,
                             Long typeParameter1, Long typeParameter2,
                             boolean nullable,
                             String tableSchema, String tableName, String columnName) {
        return getType(typeName, typeParameter1, typeParameter2, null, null,
                nullable, tableSchema, tableName, columnName);
    }

    public TInstance getType(String typeName,
                             Long typeParameter1, Long typeParameter2,
                             String charset, String collation,
                             boolean nullable,
                             String tableSchema, String tableName, String columnName) {
        return getType(typeName, typeParameter1, typeParameter2,
                charset, collation, StringFactory.DEFAULT_CHARSET_ID, StringFactory.DEFAULT_COLLATION_ID,
                nullable, tableSchema, tableName, columnName);
    }

    public TInstance getType(String typeName,
                             Long typeParameter1, Long typeParameter2,
                             String charset, String collation,
                             int defaultCharsetId, int defaultCollationId,
                             boolean nullable,
                             String tableSchema, String tableName, String columnName) {
        return getType(typeName, null, -1, typeParameter1, typeParameter2,
                charset, collation, StringFactory.DEFAULT_CHARSET_ID, StringFactory.DEFAULT_COLLATION_ID,
                nullable, tableSchema, tableName, columnName);
    }

    public TInstance getType(String typeName, UUID typeBundleUUID, int typeVersion,
                             Long typeParameter1, Long typeParameter2,
                             boolean nullable,
                             String tableSchema, String tableName, String columnName) {
        return getType(typeName, typeBundleUUID, typeVersion, typeParameter1, typeParameter2, null, null,
                nullable, tableSchema, tableName, columnName);
    }

    public TInstance getType(String typeName, UUID typeBundleUUID, int typeVersion,
                             Long typeParameter1, Long typeParameter2,
                             String charset, String collation,
                             boolean nullable,
                             String tableSchema, String tableName, String columnName) {
        return getType(typeName, typeBundleUUID, typeVersion, typeParameter1, typeParameter2,
                charset, collation, StringFactory.DEFAULT_CHARSET_ID, StringFactory.DEFAULT_COLLATION_ID,
                nullable, tableSchema, tableName, columnName);
    }

    public TInstance getType(String typeName, UUID typeBundleUUID, int typeVersion,
                             Long typeParameter1, Long typeParameter2,
                             String charset, String collation,
                             int defaultCharsetId, int defaultCollationId,
                             boolean nullable,
                             String tableSchema, String tableName, String columnName) {

        TClass typeClass = getTypeClass(typeName);
        if (typeClass == null) {
            throw new UnsupportedColumnDataTypeException(tableSchema, tableName, columnName,
                                                   typeName);
        }
        assert ((typeBundleUUID == null) || typeBundleUUID.equals(typeClass.name().bundleId().uuid())) : typeClass;
        assert ((typeVersion < 0) || (typeVersion == typeClass.serializationVersion())) : typeClass;
        if (typeClass.hasAttributes(StringAttribute.class)) {
            int charsetId = defaultCharsetId, collationId = defaultCollationId;
            if (charset != null) {
                charsetId = StringFactory.charsetNameToId(charset);
            }
            if (collation != null) {
                collationId = StringFactory.collationNameToId(collation);
            }
            return typeClass.instance((typeParameter1 == null) ? 0 : typeParameter1.intValue(),
                                   charsetId, collationId,
                                   nullable);
        }
        else if (typeParameter2 != null) {
            return typeClass.instance(typeParameter1.intValue(), typeParameter2.intValue(),
                                   nullable);
        }
        else if (typeParameter1 != null) {
            return typeClass.instance(typeParameter1.intValue(), nullable);
        }
        else {
            return typeClass.instance(nullable);
        }
    }

}

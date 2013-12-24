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

import com.foundationdb.server.collation.AkCollatorFactory;
import com.foundationdb.server.error.UnsupportedDataTypeException;
import com.foundationdb.server.types.TBundleID;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TName;
import com.foundationdb.server.types.common.types.StringFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

// TODO: Does not do anything with bundles at present. Type namespace is flat.
public class TypesRegistry
{
    private final Map<String,TBundleID> tBundleIDByName =
        new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private final Map<String,TClass> tClassByName =
        new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    public TypesRegistry(Collection<? extends TClass> tClasses) {
        for (TClass tClass : tClasses) {
            TName tName = tClass.name();
            tBundleIDByName.put(tName.bundleId().name(), tName.bundleId());
            TClass prev = tClassByName.put(tName.unqualifiedName(), tClass);
            if (prev != null) {
                throw new IllegalStateException("Duplicate classes: " + prev +
                                                " and " + tClass);
            }
        }
    }

    public Collection<? extends TBundleID> getTBundleIDs() {
        return Collections.unmodifiableCollection(tBundleIDByName.values());
    }

    public Collection<? extends TClass> getTClasses() {
        return Collections.unmodifiableCollection(tClassByName.values());
    }

    public TClass getTClass(String name) {
        return tClassByName.get(name);
    }

    /**
     * Get the type instance with the given name and parameters.
     * Table and column name are supplied for the sake of the error message.
     */
    public TInstance getTInstance(String typeName,
                                  Long typeParameter1, Long typeParameter2,
                                  String charset, String collation,
                                  boolean nullable,
                                  String tableSchema, String tableName, String columnName) {
        TClass tclass = getTClass(typeName);
        if (tclass == null) {
            throw new UnsupportedDataTypeException(tableSchema, tableName, columnName,
                                                   typeName);
        }
        if ((charset != null) || (collation != null)) {
            int charsetId = 0, collatorId = 0;
            if (charset != null) {
                charsetId = StringFactory.Charset.of(charset).ordinal();
            }
            if (collation != null) {
                collatorId = AkCollatorFactory.getAkCollator(collation).getCollationId();
            }
            return tclass.instance((typeParameter1 == null) ? 0 : typeParameter1.intValue(),
                                   charsetId, collatorId,
                                   nullable);
        }
        else if (typeParameter2 != null) {
            return tclass.instance(typeParameter1.intValue(), typeParameter2.intValue(),
                                   nullable);
        }
        else if (typeParameter1 != null) {
            return tclass.instance(typeParameter1.intValue(), nullable);
        }
        else {
            return tclass.instance(nullable);
        }
    }

}

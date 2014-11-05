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

package com.foundationdb.ais.model;

import com.foundationdb.server.store.format.StorageFormatRegistry;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.service.TypesRegistry;

/** Add convenience methods for setting up tests with columns typed by name. */
public class TestAISBuilder extends AISBuilder
{
    private final TypesRegistry typesRegistry;

    public TestAISBuilder(TypesRegistry typesRegistry) {
        this.typesRegistry = typesRegistry;
    }

    public TestAISBuilder(AkibanInformationSchema ais, TypesRegistry typesRegistry) {
        super(ais);
        this.typesRegistry = typesRegistry;
    }
    
    public TestAISBuilder(AkibanInformationSchema ais, NameGenerator nameGenerator,
                          TypesRegistry typesRegistry, StorageFormatRegistry storageFormatRegistry) {
        super(ais, nameGenerator, storageFormatRegistry);
        this.typesRegistry = typesRegistry;
    }

    public Column column(String schemaName, String tableName, String columnName,
                         Integer position, String typeBundle, String typeName,
                         boolean nullable) {
        TInstance type = typesRegistry.getType(typeBundle, typeName,
                                               null, null, nullable,
                                               schemaName, tableName, columnName);
        return column(schemaName, tableName, columnName, position,
                      type, false, null, null);
    }
    
    public Column column(String schemaName, String tableName, String columnName,
                         Integer position, String typeBundle, String typeName,
                         boolean nullable, boolean autoincrement) {
        TInstance type = typesRegistry.getType(typeBundle, typeName,
                                               null, null, nullable,
                                               schemaName, tableName, columnName);
        return column(schemaName, tableName, columnName, position,
                      type, autoincrement, null, null);
    }
    
    public Column column(String schemaName, String tableName, String columnName,
                         Integer position, String typeBundle, String typeName,
                         Long typeParameter1, Long typeParameter2, boolean nullable) {
        TInstance type = typesRegistry.getType(typeBundle, typeName,
                                               typeParameter1, typeParameter2, nullable,
                                               schemaName, tableName, columnName);
        return column(schemaName, tableName, columnName, position,
                      type, false, null, null);
    }
    
    public Column column(String schemaName, String tableName, String columnName,
                         Integer position, String typeBundle, String typeName,
                         Long typeParameter1, boolean nullable,
                         String charset, String collation) {
        TInstance type = typesRegistry.getType(typeBundle, typeName,
                                               typeParameter1, null, charset, collation, nullable,
                                               schemaName, tableName, columnName);
        return column(schemaName, tableName, columnName, position,
                      type, false, null, null);
    }
    
    public Column column(String schemaName, String tableName, String columnName,
                         Integer position, String typeBundle, String typeName,
                         Long typeParameter1, Long typeParameter2, boolean nullable,
                         String defaultValue, String defaultFunction) {
        TInstance type = typesRegistry.getType(typeBundle, typeName,
                                               typeParameter1, typeParameter2, nullable,
                                               schemaName, tableName, columnName);
        return column(schemaName, tableName, columnName, position,
                      type, false, defaultValue, defaultFunction);
    }
    
    public void parameter(String schemaName, String routineName, 
                          String parameterName, Parameter.Direction direction, 
                          String typeBundle, String typeName,
                          Long typeParameter1, Long typeParameter2) {
        TInstance type = typesRegistry.getType(typeBundle, typeName, typeParameter1, typeParameter2, true, schemaName, routineName, parameterName);
        parameter(schemaName, routineName, parameterName, direction, type);
    }

}

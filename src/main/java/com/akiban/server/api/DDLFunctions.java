/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.api;

import java.util.Collection;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.RowDef;
import com.akiban.server.api.common.NoSuchTableException;
import com.akiban.server.api.ddl.DuplicateColumnNameException;
import com.akiban.server.api.ddl.DuplicateTableNameException;
import com.akiban.server.api.ddl.ForeignConstraintDDLException;
import com.akiban.server.api.ddl.GroupWithProtectedTableException;
import com.akiban.server.api.ddl.IndexAlterException;
import com.akiban.server.api.ddl.JoinToUnknownTableException;
import com.akiban.server.api.ddl.JoinToWrongColumnsException;
import com.akiban.server.api.ddl.NoPrimaryKeyException;
import com.akiban.server.api.ddl.ParseException;
import com.akiban.server.api.ddl.ProtectedTableDDLException;
import com.akiban.server.api.ddl.UnsupportedCharsetException;
import com.akiban.server.api.ddl.UnsupportedDataTypeException;
import com.akiban.server.api.ddl.UnsupportedDropException;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.SchemaId;

public interface DDLFunctions {
    /**
     * Creates a table in a given schema with the given ddl.
     * @param schema may be null; if it is, and the schema must be provided in the DDL text
     * @param ddlText the DDL text: <tt>CREATE TABLE....</tt>
     * @throws ParseException if the given schema is <tt>null</tt> and no schema is provided in the DDL;
     *  or if there is some other parse error
     * @throws UnsupportedCharsetException if the DDL mentions any unsupported charset
     * @throws ProtectedTableDDLException if this would create a protected table, such as any table in the
     *  <tt>akiban_information_schema</tt> schema
     * @throws DuplicateTableNameException if a table by this (schema,name) already exists
     * @throws GroupWithProtectedTableException if the table's DDL would put it in the same group as a protected
     *  table, such as an <tt>akiban_information_schema</tt> table or a group table.
     * @throws JoinToUnknownTableException if the DDL defines foreign keys referring an unknown table
     * @throws JoinToWrongColumnsException if the DDL defines foreign keys referring to the wrong columns on the
     *  parent table (such as columns with different types). In the case of a group join, this exception will
     * also be thrown if the parent FK columns are not exactly equal to the parent's PK columns.
     * @throws NoPrimaryKeyException if the table does not have a PK defined
     * @throws DuplicateColumnNameException if the table defines a (table_name, column_name) pair that already
     * @throws GenericInvalidOperationException if some other exception occurred
     * exists
     */
    void createTable(Session session, String schema, String ddlText)
            throws ParseException,
            UnsupportedCharsetException,
            ProtectedTableDDLException,
            DuplicateTableNameException,
            GroupWithProtectedTableException,
            JoinToUnknownTableException,
            JoinToWrongColumnsException,
            NoPrimaryKeyException,
            DuplicateColumnNameException,
            UnsupportedDataTypeException,
            GenericInvalidOperationException;

    /**
     * Drops a table if it exists.
     * @param tableName the table to drop
     * @throws NullPointerException if tableName is null
     * @throws ProtectedTableDDLException if the given table is protected
     * @throws ForeignConstraintDDLException if dropping this table would create a foreign key violation
     * @throws UnsupportedDropException if this table is not a leaf table
     * @throws GenericInvalidOperationException if some other exception occurred
     */
    void dropTable(Session session, TableName tableName)
            throws ProtectedTableDDLException,
            ForeignConstraintDDLException,
            UnsupportedDropException,
            GenericInvalidOperationException;

    /**
     * Drops a table if it exists, and possibly its children.
     * @param schemaName the schema to drop
     * @throws NullPointerException if schemaName is null
     * @throws ProtectedTableDDLException if the given schema contains protected tables
     * @throws ForeignConstraintDDLException if dropping this schema would create a foreign key violation
     * @throws GenericInvalidOperationException if some other exception occurred
     */
    void dropSchema(Session session, String schemaName)
            throws ProtectedTableDDLException,
            ForeignConstraintDDLException,
            GenericInvalidOperationException;

     /**
     * Drops all tables associated with the group
     * @param groupName the group to drop
     * @throws NullPointerException if groupName is null
     * @throws ProtectedTableDDLException if the given group contains protected tables
     * @throws ForeignConstraintDDLException if dropping this group would create a foreign key violation
     * @throws GenericInvalidOperationException if some other exception occurred
     */
    void dropGroup(Session session, String groupName)
            throws ProtectedTableDDLException,
            ForeignConstraintDDLException,
            NoSuchTableException,
            UnsupportedDropException,
            GenericInvalidOperationException;

    /**
     * Gets the AIS from the Store.
     * @return returns the store's AIS.
     */
    AkibanInformationSchema getAIS(Session session);

    /**
     * Resolves the given table ID to its table's name.
     * @param session the session
     * @param tableId the table to look up
     * @return the table's name
     * @throws NoSuchTableException if the given table doesn't exist
     * @throws NullPointerException if the tableId is null
     */
    TableName getTableName(Session session, int tableId) throws NoSuchTableException;

    /**
     * Resolves the given table name to its table's id.
     * @param session the session
     * @param tableName the table to look up
     * @return the table's id
     * @throws NoSuchTableException if the given table doesn't exist
     * @throws NullPointerException if the tableName is null
     */
    int getTableId(Session session, TableName tableName) throws NoSuchTableException;

    /**
     * Resolves the given table to its Table
     * @param session the session
     * @param tableId the table to look up
     * @return the Table
     * @throws NoSuchTableException if the given table doesn't exist
     */
    public Table getTable(Session session, int tableId) throws NoSuchTableException;

    /**
     * Resolves the given table to its Table
     * @param session the session
     * @param tableName the table to look up
     * @return the Table
     * @throws NoSuchTableException if the given table doesn't exist
     */
    public Table getTable(Session session, TableName tableName) throws NoSuchTableException;
    /**
     * Resolves the given table to its UserTable
     * @param session the session
     * @param tableName the table to look up
     * @return the Table
     * @throws NoSuchTableException if the given table doesn't exist
     */
    public UserTable getUserTable(Session session, TableName tableName) throws NoSuchTableException;

    /**
     * Resolves the given table ID to its RowDef
     * @param tableId the table to look up
     * @return the rowdef
     * @throws NoSuchTableException if the given table doesn't exist
     */
    RowDef getRowDef(int tableId) throws NoSuchTableException;

    /**
     * Retrieves the "CREATE" DDLs for all Akiban tables, including group tables and tables in the
     * <tt>akiban_information_schema</tt> schema. The DDLs will be arranged such that it should be safe to call them
     * in order, but they will not contain any DROP commands; it is up to the caller to drop all conflicting
     * tables. Schemas will be created with <tt>IF EXISTS</tt>, so the caller does not need to drop conflicting
     * schemas.
     * @throws InvalidOperationException if an exception occurred
     * @return the list of CREATE SCHEMA and CREATE TABLE statements that correspond to the chunkserver's known tables
     */
    String getDDLs(Session session) throws InvalidOperationException;

    SchemaId getSchemaID() throws InvalidOperationException;

    /**
     * Forces an increment to the chunkserver's AIS generation ID. This can be useful for debugging.
     * @throws InvalidOperationException if an exception occurred
     */
    @SuppressWarnings("unused") // meant to be used from JMX
    void forceGenerationUpdate() throws InvalidOperationException;
    
    /**
     * Create new indexes on an existing table. All indexes must exist on the same table. Primary
     * keys can not be created through this interface. Specified index IDs will not be used as they
     * are recalculated later. Blocks until the actual index data has been created.
     * @param indexesToAdd a list of indexes to add to the existing AIS
     * @throws IndexAlterException, InvalidOperationException
     */
    void createIndexes(Session session, Collection<Index> indexesToAdd) throws IndexAlterException,
            InvalidOperationException;

    /**
     * Drop indexes on an existing table.
     * 
     * @param tableName the table containing the indexes to drop
     * @param indexesToDrop list of indexes to drop
     * @throws InvalidOperationException
     */
    void dropIndexes(Session session, TableName tableName, Collection<String> indexesToDrop)
            throws InvalidOperationException;
}

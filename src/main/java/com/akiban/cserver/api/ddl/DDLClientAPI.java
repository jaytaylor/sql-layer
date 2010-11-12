package com.akiban.cserver.api.ddl;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.cserver.api.common.TableId;

import java.util.List;
import java.util.Set;

@SuppressWarnings("unused")
public final class DDLClientAPI {

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
     * exists
     */
    public void createTable(String schema, String ddlText)
    throws  ParseException,
            UnsupportedCharsetException,
            ProtectedTableDDLException,
            DuplicateTableNameException,
            GroupWithProtectedTableException,
            JoinToUnknownTableException,
            JoinToWrongColumnsException,
            NoPrimaryKeyException,
            DuplicateColumnNameException

    {
        throw new UnsupportedOperationException();
    }

    /**
     * Drops a table if it exists, and possibly its children. Returns the names of all tables that ended up being
     * dropped; this could be an empty set, if the given tableId doesn't correspond to a known table. If the returned
     * Set is empty (tableId wasn't known), the Set is unmodifiable; otherwise, it is safe to edit.
     * @param tableId the table to drop
     * @throws NullPointerException if tableId is null
     * @throws ProtectedTableDDLException if the given table is protected
     * @throws ForeignConstraintDDLException if dropping this table would create a foreign key violation
     * @return the set of tables that ended up being dropped because of this command
     */
    public Set<TableName> dropTable(TableId tableId)
    throws  ProtectedTableDDLException,
            ForeignConstraintDDLException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns a copy of the chunkserver's AIS. The returned AIS is safe to modify.
     * @return a copy of the chunkserver's AIS
     */
    public AkibaInformationSchema getAIS() {
        throw new UnsupportedOperationException();
    }

    /**
     * Retrieves the "CREATE" DDLs for all Akiban tables, including group tables and tables in the
     * <tt>akiban_information_schema</tt> schema. The DDLs will be arranged such that it should be safe to call them
     * in order, but they will not contain any DROP commands; it is up to the caller to drop all conflicting
     * tables. Schemas will be created with <tt>IF EXISTS</tt>, so the caller does not need to drop conflicting
     * schemas.
     * @return the list of CREATE SCHEMA and CREATE TABLE statements that correspond to the chunkserver's known tables
     */
    public List<String> getDDLs() {
        throw new UnsupportedOperationException();
    }

    /**
     * Forces an increment to the chunkserver's AIS generation ID. This can be useful for debugging.
     */
    public void forceGenerationUpdate() {
        throw new UnsupportedOperationException();
    }
}

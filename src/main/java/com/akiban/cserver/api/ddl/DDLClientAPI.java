package com.akiban.cserver.api.ddl;

import com.akiban.ais.model.TableName;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.common.IdResolverImpl;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.NoSuchTableException;
import com.akiban.cserver.manage.SchemaManager;
import com.akiban.cserver.service.ServiceManager;
import com.akiban.cserver.service.ServiceManagerImpl;
import com.akiban.cserver.store.SchemaId;
import com.akiban.cserver.store.Store;
import com.akiban.message.ErrorCode;
import com.akiban.util.ArgumentValidation;

import java.util.List;

public final class DDLClientAPI {
    
    public static DDLClientAPI instance() {
        ServiceManager serviceManager = ServiceManagerImpl.get();
        if (serviceManager == null) {
            throw new RuntimeException("ServiceManager was not installed");
        }
        Store store = serviceManager.getStore();
        if (store == null) {
            throw new RuntimeException("ServiceManager had no Store");
        }
        return new DDLClientAPI(store.getSchemaManager());
    }

    private final SchemaManager schemaManager;
    private final IdResolverImpl resolver;

    public DDLClientAPI(SchemaManager schemaManager) {
        ArgumentValidation.notNull("schema manager", schemaManager);
        this.schemaManager = schemaManager;
        this.resolver = new IdResolverImpl(schemaManager);
    }

    /**
     * Throws a specific DDLException based on the invalid operation exception specified
     * @param e the cause
     * @throws DDLException the specific exception
     */
    private static void rethrow(Exception e) throws InvalidOperationException {
        if (! (e instanceof InvalidOperationException)) {
            throw new InvalidOperationException(e);
        }
        final InvalidOperationException ioe = (InvalidOperationException)e;
        switch (ioe.getCode()) {
            // TODO FINISH THIS
//            case PARSE_EXCEPTION:
//                throw new ParseException(ioe);
            default:
                throw ioe;
        }
    }

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
            DuplicateColumnNameException,
            InvalidOperationException
    {
        try {
            schemaManager.createTable(schema, ddlText);
        } catch (Exception e) {
            rethrow(e);
        }
    }

    /**
     * Drops a table if it exists, and possibly its children. Returns the names of all tables that ended up being
     * dropped; this could be an empty set, if the given tableId doesn't correspond to a known table. If the returned
     * Set is empty (tableId wasn't known), the Set is unmodifiable; otherwise, it is safe to edit.
     * @param tableId the table to drop
     * @throws NullPointerException if tableId is null
     * @throws ProtectedTableDDLException if the given table is protected
     * @throws ForeignConstraintDDLException if dropping this table would create a foreign key violation
     */
    public void dropTable(TableId tableId)
    throws  ProtectedTableDDLException,
            ForeignConstraintDDLException,
            InvalidOperationException
    {
        final TableName tableName;
        try {
            tableName = tableId.getTableName(resolver);
        }
        catch (NoSuchTableException e) {
            return; // dropping a nonexistent table is a no-op
        }
        
        try {
            schemaManager.dropTable(tableName.getSchemaName(), tableName.getTableName());
        }
        catch (Exception e) {
            rethrow(e);
        }
    }

    /**
     * Drops a table if it exists, and possibly its children. Returns the names of all tables that ended up being
     * dropped; this could be an empty set, if the given tableId doesn't correspond to a known table. If the returned
     * Set is empty (tableId wasn't known), the Set is unmodifiable; otherwise, it is safe to edit.
     * @param schemaName the schema to drop
     * @throws NullPointerException if tableId is null
     * @throws ProtectedTableDDLException if the given schema contains protected tables
     * @throws ForeignConstraintDDLException if dropping this schema would create a foreign key violation
     */
    public void dropSchema(String schemaName)
            throws  ProtectedTableDDLException,
            ForeignConstraintDDLException,
            InvalidOperationException
    {
        try {
            schemaManager.dropSchema(schemaName);
        }
        catch (Exception e) {
            rethrow(e);
        }
    }

    /**
     * Retrieves the "CREATE" DDLs for all Akiban tables, including group tables and tables in the
     * <tt>akiban_information_schema</tt> schema. The DDLs will be arranged such that it should be safe to call them
     * in order, but they will not contain any DROP commands; it is up to the caller to drop all conflicting
     * tables. Schemas will be created with <tt>IF EXISTS</tt>, so the caller does not need to drop conflicting
     * schemas.
     * @return the list of CREATE SCHEMA and CREATE TABLE statements that correspond to the chunkserver's known tables
     */
    public List<String> getDDLs() throws InvalidOperationException {
        try {
            return schemaManager.getDDLs();
        } catch (Exception e) {
            throw new InvalidOperationException(ErrorCode.UNEXPECTED_EXCEPTION, "Unexpected exception", e);
        }
    }

    public SchemaId getSchemaID() throws InvalidOperationException {
        try {
            return schemaManager.getSchemaID();
        } catch (Exception e) {
            throw new InvalidOperationException(ErrorCode.UNEXPECTED_EXCEPTION, "Unexpected exception", e);
        }
    }

    /**
     * Forces an increment to the chunkserver's AIS generation ID. This can be useful for debugging.
     */
    @SuppressWarnings("unused") // meant to be used from JMX
    public void forceGenerationUpdate() throws InvalidOperationException {
        try {
            schemaManager.forceSchemaGenerationUpdate();
        } catch (Exception e) {
            throw new InvalidOperationException(ErrorCode.UNEXPECTED_EXCEPTION, "Unexpected exception", e);
        }
    }
}

package com.akiban.cserver.manage;

import java.util.List;
import java.util.Map;

import com.akiban.ais.model.TableName;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.store.SchemaId;

@SuppressWarnings("unused")
public interface SchemaMXBean {
    static final String SCHEMA_BEAN_NAME = "com.akiban:type=Schema";

    /**
     * Gets the schema's generation ID. Each revision of the schema has a unique
     * ID.
     * 
     * @return the current schema revision's ID
     */
    SchemaId getSchemaID() throws Exception;

    void forceSchemaGenerationUpdate() throws Exception;

    /**
     * Gets the list of known user tables, with each table's DDL.
     * 
     * @return a map of table names to DDL
     * @throws Exception
     *             if there was a problem in retrieving the information
     */
    Map<TableName, String> getUserTables() throws Exception;

    boolean isProtectedTable(int rowDefId);

    boolean isProtectedTable(String schema, String table);

    /**
     * Creates a new table.
     * 
     * @param schemaName
     *            the schema name
     * @param DDL
     *            the DDL, which must start with "create table". The table's
     *            name specified in this DDL may be fully or partially qualified
     *            (that is, it may or may not contain the schemaName).
     * @return the new table's ID
     * @throws Exception
     *             if there was a problem in creating the table
     */
    void createTable(final Session session, String schemaName, String DDL) throws Exception;

    /**
     * Drops a table by name.
     * 
     * @param schema
     *            the schema name
     * @param tableName
     *            the table's name
     * @return I have no idea what this returns
     * @throws Exception
     *             if the table wasn't found
     */
    void dropTable(final Session session, String schema, String tableName) throws Exception;

    void dropAllTables(final Session session) throws Exception;

    /**
     * Drops a schema and all of its tables.
     * 
     * @param schemaName
     *            the schema name
     * @return I have no idea what this returns // TODO
     * @throws Exception
     *             if there was a problem in dropping the schema
     */
    void dropSchema(final Session session, String schemaName) throws Exception;

    /**
     * Gets the current schema's grouping description.
     * 
     * @return a String of the same format as a static grouping string
     * @throws Exception
     *             if there was a problem in getting the information
     */
    public String getGrouping() throws Exception;

    /**
     * Gets a list of DDL statements to be executed on the head to create the
     * group tables and user tables.
     * 
     * @return the list of DDLs
     * @throws Exception
     *             if there was a problem in getting the info
     */
    public List<String> getDDLs() throws Exception;
}

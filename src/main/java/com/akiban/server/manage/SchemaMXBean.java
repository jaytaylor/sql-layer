/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.manage;

import java.util.List;
import java.util.Map;

import com.akiban.ais.model.TableName;
import com.akiban.server.service.session.Session;

@SuppressWarnings("unused")
public interface SchemaMXBean {
    static final String SCHEMA_BEAN_NAME = "com.akiban:type=Schema";

    /**
     * Gets the schema's generation ID. Each revision of the schema has a unique
     * ID.
     * 
     * @return the current schema revision's ID
     */
    int getSchemaGeneration() throws Exception;

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

//    void dropAllTables(final Session session) throws Exception;

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

    /**
     * Change the stored DDL statement for a table that already exists. Does not change the tableid. 
     * 
     * @throws Exception
     */
    public void changeTableDDL(String schemaName, String tableName, String DDL) throws Exception;
}

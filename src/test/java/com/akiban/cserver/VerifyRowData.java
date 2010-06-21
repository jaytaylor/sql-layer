package com.akiban.cserver;


/**
 * Uses JDBC to push and then retrieve rows with arbitrary field values through
 * the AkibaDB engine; verifies that Chunk Server and ASE agree on RowData
 * format.
 * 
 * This is not a unit test; you need a MySQL instance running.
 * 
 * @author peter
 * 
 */
public class VerifyRowData {
    /**
     * Config property name and default for the MySQL server host
     */
    private static final String P_MYSQLHOST = "mysql.host|localhost";

    /**
     * Config property port and default for the MySQL server host
     */
    private static final String P_MYSQLPORT = "mysql.port|3306";

    /**
     * Config property name and default for the MySQL server host
     */
    private static final String P_MYSQLUSER = "mysql.username|akiba";

    /**
     * Config property name and default for the MySQL server host
     */
    private static final String P_MYSQLPASSWORD = "mysql.password|akibaDB";

    private final CServer cserver;

    public VerifyRowData() throws Exception {
        cserver = new CServer(false);
    }

}

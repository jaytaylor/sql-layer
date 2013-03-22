
package com.akiban.sql.embedded;

import java.security.Principal;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

public interface EmbeddedJDBCService
{
    public Driver getDriver();
    public Connection newConnection(Properties properties, Principal principal) throws SQLException;
}

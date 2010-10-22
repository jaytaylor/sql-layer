package com.akiban.ais.loader;

import java.sql.*;
import java.util.logging.Logger;


public class ConnectionUtils
{
    private static Logger logger = Logger.getLogger(ConnectionUtils.class.getName());
    

    public static boolean executeMultiQuery(Connection connection, String multiQuery) throws SQLException
    {
        String[] queries = multiQuery.split(";");
        boolean result = true;
        for(String query : queries)
        {
            query = query.trim();
            if(query == null || query.isEmpty()) {
                continue;
            }
            Statement statement = connection.createStatement();
            result &= statement.execute(query);
        }
        return result;
    }
    
    public static boolean execute(Connection connection, String query) throws SQLException
    {
        Statement statement = connection.createStatement();
        boolean result = statement.execute(query);
        return result;
    }
    
    public static int executeUpdate(Connection connection, String query) throws SQLException
    {
        PreparedStatement statement = connection.prepareStatement(query);
        int result = statement.executeUpdate();
        return result;
    }
    
    public static ResultSet executeQuery(Connection connection, String query) throws SQLException
    {
        ResultSet rs = null;
        PreparedStatement statement = connection.prepareStatement(query);
        rs = statement.executeQuery();
        return rs;
    }
    
    public static Connection connect(String host, String port, String dbName, String userName, String password)
    {
        String url = "jdbc:mysql://" + host + ":" + port + "/" + dbName;
        return connect(url, userName, password);
    }
    
    public static Connection connect(String host, String port, String userName, String password)
    {
        String url = "jdbc:mysql://" + host + ":" + port + "/";
        return connect(url, userName, password);
    }

    private static Connection connect(String url, String userName, String password)
    {
        Connection connection = null;

        try
        {
            Class.forName ("com.mysql.jdbc.Driver").newInstance ();
            logger.info("Creating connection to URL '" + url + "' with u=" + userName + " and p=" + password);
            connection = DriverManager.getConnection (url, userName, password);
            logger.info("Database connection established");
        }
        catch (Exception e)
        {
            throw new RuntimeException("Cannot connect to database server: " + url, e);
        }
        return connection;
    }
    
    public static void disconnect(Connection connection)
    {
        if(connection == null)
        {
            return;
        }
        
        try
        {
            connection.close ();
            logger.info("Database connection closed");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}

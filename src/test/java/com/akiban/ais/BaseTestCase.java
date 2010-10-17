package com.akiban.ais;

import junit.framework.TestCase;

abstract public class BaseTestCase extends TestCase
{
    public static String getDatabaseHost()
    {
        return "localhost";
    }

    public static String getDatabasePort()
    {
        return "3306";
    }
    
    public static String getRootUserName()
    {
        return "root";
    }
    
    public static String getRootPassword()
    {
        return "";
    }
    
    public boolean isDatabaseAvailable()
    {
        return false;
    }
}

package com.akiban.admin;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

public class AdminValue
{
    public String toString()
    {
        return String.format("AdminValue(key: %s, version: %s)", key, version);
    }

    public AdminValue(String key, Integer version, byte[] value)
    {
        this.key = key;
        this.version = version;
        this.properties = new Properties();
        valueString = new String(value);
        try {
            properties.load(new StringReader(valueString));
        } catch (IOException e) {
            throw new Admin.RuntimeException(String.format("Unable to parse properties: %s", valueString));
        }
    }

    public final String key()
    {
        return key;
    }

    public final Integer version()
    {
        return version;
    }

    public Properties properties()
    {
        return properties;
    }

    public String value()
    {
        return valueString;
    }

    private final Integer version;
    private final String key;
    private final Properties properties;
    private final String valueString;
}

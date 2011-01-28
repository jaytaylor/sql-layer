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

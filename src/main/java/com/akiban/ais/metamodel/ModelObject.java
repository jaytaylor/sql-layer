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

package com.akiban.ais.metamodel;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class ModelObject
{
    @Override
    public String toString()
    {
        return name;
    }

    public ModelObject(String name)
    {
        this.name = name;
    }

    public void addAttribute(String name, String typename)
    {
        AttrType type = AttrType.of(typename);
        attributes.put(name, new Attribute(name, type));
    }

    public void readQuery(String readQuery)
    {
        this.readQuery = readQuery;
    }

    public void writeQuery(String writeQuery)
    {
        this.writeQuery = writeQuery;
    }

    public void cleanupQuery(String cleanupQuery)
    {
        this.cleanupQuery = cleanupQuery;
    }
    
    public void tableName(String tableName) {
    	this.tableName = tableName;
    }

    public String name()
    {
        return name;
    }

    public List<Attribute> attributes()
    {
        if (attributeList == null) {
            attributeList = new ArrayList<Attribute>(attributes.values());
        }
        return attributeList;
    }
    
    public String readQuery()
    {
        return readQuery;
    }

    public String writeQuery()
    {
        return writeQuery;
    }

    public String cleanupQuery()
    {
        return cleanupQuery;
    }
    
    public String tableName() {
    	return tableName;
    }
    
    // State

    private final String name;
    private final LinkedHashMap<String, Attribute> attributes = new LinkedHashMap<String, Attribute>();
    private List<Attribute> attributeList;
    private String readQuery;
    private String writeQuery;
    private String cleanupQuery;
    private String tableName;

    // Inner classes

    public enum AttrType
    {
        INTEGER,
        LONG,
        STRING,
        BOOLEAN;

        public static AttrType of(String typename)
        {
            if (typename.equals("String")) {
                return STRING;
            } else if (typename.equals("Integer")) {
                return INTEGER;
            } else if (typename.equals("Long")) {
                return LONG;
            } else if (typename.equals("Boolean")) {
                return BOOLEAN;
            } else {
                return null;
            }
        }
    }

    public class Attribute
    {
        public String name()
        {
            return name;
        }

        public AttrType type()
        {
            return type;
        }

        Attribute(String name, AttrType type)
        {
            this.name = name;
            this.type = type;
        }

        private final String name;
        private final AttrType type;
    }
}

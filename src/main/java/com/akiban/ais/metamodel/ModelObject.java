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

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

package com.akiban.ais.model;

public class IndexName implements Comparable<IndexName>
{
    private final TableName tableName;
    private final String indexName;

    public IndexName(TableName tableName, String indexName)
    {
        this.tableName = tableName;
        this.indexName = indexName;
    }

    @Override
    public String toString()
    {
        return tableName.toString() + "." + indexName;
    }

    public String getSchemaName()
    {
        return tableName.getSchemaName();
    }

    public String getTableName()
    {
        return tableName.getTableName();
    }

    public TableName getFullTableName() {
        return tableName;
    }

    public String getName()
    {
        return indexName;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (! (obj instanceof IndexName))
            return false;
        IndexName o = (IndexName) obj;

        return getSchemaName().equals(o.getSchemaName()) &&
               getTableName().equals(o.getTableName()) &&
               getName().equals(o.getName());
    }
    
    @Override
    public int hashCode()
    {
        return getSchemaName().hashCode() +
               getTableName().hashCode() +
               getName().hashCode();
    }

    @Override
    public int compareTo(IndexName o) {
        int c = tableName.compareTo(o.tableName);
        if(c == 0) {
            c = indexName.compareTo(o.indexName);
        }
        return c;
    }
}

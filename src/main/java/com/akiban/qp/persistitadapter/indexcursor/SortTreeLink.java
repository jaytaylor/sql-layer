
package com.akiban.qp.persistitadapter.indexcursor;

import com.akiban.server.service.tree.TreeCache;
import com.akiban.server.service.tree.TreeLink;

class SortTreeLink implements TreeLink
{
    // TreeLink interface

    @Override
    public String getSchemaName()
    {
        return SCHEMA_NAME;
    }

    @Override
    public String getTreeName()
    {
        return tableName;
    }

    @Override
    public void setTreeCache(TreeCache cache)
    {
        this.cache = cache;
    }

    @Override
    public TreeCache getTreeCache()
    {
        return cache;
    }

    // SortTreeLink interface

    public SortTreeLink(String tableName)
    {
        this.tableName = tableName;
    }

    // Class state

    // All data is currently in one volume. Any schema name can be used to find it. Later, we may want multiple
    // volumes, and a "schema" for temp trees. If/when we do that, the schema name will be significant.
    private static final String SCHEMA_NAME = "TEMP_SCHEMA";

    // Object state

    private final String tableName;
    private TreeCache cache;
}

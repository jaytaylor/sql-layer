/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.qp.persistitadapter.indexcursor;

import com.foundationdb.server.service.tree.TreeCache;
import com.foundationdb.server.service.tree.TreeLink;

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

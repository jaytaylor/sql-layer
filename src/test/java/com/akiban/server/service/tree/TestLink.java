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

package com.akiban.server.service.tree;

class TestLink implements TreeLink {
    final String schemaName;
    final String treeName;
    TreeCache cache;

    TestLink(String s, String t) {
        schemaName = s;
        treeName = t;
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public String getTreeName() {
        return treeName;
    }

    @Override
    public void setTreeCache(TreeCache cache) {
        this.cache = cache;
    }

    @Override
    public TreeCache getTreeCache() {
        return cache;
    }
}
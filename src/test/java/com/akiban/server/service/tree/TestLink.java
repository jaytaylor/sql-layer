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
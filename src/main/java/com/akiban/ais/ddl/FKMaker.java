package com.akiban.ais.ddl;

import com.akiban.ais.model.TableName;
import com.akiban.ais.model.staticgrouping.GroupingVisitorStub;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FKMaker extends GroupingVisitorStub<Map<TableName,String>> {
    private Map<TableName,String> ret = new HashMap<TableName, String>();

    @Override
    public void visitChild(TableName parentName, List<String> parentColumns, TableName childName, List<String> childColumns) {
        assert ! ret.containsKey(childName) : "already saw child " + childName + ": " + ret;
        StringBuilder scratch = new StringBuilder();

        String fkName = scratch.append("__akiban_fk_").append(ret.size()).toString();
        scratch.setLength(0);
        TableName.escape(parentName.getTableName(), scratch);
        String parent = scratch.toString();
        String pCols = columns(parentColumns, scratch);
        String cCols = columns(childColumns, scratch);

        String fk = String.format(",%nCONSTRAINT `%s` FOREIGN KEY `%s` (%s) REFERENCES %s (%s)",
                fkName, fkName, cCols, parent, pCols);
        ret.put(childName, fk);
    }

    private String columns(List<String> columns, StringBuilder scratch) {
        scratch.setLength(0);
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("can't have empty string");
        }
        for (String col : columns) {
            TableName.escape(col, scratch);
            scratch.append(',');
        }
        scratch.setLength(scratch.length() - 1);
        return scratch.toString();
    }

    @Override
    public Map<TableName,String> end() {
        return ret;
    }
}

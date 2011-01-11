package com.akiban.ais.model.staticgrouping;

import java.util.List;

import com.akiban.ais.model.TableName;

public abstract class GroupingVisitorStub<T> implements GroupingVisitor<T> {
    @Override
    public void start(String defaultSchema) {
    }

    @Override
    public void visitGroup(Group group, TableName rootTable) {
    }

    @Override
    public void finishGroup() {
    }

    @Override
    public void visitChild(TableName parentName, List<String> parentColumns, TableName childName, List<String> childColumns) {
    }

    @Override
    public boolean startVisitingChildren() {
        return true;
    }

    @Override
    public void finishVisitingChildren() {
    }
}

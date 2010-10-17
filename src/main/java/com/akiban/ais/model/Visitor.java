package com.akiban.ais.model;

public interface Visitor
{
    void visitType(Type type) throws Exception;
    void visitGroup(Group group) throws Exception;
    void visitUserTable(UserTable userTable) throws Exception;
    void visitGroupTable(GroupTable groupTable) throws Exception;
    void visitColumn(Column column) throws Exception;
    void visitJoin(Join join) throws Exception;
    void visitJoinColumn(JoinColumn joinColumn) throws Exception;
    void visitIndex(Index index) throws Exception;
    void visitIndexColumn(IndexColumn indexColumn) throws Exception;
}

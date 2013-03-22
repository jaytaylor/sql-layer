
package com.akiban.ais.model;

public interface Visitor
{
    void visitType(Type type);
    void visitGroup(Group group);
    void visitUserTable(UserTable userTable);
    void visitColumn(Column column);
    void visitJoin(Join join) ;
    void visitJoinColumn(JoinColumn joinColumn);
    void visitIndex(Index index);
    void visitIndexColumn(IndexColumn indexColumn);
}

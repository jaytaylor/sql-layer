
package com.akiban.ais.model;

public class JoinColumn
{
    public static JoinColumn create(Join join, Column parent, Column child)
    {
        return join.addJoinColumn(parent, child);
    }

    @Override
    public String toString()
    {
        return "JoinColumn(" + child.getName() + " -> " + parent.getName() + ")";
    }

    public JoinColumn(Join join, Column parent, Column child)
    {
        this.join = join;
        this.parent = parent;
        this.child = child;
    }

    public Join getJoin()
    {
        return join;
    }

    public Column getParent()
    {
        return parent;
    }

    public Column getChild()
    {
        return child;
    }

    private final Join join;
    private final Column parent;
    private final Column child;
}

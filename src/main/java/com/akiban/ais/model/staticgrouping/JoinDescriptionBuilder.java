package com.akiban.ais.model.staticgrouping;

import com.akiban.ais.model.TableName;

public final class JoinDescriptionBuilder
{
    interface Callback {
        JoinDescription created(JoinDescriptionBuilder builder,
                                       TableName parent, String firstParentColumn,
                                       TableName child, String firstChildColumn);
    }
    private final TableName parent;
    private final TableName joinChild;
    private final Callback callback;
    private JoinDescription join;

    JoinDescriptionBuilder(TableName parent, TableName child, Callback callback)
    {
        this.parent = parent;
        this.joinChild = child;
        this.callback = callback;
    }

    public JoinDescriptionBuilder column(String parent, String child) {
        if (join == null) {
            join = callback.created(this, this.parent, parent, this.joinChild, child);
        }
        else {
            join.addJoinColumn(parent, child);
        }
        return this;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(JoinDescriptionBuilder.class.getName()).append('[');
        if (join == null) {
            builder.append("unbuilt join ").append(joinChild.escaped());
        }
        else {
            builder.append("valid join ").append(join);
        }
        builder.append(" has parent ").append(parent).append(']');
        return builder.toString();
    }
}

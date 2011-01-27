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

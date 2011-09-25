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

package com.akiban.sql.optimizer.plan;

import com.akiban.ais.model.Group;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** A contiguous set of tables joined together: flattened / producted
 * and acting as a single row set for higher level joins.
 */
public class TableJoins extends BasePlanWithInput implements Joinable
{
    private Group group;

    public TableJoins(Joinable joins, Group group) {
        super(joins);
        this.group = group;
    }

    public Group getGroup() {
        return group;
    }

    public Joinable getJoins() {
        return (Joinable)getInput();
    }

    @Override
    public boolean isTable() {
        return false;
    }
    @Override
    public boolean isGroup() {
        return true;
    }
    @Override
    public boolean isJoin() {
        return false;
    }
    @Override
    public boolean isInnerJoin() {
        return false;
    }

    @Override
    public String summaryString() {
        return super.summaryString() + "(" + group + ")";
    }

}

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

import com.akiban.ais.model.UserTable;

import java.util.List;
import java.util.ArrayList;

/** A table from AIS.
 */
public class TableNode extends TableTreeBase.TableNodeBase<TableNode> 
{
    private List<TableSource> uses;

    public TableNode(UserTable table) {
        super(table);
        uses = new ArrayList<TableSource>();
    }

    public void addUse(TableSource use) {
        uses.add(use);
    }

}

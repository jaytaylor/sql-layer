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

import java.util.List;
import java.util.ArrayList;

public class AncestorLookup_New extends BasePlanWithInput
{
    private TableNode source;
    private List<TableNode> ancestors;
    private List<TableSource> tables;

    public AncestorLookup_New(PlanNode input, TableNode source,
                              List<TableNode> ancestors,
                              List<TableSource> tables) {
        super(input);
        this.source = source;
        this.ancestors = ancestors;
        this.tables = tables;
    }

    public AncestorLookup_New(PlanNode input, TableSource source,
                              List<TableSource> tables) {
        super(input);
        this.source = source.getTable();
        this.tables = tables;
        this.ancestors = new ArrayList<TableNode>(tables.size());
        for (TableSource table : tables) {
            ancestors.add(table.getTable());
        }
    }

    public TableNode getSource() {
        return source;
    }

    public List<TableNode> getAncestors() {
        return ancestors;
    }

    /** The tables that this branch lookup introduces into the stream. */
    public List<TableSource> getTables() {
        return tables;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if ((getInput() == null) || getInput().accept(v)) {
                for (TableSource table : tables) {
                    if (!table.accept(v))
                        break;
                }
            }
        }
        return v.visitLeave(this);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        ancestors = new ArrayList<TableNode>(ancestors);
        tables = duplicateList(tables, map);
    }

    @Override
    public String summaryString() {
        return super.summaryString() + "(" + source + " -> " + ancestors + ")";
    }

}

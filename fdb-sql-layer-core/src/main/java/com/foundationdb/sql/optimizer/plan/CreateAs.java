/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.optimizer.plan;



/** A join to an actual table. */
public class CreateAs extends BaseJoinable implements ColumnSource
{

    TableSource tableSource;

    public CreateAs(){}

    @Override
    public String getName() {
        return "Create As Temporary Node";
    }

    @Override
    public boolean isTable() {
        return false;
    }

    public TableSource getTableSource() {return tableSource;}

    public void setTableSource(TableSource tableSource){ this.tableSource = tableSource;}

    @Override
    public boolean accept(PlanVisitor v) {
        return v.visit(this);
    }

    @Override
    protected boolean maintainInDuplicateMap() {
        return true;
    }


}
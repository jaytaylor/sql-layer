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

import java.util.List;

import com.foundationdb.ais.model.Column;

/** A SQL INSERT statement. */
public class InsertStatement extends BaseUpdateStatement
{
    private List<Column> targetColumns;

    public InsertStatement(PlanNode query, TableNode targetTable,
                           List<Column> targetColumns,
                           TableSource table) {
        super(query, StatementType.INSERT, targetTable, table);
        this.targetColumns = targetColumns;
    }

    public List<Column> getTargetColumns() {
        return targetColumns;
    }
    
    @Override
    protected void fillSummaryString(StringBuilder str) {
        super.fillSummaryString(str);
        str.append(targetColumns);
    }
}

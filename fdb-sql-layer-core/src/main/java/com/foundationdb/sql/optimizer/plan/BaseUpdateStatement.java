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

/** A statement that modifies the database.
 */
public class BaseUpdateStatement extends BasePlanWithInput
{
    public enum StatementType {
        DELETE,
        INSERT,
        UPDATE
    }
    
    private TableNode targetTable;
    private TableSource table;
    private final StatementType type;
    
    protected BaseUpdateStatement(PlanNode query, StatementType type, TableNode targetTable,
                                    TableSource table) {
        super(query);
        this.type = type;
        this.targetTable = targetTable;
        this.table = table;
    }

    public TableNode getTargetTable() {
        return targetTable;
    }


    public TableSource getTable() { 
        return table;
    }

    public StatementType getType() {
        return type;
    }

    @Override
    public String summaryString(SummaryConfiguration configuration) {
        StringBuilder str = new StringBuilder(super.summaryString(configuration));
        str.append('(');
        fillSummaryString(str);
        //if (requireStepIsolation)
        //    str.append(", HALLOWEEN");
        str.append(')');
        return str.toString();
    }

    protected void fillSummaryString(StringBuilder str) {
        str.append(getTargetTable());
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        targetTable = map.duplicate(targetTable);
    }
}

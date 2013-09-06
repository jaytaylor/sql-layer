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

import com.foundationdb.sql.optimizer.plan.ResultSet.ResultField;
import com.foundationdb.sql.optimizer.rule.EquivalenceFinder;

public class DMLStatement extends BaseStatement {

    public DMLStatement(PlanNode input, BaseUpdateStatement.StatementType type, 
                        TableSource selectTable,
                        TableNode targetTable,
                        List<ResultField> results,
                        TableSource returningTable,
                        EquivalenceFinder<ColumnExpression> columnEquivalencies) {
        super(input, columnEquivalencies);
        this.type = type;
        this.selectTable = selectTable;
        this.targetTable = targetTable;
        this.results = results;
        this.returningTable = returningTable;
    }
    
    public BaseUpdateStatement.StatementType getType() {
        return type;
    }
    
    public List<ResultField> getResultField() {
        return results;
    }

    public TableSource getSelectTable() { 
        return selectTable;
    }
    public TableSource getReturningTable() { 
        return returningTable;
    }

    public TableNode getTargetTable() {
        return targetTable;
    }

    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append('(');
        str.append(targetTable);
        str.append(')');
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
    }
    
    private final BaseUpdateStatement.StatementType type; 
    private final List<ResultField> results;
    private final TableSource selectTable, returningTable;
    private TableNode targetTable;

}

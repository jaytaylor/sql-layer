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

package com.akiban.sql.optimizer.rule;

import com.akiban.server.error.UnsupportedSQLException;

import com.akiban.sql.optimizer.plan.*;

import java.util.*;

public class OperatorAssembler extends BaseRule
{
    @Override
    public PlanNode apply(PlanNode plan) {
        return assembleStatement(plan);
    }

    protected BasePlannable assembleStatement(PlanNode plan) {
        if (plan instanceof SelectQuery)
            return selectQuery((SelectQuery)plan);
        else if (plan instanceof InsertStatement)
            return insertStatement((InsertStatement)plan);
        else if (plan instanceof UpdateStatement)
            return updateStatement((UpdateStatement)plan);
        else if (plan instanceof DeleteStatement)
            return deleteStatement((DeleteStatement)plan);
        else
            throw new UnsupportedSQLException("Cannot assemble plan: " + plan, null);
    }

    public PhysicalSelect selectQuery(SelectQuery selectQuery) {
        OperatorResult operator = assembleQuery(selectQuery.getQuery(), null);
        return new PhysicalSelect(operator.getOperator(),
                                  operator.getColumns(),
                                  null);
    }

    public PhysicalUpdate insertStatement(InsertStatement plan) {
        OperatorResult input = assembleQuery(selectQuery.getQuery(),
                                             // TODO: Need insert row type.
                                             null);
        UpdatePlannable plan = new com.akiban.qp.physicaloperator.Insert_Default(...);
        return new PhysicalUpdate(plan, null);
    }

    public PhysicalUpdate updateStatement(UpdateStatement plan) {
        UpdatePlannable plan = new com.akiban.qp.physicaloperator.Update_Default(...);
        return new PhysicalUpdate(plan, null);
    }

    public PhysicalUpdate deleteStatement(DeleteStatement plan) {
        UpdatePlannable plan = new com.akiban.qp.physicaloperator.Delete_Default(...);
        return new PhysicalUpdate(plan, null);
    }

}

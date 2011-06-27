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

package com.akiban.qp.persistitadapter;

import com.akiban.qp.physicaloperator.PhysicalOperator;
import com.akiban.qp.rowtype.RowType;

final class MaintenancePlan {

    PhysicalOperator plan() {
        return operatorPlan;
    }

    RowType flattenedParentRowType() {
        return flattenedParentRowType;
    }

    MaintenancePlan(PhysicalOperator operatorPlan, RowType flattenedParentRowType) {
        this.operatorPlan = operatorPlan;
        this.flattenedParentRowType = flattenedParentRowType;
    }

    private final PhysicalOperator operatorPlan;
    private final RowType flattenedParentRowType;
}

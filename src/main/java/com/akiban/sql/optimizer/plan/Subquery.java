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

/** A marker node around some subquery.
 */
public class Subquery extends BasePlanWithInput
{
    public Subquery(PlanNode input) {
        super(input);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Subquery)) return false;
        Subquery other = (Subquery)obj;
        return getInput().equals(other.getInput());
    }

    @Override
    public int hashCode() {
        return getInput().hashCode();
    }

}

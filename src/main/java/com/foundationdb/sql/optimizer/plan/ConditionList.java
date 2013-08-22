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

import java.util.ArrayList;
import java.util.Collection;

/** A conjunction of boolean conditions used for WHERE / HAVING / ON / ...
 */
public class ConditionList extends ArrayList<ConditionExpression>
{
    public ConditionList() {
        super();
    }

    public ConditionList(int size) {
        super(size);
    }

    public ConditionList(Collection<? extends ConditionExpression> list) {
        super(list);
    }

    public boolean accept(ExpressionVisitor v) {
        for (ConditionExpression condition : this) {
            if (!condition.accept(v)) {
                return false;
            }
        }
        return true;
    }

    public void accept(ExpressionRewriteVisitor v) {
        for (int i = 0; i < size(); i++) {
            set(i, (ConditionExpression)get(i).accept(v));
        }
    }

    public ConditionList duplicate(DuplicateMap map) {
        ConditionList copy = new ConditionList(size());
        for (ConditionExpression cond : this) {
            copy.add((ConditionExpression)cond.duplicate(map));
        }
        return copy;
    }

}

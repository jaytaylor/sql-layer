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

/** An expression in a Project list (the list right after SELECT). */
public class ResultSet extends BasePlanNode
{
    public static class ResultExpression {
        private BaseExpression expression;
        private String name;
        private boolean nameDefaulted;

        public ResultExpression(BaseExpression expression,
                                String name, boolean nameDefaulted) {
            this.expression = expression;
            this.name = name;
            this.nameDefaulted = nameDefaulted;
        }

        public BaseExpression getExpression() {
            return expression;
        }

        public String getName() {
            return name;
        }
        public boolean isNameDefaulted() {
            return nameDefaulted;
        }
        
        public String toString() {
            return expression.toString();
        }
    }

    private List<ResultExpression> results;

    public ResultSet(List<ResultExpression> results) {
        this.results = results;
    }

    public List<ResultExpression> getResults() {
        return results;
    }

    @Override
    public String toString() {
        return results.toString();
    }

}

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

import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.ais.model.Column;

import java.util.List;

/** Name the columns in a SELECT. */
public class ResultSet extends BasePlanWithInput
{
    public static class ResultField extends BaseDuplicatable {
        private String name;
        private DataTypeDescriptor sqlType;
        private Column aisColumn;

        public ResultField(String name, DataTypeDescriptor sqlType, Column aisColumn) {
            this.name = name;
            this.sqlType = sqlType;
            this.aisColumn = aisColumn;
        }

        public ResultField(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public DataTypeDescriptor getSQLtype() {
            return sqlType;
        }

        public Column getAIScolumn() {
            return aisColumn;
        }

        public String toString() {
            return name;
        }
    }

    private List<ResultField> fields;

    public ResultSet(PlanNode input, List<ResultField> fields) {
        super(input);
        this.fields = fields;
    }

    public List<ResultField> getFields() {
        return fields;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            getInput().accept(v);
        }
        return v.visitLeave(this);
    }

    @Override
    public String summaryString() {
        return super.summaryString() + fields;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        fields = duplicateList(fields, map);
    }

}

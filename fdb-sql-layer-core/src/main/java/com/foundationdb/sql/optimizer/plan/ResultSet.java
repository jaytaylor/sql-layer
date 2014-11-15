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

import com.foundationdb.ais.model.ColumnContainer;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.ais.model.Column;

import java.util.List;

/** Name the columns in a SELECT. */
public class ResultSet extends BasePlanWithInput
{
    public static class ResultField extends BaseDuplicatable implements ColumnContainer {

        private String name;
        private DataTypeDescriptor sqlType;
        private Column aisColumn;
        private TInstance type;

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
            if (sqlType == null && type != null) {
                sqlType = type.dataTypeDescriptor();
            }
            return sqlType;
        }

        public Column getAIScolumn() {
            return aisColumn;
        }

        public TInstance getType() {
            return type;
        }

        public void setType(TInstance type) {
            this.type = type;
            if (type != null)
                sqlType = null;
        }

        @Override
        public Column getColumn() {
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
    public String summaryString(SummaryConfiguration configuration) {
        StringBuilder stringBuilder = new StringBuilder(super.summaryString(configuration));
        if (configuration.includeRowTypes) {
            stringBuilder.append('[');
            for (ResultField field : fields) {
                stringBuilder.append(field);
                stringBuilder.append(" (");
                stringBuilder.append(field.getType());
                stringBuilder.append("), ");
            }
            if (fields.size() > 0) {
                stringBuilder.setLength(stringBuilder.length()-2);
            }
            stringBuilder.append(']');
        } else {
            stringBuilder.append(fields);
        }
        return stringBuilder.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        fields = duplicateList(fields, map);
    }

}

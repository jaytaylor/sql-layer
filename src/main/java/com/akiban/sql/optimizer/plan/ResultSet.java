
package com.akiban.sql.optimizer.plan;

import com.akiban.ais.model.ColumnContainer;
import com.akiban.server.types3.TInstance;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.ais.model.Column;

import java.util.List;

/** Name the columns in a SELECT. */
public class ResultSet extends BasePlanWithInput
{
    public static class ResultField extends BaseDuplicatable implements ColumnContainer {

        private String name;
        private DataTypeDescriptor sqlType;
        private Column aisColumn;
        private TInstance TInstance;

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
            if (sqlType == null && TInstance != null) {
                sqlType = TInstance.dataTypeDescriptor();
            }
            return sqlType;
        }

        public Column getAIScolumn() {
            return aisColumn;
        }

        public TInstance getTInstance() {
            return TInstance;
        }

        public void setTInstance(TInstance tInstance) {
            this.TInstance = tInstance;
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
    public String summaryString() {
        return super.summaryString() + fields;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        fields = duplicateList(fields, map);
    }

}

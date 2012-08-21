/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
            return sqlType;
        }

        public Column getAIScolumn() {
            return aisColumn;
        }

        public TInstance getTInstance() {
            return TInstance;
        }

        public void setTInstance(TInstance TInstance) {
            this.TInstance = TInstance;
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

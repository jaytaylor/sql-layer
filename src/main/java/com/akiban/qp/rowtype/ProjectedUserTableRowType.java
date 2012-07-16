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

package com.akiban.qp.rowtype;

import java.util.List;

import com.akiban.ais.model.HKey;
import com.akiban.ais.model.UserTable;
import com.akiban.server.expression.Expression;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.texpressions.TPreparedExpression;

public class ProjectedUserTableRowType extends ProjectedRowType {

    public ProjectedUserTableRowType(DerivedTypesSchema schema, UserTable table, List<? extends Expression> projections, List<? extends TPreparedExpression> tExprs) {
        super(schema, table.getTableId(), projections, tExprs);
        this.table = table;
    }

    @Override
    public UserTable userTable() {
        return table;
    }

    @Override
    public boolean hasUserTable() {
        return table != null;
    }
    
    @Override
    public int nFields()
    {
        return table.getColumns().size();
    }

    @Override
    public HKey hKey()
    {
        return table.hKey();
    }

    @Override
    public String toString()
    {
        return String.format("%s: %s", super.toString(), table);
    }

    private final UserTable table;
    

}

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
package com.akiban.server.service.restdml;


import java.util.ArrayList;
import java.util.List;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.PrimaryKey;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.expression.RowBasedUnboundExpressions;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.API.Ordering;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.TPreparedField;
import com.akiban.server.types3.texpressions.TPreparedParameter;
import com.akiban.sql.optimizer.plan.TableNode;

public class DeleteGenerator extends OperatorGenerator {

    public DeleteGenerator (AkibanInformationSchema ais) {
        super(ais);
    }
    @Override
    protected Operator create(TableName tableName) {
        UserTable table = ais().getUserTable(tableName);
        PrimaryKey pkey = table.getPrimaryKey();
        final int nkeys = pkey.getColumns().size();

        UserTableRowType tableType = schema().userTableRowType(table);
        IndexRowType indexType = schema().indexRowType(pkey.getIndex());

        // prepared parameters for all values in a index
        // build a primary key index scan
        List<TPreparedExpression> pexprs = new ArrayList<TPreparedExpression>(nkeys);
        for (int i = 0; i < nkeys; i++) {
            pexprs.add(new TPreparedParameter(i, indexType.typeInstanceAt(i)));
        }
        IndexBound bound = 
            new IndexBound(new RowBasedUnboundExpressions(indexType, null, pexprs),
                           new ColumnSelector() {
                               @Override
                               public boolean includesColumn(int columnPosition) {
                                   return columnPosition < nkeys;
                               }
                           });
        IndexKeyRange indexRange = IndexKeyRange.bounded(indexType,
                                                         bound, true,
                                                         bound, true);

        Ordering ordering = API.ordering(true);
        
        for (int i = 0; i < nkeys; i++) {
            ordering.append(null, 
                            new TPreparedField(indexType.typeInstanceAt(i), i), 
                            false);
        }
        
        Operator indexScan = API.indexScan_Default(indexType, indexRange, ordering,
                                                   true);
        
        // build ancestor lookup default
        Group group = table.getGroup(); 
        List<UserTableRowType> ancestorType = new ArrayList<>(1);
        ancestorType.add (tableType);
        
        Operator lookup = API.ancestorLookup_Default(indexScan,
                group,
                indexType,
                ancestorType,
                API.InputPreservationOption.DISCARD_INPUT);
        
        // build delete operator.
        return API.delete_Returning(lookup, true);
    }       
    
}

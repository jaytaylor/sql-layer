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

package com.akiban.qp.util;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.error.PersistitAdapterException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.Store;
import com.akiban.sql.aisddl.TableCopier;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;

import java.util.Arrays;
import java.util.Collections;

import static com.akiban.qp.operator.API.filter_Default;
import static com.akiban.qp.operator.API.groupScan_Default;
import static com.akiban.qp.operator.API.insert_Default;
import static com.akiban.qp.operator.API.project_Table;

public class OperatorBasedTableCopier implements TableCopier {
    private final ConfigurationService config;
    private final TreeService treeService;
    private final Session session;
    private final Store store;
    private final boolean usePVals;

    public OperatorBasedTableCopier(ConfigurationService config, TreeService treeService, Session session, Store store,
                                    boolean usePVals) {
        this.config = config;
        this.treeService = treeService;
        this.session = session;
        this.store = store;
        this.usePVals = usePVals;
    }

    @Override
    public void copyFullTable(AkibanInformationSchema ais, TableName source, TableName destination) {
        Schema schema = SchemaCache.globalSchema(ais);
        UserTable sourceTable = ais.getUserTable(source);

        RowType sourceType = schema.userTableRowType(sourceTable);
        RowType destType = schema.userTableRowType(ais.getUserTable(destination));
        if(sourceType.nFields() != destType.nFields()) {
            throw new IllegalArgumentException("Column count must match exactly");
        }

        Expression[] projections = new Expression[sourceType.nFields()];
        for(int i = 0; i < sourceType.nFields(); ++i) {
            projections[i] = new FieldExpression(sourceType, i);
        }

        Operator plan = project_Table(
                filter_Default(
                        groupScan_Default(sourceTable.getGroup().getGroupTable()),
                        Collections.singleton(sourceType)
                ),
                sourceType,
                destType,
                Arrays.asList(projections),
                null
        );

        UpdatePlannable cursor = insert_Default(plan, usePVals);

        // TODO: Clean this up when ALTER exposed through DDL API
        StoreAdapter adpater = new PersistitAdapter(schema, store, treeService, session, config);
        QueryContext context = new SimpleQueryContext(adpater);
        Transaction txn = treeService.getTransaction(session);
        try {
            txn.begin();
            cursor.run(context);
            txn.commit();
        } catch(PersistitException e) {
            if(txn.isActive()) {
                txn.rollback();
            }
            throw new PersistitAdapterException(e);
        } finally {
            if(txn.isActive()) {
                txn.end();
            }
        }
    }
}

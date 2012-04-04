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

package com.akiban.server.service.memcache.hprocessor;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiRequestException;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public final class DefaultProcessedRequest extends BaseHapiProcessedGetRequest {
    private final AkibanInformationSchema ais;
    private final Set<String> projectedTables;

    public DefaultProcessedRequest(HapiGetRequest request, AkibanInformationSchema ais)
            throws HapiRequestException
    {
        super(request);
        this.ais = ais;

        final UserTable resultRoot = ais.getUserTable(request.getSchema(), request.getTable());
        if (resultRoot == null) {
            throw new HapiRequestException("result table unknown: "
                    + new TableName(request.getSchema(), request.getTable()),
                    HapiRequestException.ReasonCode.UNKNOWN_IDENTIFIER);
        }
        final UserTable predicateTable = ais.getUserTable(request.getUsingTable());
        if (predicateTable == null) {
            throw new HapiRequestException("predicate table unknown: "
                    + new TableName(request.getSchema(), request.getTable()),
                    HapiRequestException.ReasonCode.UNKNOWN_IDENTIFIER);
        }
        projectedTables = getTablesProjection(resultRoot, predicateTable);
    }

    static Set<String> getTablesProjection(UserTable resultRoot, UserTable predicateTable) {
        Set<String> projection = new HashSet<String>();

        // ancestors
        UserTable projectTable = predicateTable;
        while (projectTable != null) {
            projection.add(projectTable.getName().getTableName());
            if (projectTable.getName().equals(resultRoot.getName())) {
                break;
            }
            projectTable = projectTable.getParentJoin().getParent();
        }

        // children
        projectToChildren(predicateTable, projection);

        return Collections.unmodifiableSet(projection);
    }

    private static void projectToChildren(UserTable parent, Set<String> projection) {
        for (Join childJoin : parent.getChildJoins()) {
            UserTable child = childJoin.getChild();
            projection.add(child.getName().getTableName());
            projectToChildren(child, projection);
        }
    }

    @Override
    public Set<String> getProjectedTables() {
        return projectedTables;
    }

    @Override
    public RowDef getRowDef(int tableId) throws IOException {
        UserTable userTable = ais.getUserTable(tableId);
        if (userTable == null) {
            throw new IOException("no table with tableId=" + tableId);
        }
        RowDef rowDef = userTable.rowDef();
        try {
            return rowDef;
        } catch (ClassCastException e) {
            throw new IOException("while casting RowDef for tableId=" + tableId, e);
        }
    }

    @Override
    public AkibanInformationSchema akibanInformationSchema()
    {
        return ais;
    }
}

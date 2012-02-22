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

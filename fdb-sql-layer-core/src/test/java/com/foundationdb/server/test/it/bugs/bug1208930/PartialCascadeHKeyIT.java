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

package com.foundationdb.server.test.it.bugs.bug1208930;

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.test.it.qp.OperatorITBase;

import org.junit.Test;

public class PartialCascadeHKeyIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        w = createTable(
            "s", "w",
            "wid INT NOT NULL",
            "PRIMARY KEY(wid)");
        d = createTable(
            "s", "d",
            "wid INT NOT NULL",
            "did INT NOT NULL",
            "PRIMARY KEY(wid, did)",
            "GROUPING FOREIGN KEY(wid) REFERENCES w(wid)");
        c = createTable(
            "s", "c",
            "wid INT NOT NULL", 
            "did INT NOT NULL",
            "cid INT NOT NULL", 
            "PRIMARY KEY(wid, did, cid)", 
            "GROUPING FOREIGN KEY(wid, did) REFERENCES d(wid, did)");
        o = createTable(
            "s", "o",
            "wid INT NOT NULL",
            "did INT NOT NULL",
            "cid INT NOT NULL",
            "oid INT NOT NULL",
            "PRIMARY KEY(wid, did, oid)",
            "GROUPING FOREIGN KEY(wid, did, cid) REFERENCES c(wid, did, cid)");
        i = createTable(
            "s", "i",
            "wid INT NOT NULL",
            "did INT NOT NULL",
            "oid INT NOT NULL",
            "iid INT NOT NULL",
            "PRIMARY KEY(wid, did, iid)",
            "GROUPING FOREIGN KEY(wid, did, oid) REFERENCES o(wid, did, oid)");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        schema = new com.foundationdb.qp.rowtype.Schema(ais());
        wRowType = schema.tableRowType(table(w));
        dRowType = schema.tableRowType(table(d));
        cRowType = schema.tableRowType(table(c));
        oRowType = schema.tableRowType(table(o));
        iRowType = schema.tableRowType(table(i));
        wOrdinal = ddl().getTable(session(), w).getOrdinal();
        dOrdinal = ddl().getTable(session(), d).getOrdinal();
        cOrdinal = ddl().getTable(session(), c).getOrdinal();
        oOrdinal = ddl().getTable(session(), o).getOrdinal();
        iOrdinal = ddl().getTable(session(), i).getOrdinal();
        group = group(c);
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        loadDatabase();
    }

    private void loadDatabase()
    {
        db = new Row[] {
            row(w, 1L),
            row(d, 1L, 11L),
            row(c, 1L, 11L, 111L),
            row(o, 1L, 11L, 111L, 1111L),
            row(i, 1L, 11L, 1111L, 11111L),
        };
        use(db);
    }

    @Test
    public void testHKeys()
    {
        Operator plan = API.groupScan_Default(group);
        Row[] expected = new Row[] {
            row("{1,(long)1}", wRowType, 1L),
            row("{1,(long)1,2,(long)11}", dRowType, 1L, 11L),
            row("{1,(long)1,2,(long)11,3,(long)111}", cRowType, 1L, 11L, 111L),
            row("{1,(long)1,2,(long)11,3,(long)111,4,(long)1111}", oRowType, 1L, 11L, 111L, 1111L),
            row("{1,(long)1,2,(long)11,3,(long)111,4,(long)1111,5,(long)11111}", iRowType, 1L, 11L, 1111L, 11111L),
        };
        compareRows(expected, API.cursor(plan, queryContext, queryBindings));
    }

    private int w, d, c, o, i;
    private TableRowType wRowType, dRowType, cRowType, oRowType, iRowType;
    private Group group;
    private int wOrdinal, dOrdinal, cOrdinal, oOrdinal, iOrdinal;
}

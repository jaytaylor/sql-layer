package com.foundationdb.server.test.mt.util; /**
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

import com.foundationdb.server.test.mt.OnlineMTBase;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.sql.types.DataTypeDescriptor;
import java.util.List;

import static org.junit.Assert.assertNotNull;

/** Interleaved DML during an online create index for a single table. */
public class OnlineCreateTableAsBase extends OnlineMTBase {
    protected static final String FROM_TABLE = "ft";
    protected static final String TO_TABLE = "tt";
    protected static String CREATE_QUERY;
    protected int ftID;
    protected int ttID;

    protected TableRowType fromTableRowType;
    protected TableRowType toTableRowType;
    protected List<Row> fromGroupRows;
    protected List<Row> toGroupRows;
    protected List<Row> otherGroupRows;
    protected List<String> columnNames;
    protected List<DataTypeDescriptor> fromDescriptors;
    protected List<DataTypeDescriptor> toDescriptors;
    protected TestSession server;

    //public void setupCasting

    @Override
    protected String getDDL() {
        return CREATE_QUERY;
    }

    @Override
    protected String getDDLSchema() {
        return SCHEMA_NAME;
    }

    @Override
    protected List<Row> getGroupExpected() {
        return fromGroupRows;
    }

    @Override
    protected List<Row> getOtherExpected() {
        return otherGroupRows;
    }

    protected List<Row> getToExpected(){
        return toGroupRows;
    }


    @Override
    protected OperatorCreator getGroupCreator() {
        return groupScanCreator(ftID);
    }

    @Override
    protected OperatorCreator getOtherCreator() {
        ttID = ais().getTable(SCHEMA_NAME, TO_TABLE).getTableId();
        return groupScanCreator(ttID);
    }

    @Override
    protected void postCheckAIS(AkibanInformationSchema ais) {
        Table newTable = ais().getTable(SCHEMA_NAME, TO_TABLE);
        assertNotNull("new table", newTable);
    }
}
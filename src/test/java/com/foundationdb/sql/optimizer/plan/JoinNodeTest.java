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

import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class JoinNodeTest {

    protected TableSource source1;
    protected TableSource source2;

    @Before
    public void setup() {
        TypesTranslator typesTranslator = MTypesTranslator.INSTANCE;
        AkibanInformationSchema ais = AISBBasedBuilder.create("s", typesTranslator)
                                                      .table("t1")
                                                      .colString("first_name", 32)
                                                      .colString("last_name", 32)
                                                      .table("t2")
                                                      .colString("first_name", 32)
                                                      .colString("last_name", 32)
                                                      .ais();
        Table table1 = ais.getTable("s", "t1");
        TableNode node1 = new TableNode(table1, new TableTree());
        source1 = new TableSource(node1, true, "t1");

        Table table2 = ais.getTable("s", "t2");
        TableNode node2 = new TableNode(table2, new TableTree());
        source2 = new TableSource(node2, true, "t2");
    }

    @Test
    public void TestDuplicate() {
        JoinNode joinNode = new JoinNode((Joinable)source1, (Joinable)source2, JoinNode.JoinType.LEFT);
        JoinNode duplicate = (JoinNode)joinNode.duplicate();

        assertEquals(joinNode.getJoinType(), duplicate.getJoinType());
        assertEquals(joinNode.getJoinConditions(), duplicate.getJoinConditions());
        assertEquals(joinNode.getImplementation(), duplicate.getImplementation());
    }
}

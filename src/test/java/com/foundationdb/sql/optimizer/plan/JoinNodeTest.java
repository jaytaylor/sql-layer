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

    TypesTranslator typesTranslator;
    AkibanInformationSchema ais;
    Table table;
    TableNode node;
    TableSource source;

    @Before
    public void setup() {
        typesTranslator = MTypesTranslator.INSTANCE;
        ais = AISBBasedBuilder.create("s", typesTranslator)
                              .table("t1").colString("first_name", 32)
                              .colString("last_name", 32)
                              .ais();
        table = ais.getTable("s", "t1");
        node = new TableNode(table, new TableTree());
        source = new TableSource(node, true, "t1");
    }

    @Test
    public void TestDuplicate() {
        JoinNode joinNode = new JoinNode((Joinable)source, (Joinable)source, JoinNode.JoinType.LEFT);
        JoinNode duplicate = (JoinNode)joinNode.duplicate(); // shouldn't throw null error
        assertEquals(joinNode.getJoinType(), duplicate.getJoinType());
        assertEquals(joinNode.getJoinConditions(), duplicate.getJoinConditions());
        assertEquals(joinNode.getImplementation(), duplicate.getImplementation());
    }
}

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
import com.foundationdb.sql.NamedParamsTestBase;
import com.foundationdb.sql.optimizer.OptimizerTestBase;
import com.foundationdb.sql.optimizer.rule.ASTStatementLoader;
import com.foundationdb.sql.optimizer.rule.BaseRule;
import com.foundationdb.sql.optimizer.rule.PlanContext;
import com.foundationdb.sql.optimizer.rule.RulesTestContext;
import com.foundationdb.sql.parser.DMLStatementNode;
import com.foundationdb.sql.parser.JoinNode.JoinType;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

import java.io.File;
import java.util.*;

public class JoinNodeTest {
    @Test
    public void TestDuplicate() {
        TypesTranslator typesTranslator = MTypesTranslator.INSTANCE;
        AkibanInformationSchema ais = AISBBasedBuilder.create("s", typesTranslator)
            .table("t1").colString("first_name", 32).colString("last_name", 32)
            .ais();
        Table table = ais.getTable("s", "t1");
        TableNode node = new TableNode(table, new TableTree());
        TableSource source = new TableSource(node, true, "t1");

        JoinNode joinNode = new JoinNode((Joinable)source, (Joinable)source, JoinNode.JoinType.LEFT);
        JoinNode duplicate = (JoinNode)joinNode.duplicate();

        assertEquals(joinNode.getJoinType(), duplicate.getJoinType());
    }
}

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

package com.foundationdb.server.types.common.types;

import com.foundationdb.server.types.TInstance;

import com.foundationdb.sql.parser.DMLStatementNode;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.types.DataTypeDescriptor;

import org.junit.Ignore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Ignore
public class TypesTranslatorTest
{
    protected final TypesTranslator typesTranslator;

    protected TypesTranslatorTest(TypesTranslator typesTranslator) {
        this.typesTranslator = typesTranslator;
    }

    protected DataTypeDescriptor parseType(String typeString) throws Exception {
        String sql = String.format("SELECT CAST(x AS %s)", typeString);
        StatementNode stmt = new SQLParser().parseStatement(sql);
        return ((DMLStatementNode)stmt).getResultSetNode().getResultColumns()
            .get(0).getExpression().getType();
    }

    protected void testType(String typeString, String expected) throws Exception {
        TInstance type = typesTranslator.typeForSQLType(parseType(typeString));
        assertNotNull(typeString, type);
        assertEquals(typeString, expected, type.toStringIgnoringNullability(false));
    }

}

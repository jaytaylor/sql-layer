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

package com.foundationdb.ais.util;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.JoinColumn;
import com.foundationdb.ais.model.Type;
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.ais.model.Visitor;

public class AISPrinter
{
    public static void print(AkibanInformationSchema ais) 
    {
        print(ais, System.out);
    }

    public static void print(AkibanInformationSchema ais, PrintStream output) 
    {
        ais.traversePreOrder(visitor(new PrintWriter(output)));
    }

    public static void print(AkibanInformationSchema ais, PrintWriter output) 
    {
        ais.traversePreOrder(visitor(output));
    }

    public static String toString(AkibanInformationSchema ais) 
    {
        StringWriter aisBuffer = new StringWriter();
        print(ais, new PrintWriter(aisBuffer));
        return aisBuffer.toString();
    }

    private static Visitor visitor(final PrintWriter output)
    {
        return new Visitor()
        {
            private static final String INDENT = "  ";
            
            @Override
            public void visitType(Type type) 
            {
                output.println(type);
            }

            @Override
            public void visitGroup(Group group) 
            {
                output.println(group);
            }

            @Override
            public void visitUserTable(UserTable userTable) 
            {
                output.println(userTable);
            }

            @Override
            public void visitColumn(Column column) 
            {
                output.print(INDENT);
                output.println(column);
            }

            @Override
            public void visitJoin(Join join) 
            {
                output.println(join);
            }

            @Override
            public void visitJoinColumn(JoinColumn joinColumn) 
            {
                output.print(INDENT);
                output.println(joinColumn);
            }

            @Override
            public void visitIndex(Index index) 
            {
                output.print(INDENT);
                output.println(index);
            }

            @Override
            public void visitIndexColumn(IndexColumn indexColumn) 
            {
                output.print(INDENT);
                output.print(INDENT);
                output.println(indexColumn);
            }
        };
    }
}

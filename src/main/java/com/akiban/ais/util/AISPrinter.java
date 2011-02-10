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

package com.akiban.ais.util;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.Visitor;

public class AISPrinter
{
    public static void print(AkibanInformationSchema ais) throws Exception
    {
        print(ais, System.out);
    }

    public static void print(AkibanInformationSchema ais, PrintStream output) throws Exception
    {
        ais.traversePreOrder(visitor(new PrintWriter(output)));
    }

    public static void print(AkibanInformationSchema ais, PrintWriter output) throws Exception
    {
        ais.traversePreOrder(visitor(output));
    }

    public static String toString(AkibanInformationSchema ais) throws Exception
    {
        StringWriter aisBuffer = new StringWriter();
        print(ais, new PrintWriter(aisBuffer));
        return aisBuffer.toString();
    }

    private static Visitor visitor(final PrintWriter output)
    {
        return new Visitor()
        {
            @Override
            public void visitType(Type type) throws Exception
            {
                output.println(type);
            }

            @Override
            public void visitGroup(Group group) throws Exception
            {
                output.println(group);
            }

            @Override
            public void visitUserTable(UserTable userTable) throws Exception
            {
                output.print("    ");
                output.println(userTable);
            }

            @Override
            public void visitGroupTable(GroupTable groupTable) throws Exception
            {
                output.print("    ");
                output.println(groupTable);
            }

            @Override
            public void visitColumn(Column column) throws Exception
            {
                output.print("        ");
                output.println(column);
            }

            @Override
            public void visitJoin(Join join) throws Exception
            {
                output.print("    ");
                output.println(join);
            }

            @Override
            public void visitJoinColumn(JoinColumn joinColumn) throws Exception
            {
                output.print("        ");
                output.println(joinColumn);
            }

            @Override
            public void visitIndex(Index index) throws Exception
            {
                output.print("        ");
                output.println(index);
            }

            @Override
            public void visitIndexColumn(IndexColumn indexColumn) throws Exception
            {
                output.print("            ");
                output.println(indexColumn);
            }
        };
    }
}
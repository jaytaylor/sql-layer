package com.akiban.ais.util;

import com.akiban.ais.model.*;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;

public class AISPrinter
{
    public static void print(AkibaInformationSchema ais) throws Exception
    {
        print(ais, System.out);
    }

    public static void print(AkibaInformationSchema ais, PrintStream output) throws Exception
    {
        ais.traversePreOrder(visitor(new PrintWriter(output)));
    }

    public static void print(AkibaInformationSchema ais, PrintWriter output) throws Exception
    {
        ais.traversePreOrder(visitor(output));
    }

    public static String toString(AkibaInformationSchema ais) throws Exception
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
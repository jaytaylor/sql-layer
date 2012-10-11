/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.ais.util;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.Visitor;

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
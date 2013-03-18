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

package com.akiban.direct;

import java.io.PrintWriter;
import java.util.Stack;

/**
 * Helper methods for writing a Java class to a PrintWriter
 * 
 * @author peter
 * 
 */
abstract class ClassSourceWriterBase extends ClassBuilder {

    private final PrintWriter writer;
    
    protected int indentation;
    protected final Stack<String> classNames = new Stack<String>();
    protected String[] imports;

    public ClassSourceWriterBase(final PrintWriter writer, final String packageName) {
        super(packageName);
        this.writer = writer;
    }

    @Override
    public void close() {
        writer.close();
    }

    protected void print(String... strings) {
        for (int i = 0; i < indentation; i++) {
            writer.print("    ");
        }
        append(strings);
    }

    protected void append(final String... strings) {
        for (final String s : strings) {
            writer.print(s);
        }
    }

    protected void println(String... strings) {
        print(strings);
        newLine();
    }

    protected void newLine() {
        writer.println();
    }

    protected String shortName(String fqn) {
        final int index = Math.max(fqn.lastIndexOf('$'), fqn.lastIndexOf('.'));
        if (index < 0) {
            return fqn;
        } else {
            return fqn.substring(index + 1);
        }
    }
    
    protected String externalName(String fqn) {
        return fqn.replace('$', '.');
    }
}

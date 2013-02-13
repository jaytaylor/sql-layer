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
public class ClassSourceWriter extends ClassBuilder {

    private final PrintWriter writer;
    private final boolean isAbstract;
    private final Stack<String> classNames = new Stack<String>();
    private String[] imports;
    private int indentation = 0;

    public ClassSourceWriter(final PrintWriter writer, final String packageName, final String schema,
            boolean isAbstract) {
        super(packageName, schema);
        this.writer = writer;
        this.isAbstract = isAbstract;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.akiban.direct.ClassBuilder#preamble(java.lang.String[])
     */
    @Override
    public void preamble(final String[] imports) {
        println("package " + packageName + ";");
        newLine();
        for (final String s : imports) {
            println("import " + s + ";");
        }
        this.imports = imports;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.akiban.direct.ClassBuilder#startInterface(java.lang.String)
     */
    @Override
    public void startClass(final String name, final boolean isInterface) {
        newLine();
        println("public " + (isAbstract ? "abstract " : "") + (isInterface ? "interface " : "class ")
                + shortName(name) + " {");
        indentation++;
        classNames.push(name);
    }

    @Override
    public void addMethod(final String name, final String returnType, final String[] argumentTypes,
            final String[] argumentNames, final String[] body) {
        newLine();
        print(localName(returnType, classNames.peek()), " ", name, "(");
        boolean first = true;
        int counter = 0;
        for (final String s : argumentTypes) {
            if (!first) {
                append(", ");
            }
            String argName;
            if (argumentTypes != null) {
                argName = argumentNames[counter];
                counter++;
            } else {
                argName = "z" + ++counter;
            }
            append(s, " ", argName);
        }
        append(")");
        if (body == null) {
            append(";");
        } else {
            append(" {");
            newLine();
            indentation++;
            for (String s : body) {
                println(s);
            }
            indentation--;
            println("}");
        }
        newLine();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.akiban.direct.ClassBuilder#end()
     */
    @Override
    public void end() {
        indentation--;
        println("}");
        if (indentation == 0) {
            writer.close();
        }
        classNames.pop();
    }

    private void print(String... strings) {
        for (int i = 0; i < indentation; i++) {
            writer.print("    ");
        }
        append(strings);
    }

    private void append(final String... strings) {
        for (final String s : strings) {
            writer.print(s);
        }
    }

    private void println(String... strings) {
        print(strings);
        newLine();
    }

    private void newLine() {
        writer.println();
    }

    private String localName(String fqn, String className) {
        final int index = Math.max(fqn.lastIndexOf('$'), fqn.lastIndexOf('.'));
        if (index < 0) {
            return fqn;
        }
        boolean shorten = false;
        for (final String s : imports) {
            if (s.equals(fqn)) {
                shorten = true;
                break;
            }
        }
        if (!shorten && (className.equals(fqn) || className.equals(fqn.substring(0, index - 1)))) {
            shorten = true;
        }
        if (shorten) {
            return fqn.substring(index + 1);
        } else {
            return fqn;
        }
    }

    private String shortName(String fqn) {
        final int index = Math.max(fqn.lastIndexOf('$'), fqn.lastIndexOf('.'));
        if (index < 0) {
            return fqn;
        } else {
            return fqn.substring(index + 1);
        }
    }
}

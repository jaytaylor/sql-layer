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

/**
 * Helper methods for writing a Java class to a PrintWriter
 * @author peter
 *
 */
public class ClassSourceWriter extends ClassBuilder {

    final PrintWriter writer;
    final String packageName;
    final String className;
    final boolean isAbstract;
    final boolean isInterface;
    int indentation = 0;
    
    public ClassSourceWriter(final PrintWriter writer, final String packageName, final String className, boolean isAbstract, boolean isInterface) {
        this.writer = writer;
        this.packageName = packageName;
        this.className = className;
        this.isAbstract = isAbstract;
        this.isInterface = isInterface;
    }
    
    /* (non-Javadoc)
     * @see com.akiban.direct.ClassBuilder#preamble(java.lang.String[])
     */
    @Override
    public void preamble(final String[] imports) {
        println("package " + packageName + ";");
        newLine();
        for (final String s : imports) {
            println("import " + s + ";");
        }
    }
    
    /* (non-Javadoc)
     * @see com.akiban.direct.ClassBuilder#startInterface(java.lang.String)
     */
    @Override
    public void startClass(final String name) {
        newLine();
        println(publicModifier() + (isAbstract ? "abstract ": "")  + (isInterface ? "interface " : "class ") + name + " {");
        indentation++;
    }
    
    @Override
    public void addMethod(final String name, final String returnType, final String[] argumentTypes, final String[] argumentNames, final String[] body) {
        newLine();
        print(returnType, " ", name, "(");
        boolean first  = true;
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
 
    
    /* (non-Javadoc)
     * @see com.akiban.direct.ClassBuilder#end()
     */
    @Override
    public void end() {
        indentation--;
        println("}");
        if (indentation == 0) {
            writer.close();
        }
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
    
    private String publicModifier() {
        return isInterface ? "" : "public ";
    }
    
}

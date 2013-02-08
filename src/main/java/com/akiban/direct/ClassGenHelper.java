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
public class ClassGenHelper {

    final PrintWriter writer;
    final String packageName;
    final String className;
    int indentation = 0;
    
    public ClassGenHelper(final PrintWriter writer, final String packageName, final String className) {
        this.writer = writer;
        this.packageName = packageName;
        this.className = className;
    }
    
    public void preamble(final String[] imports) {
        println("package " + packageName + ";");
        newLine();
        for (final String s : imports) {
            println("import " + s + ";");
        }
    }
    
    public void startInterface(final String name) {
        println("interface " + name + " {");
        indentation++;
    }
    
    public void method(final String name, final String returnType, final String[] argumentTypes) {
        print(returnType, " ", name, "(");
        boolean first  = true;
        int counter = 0;
        for (final String s : argumentTypes) {
            if (!first) {
                append(", ");
            }
            append(s, " z" + ++counter);
        }
        append(");");
        newLine();
    }
    
    public void property(final String name, final String type) {
        String caseConverted = asJavaName(name, true);
        boolean b = "boolean".equals(type);
        method((b ? "is" : "get") + caseConverted, type, new String[0]);
        method("set" + caseConverted, "void", new String[]{type});
    }
    
    
    public void end() {
        indentation--;
        println("}");
        writer.flush();
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

    public void newLine() {
        writer.println();
    }
    
    public void close() {
        writer.close();
    }
    
    /*
     * Complete demo hack for now.  Remove file s from table name (because we
     * want a singular name in the generated code). Make other assumptions
     * about uniqueness, etc.
     */
    public static String asJavaName(final String name, final boolean toUpper) {
        StringBuilder sb = new StringBuilder(name);
        if (sb.length() > 1 && name.charAt(sb.length() - 1) == 's') {
            if (sb.length() > 2 && name.charAt(sb.length()  -2) == 'e') {
                sb.setLength(sb.length() - 2);
            } else {
                sb.setLength(sb.length() - 1);
            }
        }
        boolean isUpper = Character.isUpperCase(sb.charAt(0));
        if (toUpper && !isUpper) {
            sb.setCharAt(0, Character.toUpperCase(sb.charAt(0)));
        }
        if (!toUpper && isUpper) {
            sb.setCharAt(0, Character.toLowerCase(sb.charAt(0)));
        }
        return sb.toString();
        
    }
}

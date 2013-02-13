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

public abstract class ClassBuilder {

    protected final String packageName;
    protected String schema;

    
    public abstract void preamble(String[] imports);

    public abstract void startClass(String name, boolean isInterface);
    
    public abstract void addMethod(String name, String returnTuype, String[] argumentTypes, String[] argumentNames, String[] body);
    
    protected ClassBuilder(String packageName, String schema) {
        this.packageName = packageName;
        this.schema = schema;
    }

    /* (non-Javadoc)
     * @see com.akiban.direct.ClassBuilder#property(java.lang.String, java.lang.String)
     */
    public void addProperty(final String name, final String type, final String argName, final String[] getBody, final String[] setBody) {
        String caseConverted = asJavaName(name, true);
        boolean b = "boolean".equals(type);
        addMethod((b ? "is" : "get") + caseConverted, type, new String[0], null, getBody);
        addMethod("set" + caseConverted, "void", new String[]{type}, new String[]{argName == null ? "v" : argName}, setBody);
    }
    

    public abstract void end();

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
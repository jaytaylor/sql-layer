package com.akiban.direct;

public abstract class ClassBuilder {

    protected final String packageName;
    protected String schema;

    
    public abstract void preamble(String[] imports);

    public abstract void startClass(String name);
    
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
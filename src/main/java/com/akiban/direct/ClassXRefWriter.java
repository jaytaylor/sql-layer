
package com.akiban.direct;

import java.io.PrintWriter;

/**
 * Helper methods for writing a Java class to a AkibanAppender. This implementation
 * actually writes a JSON-formatted structure containing a relationships between
 * class, method and property names.
 * 
 * 
 * @author peter
 * 
 */
public class ClassXRefWriter extends ClassSourceWriterBase {

    boolean first = true;

    public ClassXRefWriter(final PrintWriter writer, final String packageName) {
        super(writer, packageName);
        append("{");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.akiban.direct.ClassBuilder#startInterface(java.lang.String)
     */
    @Override
    public void startClass(final String name, final boolean isInterface, String extendsClass,
            String[] implementsInterfaces, String[] imports) {
        classNames.push(name);
        indentation++;
        if (!first) {
            append(",");
            newLine();
        }
        first = true;
        appendq(shortName(name));
        append(":[");
        first = true;
    }

    @Override
    public void addMethod(final String name, final String returnType, final String[] argumentTypes,
            final String[] argumentNames, final String[] body) {
        if (!first) {
            append(",");
        }
        newLine();
        print("[");
        String n = name;
        int counter = 0;
        if (argumentTypes != null) {
            n += "(";
            for (int i = 0; i < argumentTypes.length; i++) {
                if (i > 0) {
                    n += ",";
                }
                final String argName;
                if (argumentNames != null) {
                    argName = argumentNames[counter];
                    counter++;
                } else {
                    argName = "z" + ++counter;
                }

                n += argName;
            }
            n += ")";
        }
        first = true;
        appendq(n);
        if (returnType.startsWith(PACKAGE)) {
            appendq(shortName(returnType));
        } else {
            appendq("");
        }
        append("]");
        first = false;
    }

    @Override
    public void addConstructor(final String[] argumentTypes, final String[] argumentNames, final String[] body) {
        // ignore
    }

    @Override
    public String addProperty(final String name, final String type, final String argName, final String[] getBody,
            final String[] setBody, final boolean hasSetter) {
        String caseConverted = asJavaName(name, false);
        /*
         * This isn't really a method, of course - we are exploiting the known
         * behavior of addMethod to add a reference for the property name
         */
        addMethod(caseConverted, type, null, null, null);
        return super.addProperty(name, type, argName, getBody, setBody, hasSetter);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.akiban.direct.ClassBuilder#end()
     */
    @Override
    public void end() {
        append("]");
        indentation--;
        classNames.pop();
    }

    @Override
    public void close() {
        newLine();
        append("}");
        newLine();
    }

    private void appendq(final String s) {
        if (first) {
            first = false;
        } else {
            append(",");
        }
        append("\"" + s + "\"");
    }
}

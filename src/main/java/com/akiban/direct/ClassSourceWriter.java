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
 * 
 * @author peter
 * 
 */
public class ClassSourceWriter extends ClassSourceWriterBase {

    private final boolean isAbstract;
    private String[] imports;

    public ClassSourceWriter(final PrintWriter writer, final String packageName, boolean isAbstract) {
        super(writer, packageName);
        this.isAbstract = isAbstract;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.akiban.direct.ClassBuilder#startInterface(java.lang.String)
     */
    @Override
    public void startClass(final String name, final boolean isInterface, String extendsClass,
            String[] implementsInterfaces, String[] imports) {
        if (name.indexOf('$') == -1) {
            /*
             * Only for non-inner classes
             */
            println("package " + packageName + ";");
            newLine();
            if (imports != null) {
                for (final String s : imports) {
                    println("import " + s + ";");
                }
                this.imports = imports;
            }
        }
        newLine();
        print("public ", (isAbstract ? "abstract " : ""), (isInterface ? "interface " : "class "), shortName(name));
        indentation++;
        if (extendsClass != null || implementsInterfaces != null && implementsInterfaces.length > 0) {
            newLine();
            if (extendsClass != null) {
                print("extends ", externalName(localName(extendsClass, name)));
            }
            if (isInterface) {
                if (implementsInterfaces != null) {
                    for (final String s : implementsInterfaces) {
                        append(", ", externalName(localName(s, name)));
                    }
                    append(" ");
                }
            } else {
                if (implementsInterfaces != null) {
                    boolean first = true;
                    for (final String s : implementsInterfaces) {
                        append(first ? " implements " : ", ", externalName(localName(s, name)));
                        first = false;
                    }
                    append(" ");
                }
            }
        } else {
            append(" ");
        }
        append("{");
        newLine();
        classNames.push(name);
    }

    @Override
    public void addMethod(final String name, final String returnType, final String[] argumentTypes,
            final String[] argumentNames, final String[] body) {
        print("public ", localName(externalName(returnType), classNames.firstElement()), " ", name, "(");
        boolean first = true;
        int counter = 0;
        for (final String s : argumentTypes) {
            if (!first) {
                append(", ");
            }
            String argName;
            if (argumentNames != null) {
                argName = argumentNames[counter];
                counter++;
            } else {
                argName = "z" + ++counter;
            }
            append(localName(externalName(s), classNames.firstElement()), " ", argName);
        }
        append(")");
        if (body == null) {
            append(";");
        } else {
            append(" {");
            newLine();
            indentation++;
            for (String s : body) {
                println(s, ";");
            }
            indentation--;
            println("}");
        }
        newLine();
    }

    @Override
    public void addConstructor(final String[] argumentTypes,
            final String[] argumentNames, final String[] body) {
        newLine();
        print("public ", localName(externalName(classNames.firstElement()), classNames.firstElement()),  "(");
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
            append(localName(externalName(s), classNames.firstElement()), " ", argName);
        }
        append(")");
        if (body == null) {
            append(";");
        } else {
            append(" {");
            newLine();
            indentation++;
            for (String s : body) {
                println(s, ";");
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
        newLine();
        classNames.pop();
    }
    

    private String localName(String fqn, String className) {
        int ltIndex = fqn.indexOf('<');
        int gtIndex = fqn.lastIndexOf('>');
        if (ltIndex != -1) {
            assert gtIndex > ltIndex && gtIndex == fqn.length() - 1;
            return localName(fqn.substring(0, ltIndex), className) + "<" + localName(fqn.substring(ltIndex + 1, gtIndex), className) + ">";
        }
        final int dotIndex = Math.max(fqn.lastIndexOf('$'), fqn.lastIndexOf('.'));

        if (dotIndex < 1) {
            return fqn;
        }
        boolean shorten = "java.lang.".equals(fqn.substring(0, dotIndex + 1));

        if (!shorten) {
            for (final String s : imports) {
                if (s.equals(fqn)) {
                    shorten = true;
                    break;
                }
            }
        }

        if (!shorten && (className.equals(fqn) || className.equals(fqn.substring(0, dotIndex)))) {
            shorten = true;
        }
        if (shorten) {
            return fqn.substring(dotIndex + 1);
        } else {
            return fqn;
        }
    }

}

/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.direct;

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

    public ClassSourceWriter(final PrintWriter writer, boolean isAbstract) {
        super(writer);
        this.isAbstract = isAbstract;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.foundationdb.direct.ClassBuilder#startInterface(java.lang.String)
     */
    @Override
    public void startClass(final String name, final boolean isInterface, String extendsClass,
            String[] implementsInterfaces, String[] imports) {
        if (name.indexOf('$') == -1) {
            /*
             * Only for non-inner classes
             */
            println("package " + PACKAGE + ";");
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
    public void addStaticInitializer(final String body) {
        println("static {");
        println(body + ";");
        println("}");
        newLine();
    }



    /*
     * (non-Javadoc)
     * 
     * @see com.foundationdb.direct.ClassBuilder#end()
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

        if (!shorten && imports != null) {
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


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

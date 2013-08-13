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

    public ClassSourceWriterBase(final PrintWriter writer) {
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

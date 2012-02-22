/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.ais.io;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.Map;

import com.akiban.ais.metamodel.MetaModel;
import com.akiban.ais.metamodel.ModelObject;
import com.akiban.ais.metamodel.Target;
import com.akiban.ais.model.AkibanInformationSchema;

public class CSVTarget extends Target
{
    // Target interface

    @Override
    public void deleteAll()
    {
    }

    @Override
    public void writeCount(int count)
    {
    }

    @Override
    public void close()
    {
        output.close();
    }

    // PersistitTarget interface

    public CSVTarget(PrintWriter output)
    {
        this.output = output;
    }

    // For use by this class
    @Override
    public void writeVersion(int modelVersion)
    {
        output.print(modelVersion);
        output.println();
    }

    @Override
    protected final void write(String typename, Map<String, Object> map)
    {
        output.print(quote(typename));
        ModelObject modelObject = MetaModel.only().definition(typename);
        for (ModelObject.Attribute attribute : modelObject.attributes()) {
            switch (attribute.type()) {
                case INTEGER:
                    writeIntOrNull((Integer) map.get(attribute.name()));
                    break;
                case LONG:
                    writeLongOrNull((Long) map.get(attribute.name()));
                    break;
                case STRING:
                    writeStringOrNull((String) map.get(attribute.name()));
                    break;
                case BOOLEAN:
                    writeBooleanOrNull((Boolean) map.get(attribute.name()));
                    break;
            }
        }
        output.println();
    }

    private void writeStringOrNull(String s)
    {
        if((s != null) && s.contains(",")) {
            throw new IllegalArgumentException("No commas allowed: " + s);
        }
        output.print(", ");
        if (s != null) {
            output.print(quote(s));
        }
    }

    private void writeIntOrNull(Integer i)
    {
        output.print(", ");
        if (i != null) {
            output.print(i);
        }
    }

    private void writeLongOrNull(Long l)
    {
        output.print(", ");
        if (l != null) {
            output.print(l);
        }
    }

    private void writeBooleanOrNull(Boolean b)
    {
        output.print(", ");
        output.print(b == null ? "-1" : b ? "1" : "0");
    }

    private String quote(String s)
    {
        return '"' + s + '"';
    }

    // State

    private PrintWriter output;

    public static String toString(AkibanInformationSchema ais) throws Exception
    {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(stream);
        new Writer(new CSVTarget(writer)).save(ais);
        writer.close();
        stream.flush();
        return stream.toString();
    }
}
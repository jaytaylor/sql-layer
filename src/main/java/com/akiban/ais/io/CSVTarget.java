package com.akiban.ais.io;

import com.akiban.ais.metamodel.MetaModel;
import com.akiban.ais.metamodel.ModelObject;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.Target;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Map;

public class CSVTarget extends Target
{
    // Target interface

    @Override
    public void deleteAll() throws Exception
    {
    }

    @Override
    public void writeCount(int count) throws Exception
    {
    }

    @Override
    public void close() throws Exception
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
    protected final void write(String typename, Map<String, Object> map) throws Exception
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

    public static String toString(AkibaInformationSchema ais) throws Exception
    {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(stream);
        new Writer(new CSVTarget(writer)).save(ais);
        writer.close();
        stream.flush();
        return stream.toString();
    }
}
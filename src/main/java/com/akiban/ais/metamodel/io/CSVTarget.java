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

package com.akiban.ais.metamodel.io;

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
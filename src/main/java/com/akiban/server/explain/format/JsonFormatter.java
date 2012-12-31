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

package com.akiban.server.explain.format;

import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.Explainer;
import com.akiban.server.explain.PrimitiveExplainer;
import com.akiban.server.explain.Label;

import com.akiban.server.error.AkibanInternalException;

import com.google.gson.stream.JsonWriter;

import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.io.StringWriter;

public class JsonFormatter
{
    public String format(Explainer explainer) {
        StringWriter str = new StringWriter();
        JsonWriter writer = new JsonWriter(str);
        if (true) {
            writer.setIndent("  "); // Pretty print
        }
        try {
            write(writer, explainer);
        }
        catch (IOException ex) {
            throw new AkibanInternalException("Error writing to string", ex);
        }
        return str.toString();
    }

    public void write(JsonWriter writer, Explainer explainer) throws IOException {
        switch (explainer.getType().generalType()) {
        case SCALAR_VALUE:
            writePrimitive(writer, (PrimitiveExplainer)explainer);
            break;
        default:
            writeCompound(writer, (CompoundExplainer)explainer);
            break;
        }
    }

    protected void writePrimitive(JsonWriter writer, PrimitiveExplainer explainer) throws IOException {
        switch (explainer.getType()) {
        case STRING:
            writer.value((String)explainer.get());
            break;
        case EXACT_NUMERIC:
        case FLOATING_POINT:
            writer.value((Number)explainer.get());
            break;
        default:
            writer.value(explainer.get().toString());
        }
    }

    protected void writeCompound(JsonWriter writer, CompoundExplainer explainer) throws IOException {
        writer.beginObject();
        writer.name("type");
        writer.value(explainer.getType().name().toLowerCase());
        for (Map.Entry<Label, List<Explainer>> entry : explainer.get().entrySet()) {
            writer.name(entry.getKey().name().toLowerCase());
            writer.beginArray();
            for (Explainer child : entry.getValue()) {
                write(writer, child);
            }
            writer.endArray();
        }
        writer.endObject();
    }
}

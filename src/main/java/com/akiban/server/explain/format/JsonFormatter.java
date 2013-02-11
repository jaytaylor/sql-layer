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

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;

public class JsonFormatter
{
    private final JsonFactory factory = new JsonFactory();
    private final boolean pretty = true;

    public String format(Explainer explainer) {
        StringWriter str = new StringWriter();
        try {
            JsonGenerator generator = factory.createJsonGenerator(str);
            if (pretty) {
                generator.useDefaultPrettyPrinter();
            }
            generate(generator, explainer);
            generator.flush();
        }
        catch (IOException ex) {
            throw new AkibanInternalException("Error writing to string", ex);
        }
        return str.toString();
    }

    public void format(Explainer explainer, OutputStream stream) throws IOException {
        JsonGenerator generator = factory.createJsonGenerator(stream, JsonEncoding.UTF8);
        if (pretty) {
            generator.useDefaultPrettyPrinter();
        }
        generate(generator, explainer);
        generator.flush();
    }

    public void generate(JsonGenerator generator, Explainer explainer) throws IOException {
        switch (explainer.getType().generalType()) {
        case SCALAR_VALUE:
            generatePrimitive(generator, (PrimitiveExplainer)explainer);
            break;
        default:
            generateCompound(generator, (CompoundExplainer)explainer);
            break;
        }
    }

    protected void generatePrimitive(JsonGenerator generator, PrimitiveExplainer explainer) throws IOException {
        generator.writeObject(explainer.get());
    }

    protected void generateCompound(JsonGenerator generator, CompoundExplainer explainer) throws IOException {
        generator.writeStartObject();
        generator.writeObjectField("type", explainer.getType().name().toLowerCase());
        for (Map.Entry<Label, List<Explainer>> entry : explainer.get().entrySet()) {
            generator.writeFieldName(entry.getKey().name().toLowerCase());
            generator.writeStartArray();
            for (Explainer child : entry.getValue()) {
                generate(generator, child);
            }
            generator.writeEndArray();
        }
        generator.writeEndObject();
    }
}

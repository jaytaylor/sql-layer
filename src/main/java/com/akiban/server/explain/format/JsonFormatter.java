
package com.akiban.server.explain.format;

import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.Explainer;
import com.akiban.server.explain.PrimitiveExplainer;
import com.akiban.server.explain.Label;

import com.akiban.server.error.AkibanInternalException;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;

import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;

import static com.akiban.util.JsonUtils.createJsonGenerator;

public class JsonFormatter
{
    private final boolean pretty = true;

    public String format(Explainer explainer) {
        StringWriter str = new StringWriter();
        try {
            JsonGenerator generator = createJsonGenerator(str);
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

    public void format(Explainer explainer, Writer writer) throws IOException {
        format(explainer, createJsonGenerator(writer));
    }

    public void format(Explainer explainer, OutputStream stream) throws IOException {
        format(explainer, createJsonGenerator(stream, JsonEncoding.UTF8));
    }

    public void format(Explainer explainer, JsonGenerator generator) throws IOException {
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

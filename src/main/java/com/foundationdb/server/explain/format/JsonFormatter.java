/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.server.explain.format;

import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.Explainer;
import com.foundationdb.server.explain.PrimitiveExplainer;
import com.foundationdb.server.explain.Label;

import com.foundationdb.server.error.AkibanInternalException;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;

import static com.foundationdb.util.JsonUtils.createJsonGenerator;
import static com.foundationdb.util.JsonUtils.createPrettyJsonGenerator;
import static com.foundationdb.util.JsonUtils.makePretty;

public class JsonFormatter
{
    private final boolean pretty = true;

    public String format(Explainer explainer) {
        StringWriter str = new StringWriter();
        try {
            JsonGenerator generator = pretty ? createPrettyJsonGenerator(str) : createJsonGenerator(str);
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
            makePretty(generator);
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

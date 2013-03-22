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

package com.akiban.util;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;

public final class JsonUtils {

    public static JsonGenerator createJsonGenerator(Writer out) throws IOException {
        return jsonFactory.createJsonGenerator(out);
    }

    public static JsonGenerator createJsonGenerator(OutputStream stream, JsonEncoding encoding) throws IOException {
        return jsonFactory.createJsonGenerator(stream, encoding);
    }

    public static JsonParser jsonParser(String string) throws IOException {
        return JsonUtils.jsonFactory.createJsonParser(string);
    }

    public static JsonNode readTree(String json) throws IOException {
        return mapper.readTree(json);
    }

    public static JsonNode readTree(byte[] json) throws IOException {
        return mapper.readTree(json);
    }

    public static JsonNode readTree(InputStream json) throws IOException {
        return mapper.readTree(json);
    }

    public static JsonNode readTree(Reader json) throws IOException {
        return mapper.readTree(json);
    }

    public static JsonNode readTree(File source) throws IOException {
        return mapper.readTree(source);
    }

    public static <T> T readValue(File file, Class<? extends T> cls) throws IOException {
        return mapper.readValue(file, cls);
    }

    public static <T> T readValue(Reader source, Class<? extends T> cls) throws IOException {
        return mapper.readValue(source, cls);
    }

    public static String normalizeJson(String json) {
        try {
            JsonNode node = mapper.readTree(new StringReader(json));
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static final ObjectMapper mapper = new ObjectMapper();
    private static final JsonFactory jsonFactory = new JsonFactory(mapper);

    private JsonUtils() {}
}

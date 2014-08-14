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

package com.foundationdb.util;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.PrettyPrinter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter.Indenter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.util.Arrays;

public final class JsonUtils {

    public static JsonGenerator createJsonGenerator(Writer out) throws IOException {
        return jsonFactory.createGenerator(out);
    }

    public static JsonGenerator createJsonGenerator(OutputStream stream, JsonEncoding encoding) throws IOException {
        return jsonFactory.createGenerator(stream, encoding);
    }

    public static JsonGenerator createPrettyJsonGenerator(Writer out) throws IOException {
        return createJsonGenerator(out).setPrettyPrinter(prettyPrinter);
    }

    public static JsonGenerator createPrettyJsonGenerator(OutputStream stream, JsonEncoding encoding) throws IOException {
        return createJsonGenerator(stream, encoding).setPrettyPrinter(prettyPrinter);
    }

    public static void makePretty(JsonGenerator generator) {
        generator.setPrettyPrinter(prettyPrinter);
    }

    public static JsonParser jsonParser(String string) throws IOException {
        return JsonUtils.jsonFactory.createParser(string);
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

    public static class LineFeedIndenter implements Indenter
    {
        private static final char LINE_FEED = '\n';
        private final static int SPACE_COUNT = 64;
        private final static char[] SPACES = new char[SPACE_COUNT];
        static {
            Arrays.fill(SPACES, ' ');
        }

        @Override
        public boolean isInline() {
            return false;
        }

        /** See {@link DefaultPrettyPrinter.Lf2SpacesIndenter#writeIndentation(JsonGenerator, int)} */
        @Override
        public void writeIndentation(JsonGenerator jg, int level) throws IOException {
            jg.writeRaw(LINE_FEED);
            if (level > 0) {
                level += level;
                while (level > SPACE_COUNT) {
                    jg.writeRaw(SPACES, 0, SPACE_COUNT);
                    level -= SPACES.length;
                }
                jg.writeRaw(SPACES, 0, level);
            }
        }
    }

    public static final ObjectMapper mapper = new ObjectMapper();
    private static final JsonFactory jsonFactory = new JsonFactory(mapper);
    private static final PrettyPrinter prettyPrinter;

    static {
        // Default uses line.separator by default, use a consistent '\n' instead.
        DefaultPrettyPrinter pp = new DefaultPrettyPrinter();
        Indenter indenter = new LineFeedIndenter();
        pp.indentObjectsWith(indenter);
        prettyPrinter = pp;
    }

    private JsonUtils() {}
}

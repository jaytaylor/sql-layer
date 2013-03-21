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
            return mapper.defaultPrettyPrintingWriter().writeValueAsString(node);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static final ObjectMapper mapper = new ObjectMapper();
    private static final JsonFactory jsonFactory = new JsonFactory(mapper);

    private JsonUtils() {}
}

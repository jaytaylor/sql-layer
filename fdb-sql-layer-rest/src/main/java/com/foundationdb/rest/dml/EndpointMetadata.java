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

package com.foundationdb.rest.dml;

import static com.foundationdb.util.JsonUtils.readTree;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response.Status;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.rest.RestResponseBuilder;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Easy access to the server version
 */
public class EndpointMetadata {

    private final static String REQUIRED = "required";
    private final static String DEFAULT = "default";

    private final static String METHOD = "method";
    private final static String PATH = "path";
    private final static String IN = "in";
    private final static String OUT = "out";
    private final static String FUNCTION = "function";

    private final static String PP = "pp:";
    private final static String QP = "qp:";
    private final static String JSON = "json:";
    private final static String CONTENT = "content";

    final static String X_TYPE_INT = "int";
    final static String X_TYPE_LONG = "long";
    final static String X_TYPE_FLOAT = "float";
    final static String X_TYPE_DOUBLE = "double";
    final static String X_TYPE_STRING = "string";
    final static String X_TYPE_DATE = "date";
    final static String X_TYPE_TIMESTAMP = "timestamp";
    final static String X_TYPE_BYTEARRAY = "bytearray";
    final static String X_TYPE_JSON = "json";
    final static String X_TYPE_VOID = "void";

    final static List<String> X_TYPES = Arrays.asList(new String[] { X_TYPE_INT, X_TYPE_LONG, X_TYPE_FLOAT,
            X_TYPE_DOUBLE, X_TYPE_STRING, X_TYPE_DATE, X_TYPE_TIMESTAMP, X_TYPE_BYTEARRAY, X_TYPE_JSON, X_TYPE_VOID });

    /*
     * TODO: support X_TYPE_BYTEARRAY type
     */
    final static List<String> OUT_TYPES = Arrays.asList(new String[] { X_TYPE_STRING, X_TYPE_JSON, X_TYPE_VOID });

    private final static Charset UTF8 = Charset.forName("UTF8");

    static Object convertType(ParamMetadata pm, Object v) throws Exception {

        if (v == null) {
            if (pm.defaultValue != null) {
                return pm.defaultValue;
            } else if (pm.required) {
                throw new IllegalArgumentException("Argument for " + pm + " may not be null");
            } else {
                return null;
            }
        }

        switch (pm.type) {
        case EndpointMetadata.X_TYPE_INT:
            if (v instanceof JsonNode) {
                if (((JsonNode) v).isNumber()) {
                    return ((JsonNode) v).intValue();
                } else {
                    break;
                }
            } else {
                assert v instanceof String;
                return Integer.parseInt((String) v);
            }
        case EndpointMetadata.X_TYPE_LONG:
            if (v instanceof JsonNode) {
                if (((JsonNode) v).isNumber()) {
                    return ((JsonNode) v).longValue();
                } else {
                    break;
                }
            } else {
                assert v instanceof String;
                return Long.parseLong((String) v);
            }
        case EndpointMetadata.X_TYPE_FLOAT:
            if (v instanceof JsonNode) {
                if (((JsonNode) v).isNumber()) {
                    return ((JsonNode) v).numberValue().floatValue();
                } else {
                    break;
                }
            } else {
                assert v instanceof String;
                return Float.parseFloat((String) v);
            }
        case EndpointMetadata.X_TYPE_DOUBLE:
            if (v instanceof JsonNode) {
                if (((JsonNode) v).isNumber()) {
                    return ((JsonNode) v).numberValue().doubleValue();
                } else {
                    break;
                }
            } else {
                assert v instanceof String;
                return Double.parseDouble((String) v);
            }
        case EndpointMetadata.X_TYPE_STRING:
            return asString(pm, v);
        case EndpointMetadata.X_TYPE_DATE:
            return asDate(pm, v);
        case EndpointMetadata.X_TYPE_TIMESTAMP:
            return asTimestamp(pm, v);
        case EndpointMetadata.X_TYPE_BYTEARRAY:
            assert v instanceof byte[];
            return v;
        case EndpointMetadata.X_TYPE_JSON:
            String json = asString(pm, v);
            return readTree(json);
        default:
        }
        throw new IllegalArgumentException("Type specified by " + pm + " is not supported");
    }

    private static String asString(ParamMetadata pm, Object v) {
        if (v instanceof JsonNode) {
            if (((JsonNode) v).isTextual()) {
                return ((JsonNode) v).textValue();
            } else {
                throw new IllegalArgumentException("JsonNode " + v + " is not textual");
            }
        } else if (v instanceof String) {
            return (String) v;
        } else if (v instanceof byte[]) {
            return new String((byte[]) v, UTF8);
        } else if (v != null) {
            return v.toString();
        } else {
            return null;
        }
    }

    private static String assertName(final String s, final String context) {
        boolean okay = true;
        for (int index = 0; okay && index < s.length(); index++) {
            if (index == 0 && !Character.isJavaIdentifierStart(s.charAt(index)) || index > 0
                    && !Character.isJavaIdentifierPart(s.charAt(index))) {
                okay = false;
            }
        }
        if (s.isEmpty() || !okay) {
            throw new IllegalArgumentException("Invalid name " + context + " :" + s);
        }
        return s;
    }

    private static Date asDate(ParamMetadata pm, Object v) throws ParseException {
        String s = asString(pm, v);
        if ("today".equalsIgnoreCase(s)) {
            return new Date(System.currentTimeMillis());
        }
        return Date.valueOf(s);
    }

    private static Timestamp asTimestamp(ParamMetadata pm, Object v) throws ParseException {
        String s = asString(pm, v);
        if ("now".equalsIgnoreCase(s)) {
            return new Timestamp(System.currentTimeMillis());
        }
        return Timestamp.valueOf(s);
    }

    static EndpointMetadata createEndpointMetadata(final String schema, final String routineName,
            final String specification) throws Exception {
        EndpointMetadata em = new EndpointMetadata();
        em.schemaName = schema;
        em.routineName = routineName;

        Tokenizer tokens = new Tokenizer(specification, ",= ");

        while (tokens.hasMore()) {
            String name = tokens.nextName(true);
            if (!tokens.delimiterIn("=") || !tokens.hasMore()) {
                throw new IllegalArgumentException("Element " + name + " has no associated value: " + specification);
            }

            switch (name.toLowerCase()) {

            case METHOD: {
                String v = tokens.nextName(true);
                if (!("GET".equalsIgnoreCase(v)) && !("POST".equalsIgnoreCase(v)) && !("PUT".equalsIgnoreCase(v))
                        && !("DELETE".equalsIgnoreCase(v))) {
                    throw new IllegalArgumentException("Method must be GET, POST, PUT or DELETE");
                }
                em.method = v.toUpperCase();
                break;
            }
            case PATH: {
                String v = tokens.next(true);
                int p = v.indexOf('/');
                if (p == -1) {
                    em.name = v;
                } else {
                    em.name = v.substring(0, p);
                    em.pattern = Pattern.compile(v.substring(p + 1));
                }
                break;
            }
            case FUNCTION: {
                em.function = tokens.nextName(true);
                break;
            }
            case OUT: {
                tokens.grouped = true;
                String v = tokens.next(true);
                tokens.grouped = false;
                em.outParam = createOutParameter(new Tokenizer(v, ", "));
                break;
            }

            case IN: {
                tokens.grouped = true;
                String v = tokens.next(false);
                tokens.grouped = false;
                List<ParamMetadata> list = new ArrayList<>();
                Tokenizer inTokens = new Tokenizer(v, ", ");
                while (inTokens.hasMore()) {
                    final ParamMetadata pm = createInParameter(inTokens);
                    if (em.expectedContentType == null && pm.source != null && pm.source.mimeType() != null) {
                        em.expectedContentType = pm.source.mimeType();
                    }
                    list.add(pm);
                    if (inTokens.hasMore() && !inTokens.delimiterIn(",")) {
                        throw new IllegalArgumentException("Ambiguous input parameter specification: " + v);
                    }
                }
                em.inParams = list.toArray(new ParamMetadata[list.size()]);
                break;
            }
            default:
                throw new IllegalArgumentException("Invalid parameter specification element " + name + ": "
                        + specification);
            }
        }
        em.validate();
        return em;
    }

    static ParamMetadata createInParameter(final Tokenizer tokens) throws Exception {
        String v = tokens.next(true);
        ParamMetadata pm = createParameter(tokens);
        if (pm.type == X_TYPE_VOID) {
            throw new IllegalArgumentException("Input parameter may not have type " + X_TYPE_VOID);
        }
        ParamSourceMetadata psm = null;
        if (v.regionMatches(true, 0, PP, 0, PP.length())) {
            psm = new ParamSourcePath(Integer.parseInt(v.substring(PP.length())));
        } else if (v.regionMatches(true, 0, QP, 0, QP.length())) {
            psm = new ParamSourceQueryParam(assertName(v.substring(QP.length()), tokens.source));
        } else if (v.regionMatches(true, 0, JSON, 0, JSON.length())) {
            psm = new ParamSourceJson(assertName(v.substring(JSON.length()), tokens.source));
        } else if (v.equalsIgnoreCase(CONTENT)) {
            psm = new ParamSourceContent(pm.type);
        }
        if (psm == null) {
            throw new IllegalArgumentException("Invalid parameter source for " + pm.name + ": " + tokens.source);
        }
        pm.source = psm;
        return pm;
    }

    static ParamMetadata createOutParameter(final Tokenizer tokens) throws Exception {
        ParamMetadata pm = createParameter(tokens);
        if (!EndpointMetadata.OUT_TYPES.contains(pm.type)) {
            throw new IllegalArgumentException("Unsuported output parameter type " + pm.type);
        }

        return pm;
    }

    static ParamMetadata createParameter(final Tokenizer tokens) throws Exception {
        ParamMetadata pm = new ParamMetadata();
        String v = tokens.nextName(true);
        String type = v.toLowerCase();
        if (!EndpointMetadata.X_TYPES.contains(type)) {
            throw new IllegalArgumentException("Unknown parameter type " + v);
        }
        pm.type = type;

        String qualifier;
        while (tokens.getLastDelimiter() == ' ' && !(qualifier = tokens.next(false)).isEmpty()) {
            if (REQUIRED.equalsIgnoreCase(qualifier)) {
                pm.required = true;
            } else if (DEFAULT.equalsIgnoreCase(qualifier)) {
                v = tokens.next(true);
                pm.defaultValue = convertType(pm, v);
            } else {
                throw new IllegalArgumentException("Unknown qualifier '" + qualifier + "' in " + tokens.source);
            }
        }
        return pm;
    }

    /**
     * Meta-data for one REST call parameter.
     */
    static class ParamMetadata {
        String name;
        String type;
        boolean required;
        Object defaultValue;
        ParamSourceMetadata source;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (source != null) {
                append(sb, source.toString(), " ");
            }
            append(sb, type);
            if (required) {
                append(sb, " ", REQUIRED);
            }
            if (defaultValue != null) {
                append(sb, " ", DEFAULT, " ", "\'", defaultValue.toString(), "\'");
            }
            return sb.toString();
        }

        public boolean equals(Object other) {
            ParamMetadata pm = (ParamMetadata) other;
            return EndpointMetadata.equals(pm.name, name) && EndpointMetadata.equals(pm.type, type)
                    && pm.required == required && EndpointMetadata.equals(pm.defaultValue, defaultValue)
                    && EndpointMetadata.equals(source, pm.source);
        }
    }

    /**
     * Cache to hold partial result, e.b. the JsonNode of the root of a tree
     * created by parsing the request body as JSON.
     */
    static class ParamCache {
        Matcher matcher;
        JsonNode tree;
    }

    abstract static class ParamSourceMetadata {
        abstract Object value(final String pathParams, final MultivaluedMap<String, String> queryParams,
                Object content, ParamCache cache) throws Exception;

        String mimeType() {
            return null;
        }
    }

    /**
     * Meta-data for a parameter intended to be filled from the query parameters
     * of the REST call.
     */
    static class ParamSourceQueryParam extends ParamSourceMetadata {

        final String paramName;

        ParamSourceQueryParam(final String paramName) {
            this.paramName = paramName;
        }

        @Override
        Object value(final String pathParams, final MultivaluedMap<String, String> queryParams, Object content,
                ParamCache cache) {
            List<String> values = queryParams.get(paramName);
            if (values != null) {
                if (values.size() == 1) {
                    return values.get(0);
                } else {
                    return values;
                }
            } else {
                return null;
            }
        }

        @Override
        public String toString() {
            return QP + paramName;
        }

        @Override
        public boolean equals(Object other) {
            return (other instanceof ParamSourceQueryParam)
                    && EndpointMetadata.equals(((ParamSourceQueryParam) other).paramName, paramName);
        }
    }

    /**
     * Meta-data for a parameter intended to be filled from the URI of the REST
     * call. The procedure call end-point aggregates all text of the URI
     * following procedure name into a string; this class defines a Pattern for
     * parsing out an field from that text using a supplied RegEx pattern. The
     * pattern is case-insensitive.
     */
    static class ParamSourcePath extends ParamSourceMetadata {
        final int matchingGroup;

        ParamSourcePath(final int matchingGroup) {
            this.matchingGroup = matchingGroup;
        }

        Object value(final String pathParams, final MultivaluedMap<String, String> queryParams, Object content,
                ParamCache cache) {
            if (cache.matcher.matches()) {
                return cache.matcher.group(matchingGroup);
            } else {
                return null;
            }
        }

        @Override
        public String toString() {
            return PP + matchingGroup;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ParamSourcePath
                    && EndpointMetadata.equals(((ParamSourcePath) other).matchingGroup, matchingGroup);
        }
    }

    /**
     * Meta-data for a parameter that conveys the byte array supplied in the
     * request body of the REST call. This is intended to support a rest call
     * that receives a text-valued payload. This class represents both
     * text/plain and application/json payloads. The constructor argument
     * distinguishes which type is intended.
     */
    static class ParamSourceContent extends ParamSourceMetadata {
        final private String type;

        ParamSourceContent(final String type) {
            this.type = type;
        }

        @Override
        String mimeType() {
            return X_TYPE_BYTEARRAY.equalsIgnoreCase(type) ? MediaType.APPLICATION_OCTET_STREAM : X_TYPE_JSON
                    .equalsIgnoreCase(type) ? MediaType.APPLICATION_JSON : MediaType.TEXT_PLAIN;
        }

        Object value(final String pathParams, final MultivaluedMap<String, String> queryParams, Object content,
                ParamCache cache) {

            if (content == null) {
                return null;
            } else if (X_TYPE_BYTEARRAY.equalsIgnoreCase(type)) {
                return content;
            } else {
                return new String((byte[]) content, UTF8);
            }
        }

        @Override
        public String toString() {
            return CONTENT;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ParamSourceContent
                    && EndpointMetadata.equals(((ParamSourceContent) other).type, type);
        }
    }

    static class ParamSourceJson extends ParamSourceMetadata {

        final String paramName;

        ParamSourceJson(final String paramName) {
            this.paramName = paramName;
        }

        @Override
        String mimeType() {
            return MediaType.APPLICATION_JSON;
        }

        Object value(final String pathParams, final MultivaluedMap<String, String> queryParams, Object content,
                ParamCache cache) throws Exception {
            if (cache.tree == null) {
                String s;
                if (content instanceof byte[]) {
                    s = new String((byte[]) content, UTF8);
                } else if (content == null) {
                    s = "";
                } else {
                    s = (String) content;
                }
                try {
                    cache.tree = readTree(s);
                } catch (IOException e) {
                    throw new WebApplicationException(e, Status.CONFLICT);
                }
            }
            return cache.tree.get(paramName);
        }

        @Override
        public String toString() {
            return JSON + paramName;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ParamSourceJson
                    && EndpointMetadata.equals(((ParamSourceJson) other).paramName, paramName);
        }

    }

    static class EndpointAddress implements Comparable<EndpointAddress> {
        /**
         * One of GET, POST, PUT or DELETE
         */
        private final String method;

        /**
         * Schema name
         */
        private final String schema;
        /**
         * Name of the end-point
         */
        private final String name;

        EndpointAddress(final String method, TableName procName) {
            this.method = method;
            this.schema = procName.getSchemaName();
            this.name = procName.getTableName();
        }

        /**
         * Sorts by schema, method, procedure name.
         */
        public int compareTo(EndpointAddress other) {
            int c = schema.compareTo(other.schema);
            if (c == 0) {
                c = method.compareTo(other.method);
            }
            if (c == 0) {
                c = name.compareTo(other.name);
            }
            return c;
        }

        @Override
        public int hashCode() {
            return name.hashCode() ^ schema.hashCode() ^ method.hashCode();
        }

        @Override
        public boolean equals(Object other) {
            EndpointAddress ea = (EndpointAddress) other;
            return name.equals(ea.name) && method.equals(ea.method) && schema.equals(ea.schema);
        }

    }

    static class Tokenizer {
        final String source;
        final StringBuilder result = new StringBuilder();
        final String delimiters;
        int index = 0;
        boolean grouped = false;
        int depth = 0;
        char delimiter = (char) -1;

        Tokenizer(String source, String delimiters) {
            this.source = source;
            this.delimiters = delimiters;
        }

        boolean hasMore() {
            eatSpaces();
            return index < source.length();
        }

        boolean delimiterIn(final String match) {
            return match.indexOf(delimiter) >= 0;
        }

        String next(boolean required) {
            result.setLength(0);
            boolean quoted = false;
            boolean literal = false;
            boolean first = true;
            delimiter = 0;
            for (; index < source.length(); index++) {
                char c = source.charAt(index);
                if (quoted) {
                    quoted = false;
                    result.append(c);
                } else if (c == '\\') {
                    quoted = true;
                } else if (grouped && c == '(') {
                    if (depth > 0) {
                        result.append(c);
                    }
                    depth++;
                } else if (grouped && c == ')') {
                    if (depth > 0) {
                        depth--;
                        if (depth > 0) {
                            result.append(c);
                        }
                    }
                } else if (!literal && depth == 0 && delimiters.indexOf(c) >= 0) {
                    delimiter = c;
                    index++;
                    break;
                } else if (c == '\'') {
                    if (first) {
                        literal = true;
                    } else if (literal) {
                        literal = false;
                    }
                } else {
                    result.append(c);
                }
                first = false;
            }
            eatSpaces();
            if (required && result.length() == 0) {
                throw new IllegalArgumentException("Token missing: " + source);
            }
            return result.toString();
        }

        String nextName(final boolean required) {
            result.setLength(0);
            boolean first = true;
            delimiter = 0;
            for (; index < source.length(); index++) {
                char c = source.charAt(index);
                if (delimiters.indexOf(c) >= 0) {
                    delimiter = c;
                    index++;
                    break;
                }
                if (!Character.isJavaIdentifierPart(c) || (first && !Character.isJavaIdentifierStart(c))) {
                    throw new IllegalArgumentException("Invalid character in name: " + source);
                }
                result.append(c);
                first = false;
            }
            eatSpaces();
            if (required && result.length() == 0) {
                throw new IllegalArgumentException("Token missing: " + source);
            }
            return result.toString();
        }

        char getLastDelimiter() {
            return delimiter;
        }

        private void eatSpaces() {
            while (index < source.length() && source.charAt(index) == ' ') {
                index++;
            }
            if (index == source.length()) {
                delimiter = 0;
            }
        }
    }

    /**
     * Schema in which this endpoint is defined
     */
    String schemaName;

    /**
     * Name of the Routine that contains the function definition
     */
    String routineName;

    /**
     * GET / POST / PUT / DELETE
     */

    String method;

    /**
     * Name of the function to call in the script
     */
    String function;

    /**
     * Name of endpoint. This is the text prefix of a pattern - i.e., everything
     * before the '/'.
     */

    String name;
    /**
     * Pattern for matching path
     */
    Pattern pattern;

    /**
     * Parameter meta-data for the return value (specifies type and optional
     * default value)
     */
    ParamMetadata outParam;

    /**
     * Parameter meta-data for input parameters
     */
    ParamMetadata[] inParams;

    String expectedContentType;

    static boolean equals(Object a, Object b) {
        if (a == null) {
            return b == null;
        } else if (a instanceof Pattern) {
            if (!(b instanceof Pattern)) {
                return false;
            } else {
                return a.toString().equals(b.toString());
            }
        } else if (a instanceof Object[]) {
            if (!(b instanceof Object[])) {
                return false;
            }
            return Arrays.equals((ParamMetadata[]) a, (ParamMetadata[]) b);
        } else {
            return a.equals(b);
        }
    }

    private void validate() {
        notNull(schemaName, "schema name");
        notNull(routineName, "routine name");
        notNull(method, "'method='");
        notNull(name, "'path='");
        notNull(function, "'function='");
        notNull(inParams, "'in='");
        notNull(outParam, "'out='");
    }

    private void notNull(final Object v, final String name) {
        if (v == null) {
            throw new IllegalArgumentException(name + " not specified in " + toString());
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        append(sb, METHOD, "=", method, " ", PATH, "=", name);
        if (pattern != null) {
            append(sb, "/", pattern.toString());
        }
        append(sb, " ", FUNCTION, "=", function, " ", IN, "=(");
        if (inParams == null) {
            sb.append("null");
        } else {
            for (int index = 0; index < inParams.length; index++) {
                if (index > 0) {
                    sb.append(", ");
                }
                sb.append(inParams[index]);
            }
        }
        append(sb, ") ", OUT, "=");
        if (outParam == null) {
            sb.append("null");
        } else {
            append(sb, outParam.toString());
        }
        return sb.toString();
    }

    void setPathParameterPattern(final String s) {
        pattern = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
    }

    Matcher getParamPathMatcher(final ParamCache cache, final String pathParamString) {
        if (cache.matcher == null) {
            cache.matcher = pattern.matcher(pathParamString.isEmpty() ? pathParamString : pathParamString.substring(1));
        }
        return cache.matcher;
    }

    private static void append(final StringBuilder sb, final CharSequence... strings) {
        for (final CharSequence s : strings) {
            for (int index = 0; index < s.length(); index++) {
                char c = s.charAt(index);
                if ("\\".indexOf(c) >= 0) {
                    sb.append('\\');
                }
                sb.append(c);
            }
        }
    }

    @Override
    public boolean equals(final Object other) {
        EndpointMetadata em = (EndpointMetadata) other;
        return equals(em.schemaName, schemaName) && equals(em.expectedContentType, expectedContentType)
                && equals(em.function, function) && equals(em.method, method) && equals(em.name, name)
                && equals(em.pattern, pattern) && equals(em.outParam, outParam) && equals(em.inParams, inParams);
    }

    public void setResponseHeaders(final RestResponseBuilder builder) {
        switch (outParam.type) {

        case EndpointMetadata.X_TYPE_STRING:
            builder.type(MediaType.TEXT_PLAIN_TYPE);
            break;

        case EndpointMetadata.X_TYPE_JSON:
            builder.type(MediaType.APPLICATION_JSON_TYPE);
            break;

        case EndpointMetadata.X_TYPE_VOID:
            builder.type((MediaType)null);
            builder.status(Status.NO_CONTENT);
            break;

        case EndpointMetadata.X_TYPE_BYTEARRAY:
            /*
             * intentionally falls through TODO: support X_TYPE_BYTEARRAY
             */

        default:
            assert false : "Invalid output type";
        }
    }

    public boolean isVoid() {
        return X_TYPE_VOID.equals(outParam.type);
    }

}

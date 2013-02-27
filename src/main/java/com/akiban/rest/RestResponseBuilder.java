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

package com.akiban.rest;

import com.akiban.rest.resources.ResourceHelper;
import com.akiban.server.Quote;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.NoSuchRoutineException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.util.AkibanAppender;
import org.codehaus.jackson.JsonParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class RestResponseBuilder {
    public interface BodyGenerator {
        public void write(PrintWriter writer) throws Exception;
    }

    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final Map<Class,Response.Status> EXCEPTION_STATUS_MAP = buildExceptionStatusMap();
    private static final Logger LOG = LoggerFactory.getLogger(RestResponseBuilder.class.getName());

    private final boolean isJsonp;
    private BodyGenerator outputGenerator;
    private String outputBody;
    private String jsonp;
    private int status;


    public RestResponseBuilder(String jsonp) {
        this.jsonp = jsonp;
        this.isJsonp = jsonp != null;
        this.status = Response.Status.OK.getStatusCode();
    }

    public static RestResponseBuilder forJsonp(String jsonp) {
        return new RestResponseBuilder(jsonp);
    }

    public RestResponseBuilder status(Response.Status status) {
        this.status = status.getStatusCode();
        return this;
    }

    public RestResponseBuilder body(String outputBody) {
        this.outputBody = outputBody;
        this.outputGenerator = null;
        return this;
    }

    public RestResponseBuilder body(ErrorCode code, String message) {
        body(formatErrorWithJsonp(code.getFormattedValue(), message));
        return this;
    }

    public RestResponseBuilder body(BodyGenerator outputGenerator) {
        this.outputBody = null;
        this.outputGenerator = outputGenerator;
        return this;
    }

    public Response build() {
        if(outputBody == null && outputGenerator == null && jsonp == null) {
            status(Response.Status.NO_CONTENT);
        }
        Response.ResponseBuilder builder = Response.status(status).entity(createStreamingOutput());
        if(isJsonp) {
            builder.type(ResourceHelper.APPLICATION_JAVASCRIPT_TYPE);
        }
        return builder.build();
    }

    public static void formatJsonError(StringBuilder builder, String code, String message) {
        builder.append("{\"code\":\"");
        builder.append(code);
        builder.append("\", \"message\":\"");
        Quote.JSON_QUOTE.append(AkibanAppender.of(builder), message);
        builder.append("\"}");
    }


    private String formatErrorWithJsonp(String code, String message) {
        StringBuilder builder = new StringBuilder();
        if(isJsonp) {
            builder.append(jsonp);
            builder.append('(');
        }
        formatJsonError(builder, code, message);
        if(isJsonp) {
            builder.append(')');
        }
        builder.append('\n');
        return builder.toString();
    }

    private WebApplicationException wrapException(Exception e) {
        final ErrorCode code;
        if(e instanceof InvalidOperationException) {
            code = ((InvalidOperationException)e).getCode();
        } else if(e instanceof SQLException) {
            code = ErrorCode.valueOfCode(((SQLException)e).getSQLState());
        } else {
            code = ErrorCode.UNEXPECTED_EXCEPTION;
        }
        Response.Status status = EXCEPTION_STATUS_MAP.get(e.getClass());
        if(status == null) {
            status = Response.Status.CONFLICT;
        }
        String message = e.getMessage();
        if(message == null) {
            message = e.getClass().getSimpleName();
        }
        code.logAtImportance(LOG, e);
        return new WebApplicationException(
                Response.status(status)
                        .entity(formatErrorWithJsonp(code.getFormattedValue(), message))
                        .type(isJsonp ? ResourceHelper.APPLICATION_JAVASCRIPT_TYPE : MediaType.APPLICATION_JSON_TYPE)
                        .build()
        );
    }

    private StreamingOutput createStreamingOutput() {
        return new StreamingOutput() {
            @Override
            public void write(OutputStream output)  {
                try {
                    PrintWriter writer = new PrintWriter(new OutputStreamWriter(output, UTF8), false);
                    if(isJsonp) {
                        writer.write(jsonp);
                        writer.write('(');
                    }
                    if(outputGenerator != null) {
                        outputGenerator.write(writer);
                    } else if(outputBody != null) {
                        writer.write(outputBody);
                    }
                    if(isJsonp) {
                        writer.write(')');
                    }
                    writer.write('\n');
                    writer.flush();
                    writer.close();
                } catch(Exception e) {
                    throw wrapException(e);
                }
            }
        };
    }


    private static Map<Class, Response.Status> buildExceptionStatusMap() {
        Map<Class, Response.Status> map = new HashMap<>();
        map.put(NoSuchTableException.class, Response.Status.NOT_FOUND);
        map.put(NoSuchRoutineException.class, Response.Status.NOT_FOUND);
        map.put(JsonParseException.class, Response.Status.BAD_REQUEST);
        return map;
    }
}

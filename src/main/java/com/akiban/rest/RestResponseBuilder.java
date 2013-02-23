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

import com.akiban.server.Quote;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.NoSuchRoutineException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.util.AkibanAppender;
import org.codehaus.jackson.JsonParseException;

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
    public interface ResponseGenerator {
        public void write(PrintWriter writer) throws Exception;
    }

    private static final int DEFAULT_RESPONSE_STATUS = Response.Status.OK.getStatusCode();

    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final Map<Class,Response.Status> EXCEPTION_STATUS_MAP = buildExceptionStatusMap();
    public static final Response FORBIDDEN_RESPONSE = Response.status(Response.Status.FORBIDDEN).build();

    private int status = DEFAULT_RESPONSE_STATUS;
    private ResponseGenerator outputGenerator;
    private String jsonp;


    public RestResponseBuilder(String jsonp) {
        this.jsonp = jsonp;
    }

    public RestResponseBuilder setStatus(int status) {
        this.status = status;
        return this;
    }

    public RestResponseBuilder setStatus(Response.Status status) {
        this.status = status.getStatusCode();
        return this;
    }

    public RestResponseBuilder setOutputGenerator(ResponseGenerator output) {
        this.outputGenerator = output;
        return this;
    }

    public Response build() {
        return Response
                .status(status)
                .entity(createStreamingOutput())
                .build();
    }

    private StreamingOutput createStreamingOutput() {
        return new StreamingOutput() {
            @Override
            public void write(OutputStream output)  {
                try {
                    PrintWriter writer = new PrintWriter(new OutputStreamWriter(output, UTF8), false);
                    boolean isJSONP = jsonp != null;
                    if(isJSONP) {
                        writer.write(jsonp);
                        writer.write('(');
                    }
                    if(outputGenerator != null) {
                        outputGenerator.write(writer);
                    }
                    if(isJSONP) {
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

    public static WebApplicationException wrapException(Exception e) {
        StringBuilder err = new StringBuilder(100);
        err.append("{\"code\":\"");
        String code;
        if(e instanceof InvalidOperationException) {
            code = ((InvalidOperationException)e).getCode().getFormattedValue();
        } else if(e instanceof SQLException) {
            code = ((SQLException)e).getSQLState();
        } else {
            code = ErrorCode.UNEXPECTED_EXCEPTION.getFormattedValue();
        }
        err.append(code);
        err.append("\",\"message\":\"");
        Quote.JSON_QUOTE.append(AkibanAppender.of(err), e.getMessage());
        err.append("\"}\n");
        Response.Status status = EXCEPTION_STATUS_MAP.get(e.getClass());
        if(status == null) {
            status = Response.Status.CONFLICT;
        }
        return new WebApplicationException(
                Response.status(status)
                        .entity(err.toString())
                        .type(MediaType.APPLICATION_JSON)
                        .build()
        );
    }

    private static Map<Class, Response.Status> buildExceptionStatusMap() {
        Map<Class, Response.Status> map = new HashMap<>();
        map.put(NoSuchTableException.class, Response.Status.NOT_FOUND);
        map.put(NoSuchRoutineException.class, Response.Status.NOT_FOUND);
        map.put(JsonParseException.class, Response.Status.BAD_REQUEST);
        return map;
    }
}

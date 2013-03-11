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

package com.akiban.rest.resources;

import static com.akiban.rest.resources.ResourceHelper.JSONP_ARG_NAME;

import java.io.PrintWriter;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.direct.ClassBuilder;
import com.akiban.direct.ClassSourceWriter;
import com.akiban.rest.ResourceRequirements;
import com.akiban.rest.RestResponseBuilder;
import com.akiban.rest.RestResponseBuilder.BodyGenerator;
import com.akiban.server.error.NoSuchSchemaException;

/**
 * Easy access to the server version
 */
@Path("/direct")
public class DirectResource {

    private final static String TABLE_ARG_NAME = "table";
    private final static String MODULE_ARG_NAME = "module";
    private final static String LANGUAGE = "language";
    private final static String PACKAGE = "com.akiban.direct.entity";
    private final static String CREATE_PROCEDURE_FORMAT = "CREATE PROCEDURE %s"
            + " (IN params TEXT,OUT result TEXT) LANGUAGE %sPARAMETER STYLE variables AS $$%s$$;";
    private final static String DROP_PROCEDURE_FORMAT = "DROP PROCEDURE %s";
    
    private final ResourceRequirements reqs;

    public DirectResource(ResourceRequirements reqs) {
        this.reqs = reqs;
    }

    /**
     * Derive and return code-generated a set of Java interfaces describing
     * created from a schema. The supplied table is
     * 
     * @param request
     * @param table
     * @param jsonp
     * @return
     * @throws Exception
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/igen")
    public Response get(@Context final HttpServletRequest request, @QueryParam(TABLE_ARG_NAME) final String table,
            @QueryParam(JSONP_ARG_NAME) final String jsonp) throws Exception {

        return RestResponseBuilder.forRequest(request).body(new BodyGenerator() {
            @Override
            public void write(PrintWriter writer) throws Exception {
                final TableName tableName = ResourceHelper.parseTableName(request, table == null ? "*" : table);
                final String schemaName = tableName.getSchemaName();
                final AkibanInformationSchema ais = reqs.dxlService.ddlFunctions().getAIS(
                        reqs.sessionService.createSession());
                if (ais.getSchema(schemaName) == null) {
                    throw new NoSuchSchemaException(schemaName);
                }
                ClassBuilder helper = new ClassSourceWriter(writer, PACKAGE, false);
                helper.writeGeneratedInterfaces(ais, tableName);
                helper.close();
            }
        }).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/cgen")
    public Response cgen(@Context final HttpServletRequest request, @QueryParam(TABLE_ARG_NAME) final String table,
            @QueryParam(JSONP_ARG_NAME) final String jsonp) throws Exception {

        return RestResponseBuilder.forRequest(request).body(new BodyGenerator() {
            @Override
            public void write(PrintWriter writer) throws Exception {
                final TableName tableName = ResourceHelper.parseTableName(request, table == null ? "*" : table);
                final String schemaName = tableName.getSchemaName();
                final AkibanInformationSchema ais = reqs.dxlService.ddlFunctions().getAIS(
                        reqs.sessionService.createSession());
                if (ais.getSchema(schemaName) == null) {
                    throw new NoSuchSchemaException(schemaName);
                }
                ClassBuilder helper = new ClassSourceWriter(writer, PACKAGE, false);
                helper.writeGeneratedClass(ais, tableName);
                helper.close();
            }
        }).build();
    }

    @PUT
    @Produces(MediaType.APPLICATION_JSON)
    @Path("procedure")
    public Response createProcedure(@Context final HttpServletRequest request,
            @QueryParam(MODULE_ARG_NAME) @DefaultValue("DefaultModule") final String module,
            @QueryParam(LANGUAGE) @DefaultValue("Javascript") final String language,
            @QueryParam(JSONP_ARG_NAME) final String jsonp, final byte[] payload) {
        return RestResponseBuilder.forRequest(request).body(new BodyGenerator() {
            @Override
            public void write(PrintWriter writer) throws Exception {
                final TableName procName = ResourceHelper.parseTableName(request, module);
                final String sql = String.format(CREATE_PROCEDURE_FORMAT, procName, language, new String(payload));
                reqs.restDMLService.runSQL(writer, request, sql);
            }
        }).build();
    }

    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Path("procedure")
    public Response deleteProcedure(@Context final HttpServletRequest request,
            @QueryParam(MODULE_ARG_NAME) @DefaultValue("DefaultModule") final String module,
            @QueryParam(JSONP_ARG_NAME) final String jsonp) {
        return RestResponseBuilder.forRequest(request).body(new BodyGenerator() {
            @Override
            public void write(PrintWriter writer) throws Exception {
                final TableName procName = ResourceHelper.parseTableName(request, module);
                final String sql = String.format(DROP_PROCEDURE_FORMAT, procName);
                reqs.restDMLService.runSQL(writer, request, sql);
            }
        }).build();

    }

    
}

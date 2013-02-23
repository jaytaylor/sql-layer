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

import com.akiban.ais.model.TableName;
import com.akiban.rest.ResourceRequirements;

import java.security.Principal;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import com.akiban.rest.RestResponseBuilder;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Entity based access (GET), creation (PUT, POST), and modification (PUT, DELETE)
 */
@Path("/entity/{entity}")
public class EntityResource {
    private static final String IDENTIFIERS_MULTI = "{identifiers:.*}";
    private final ResourceRequirements reqs;
    
    public EntityResource(ResourceRequirements reqs) {
        this.reqs = reqs;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response retrieveEntity(@Context HttpServletRequest request,
                                   @QueryParam("format") String format,
                                   @QueryParam("jsonp") String jsonp,
                                   @PathParam("entity") String entity,
                                   @QueryParam("depth") Integer depth) throws Exception {
        TableName tableName = parseTableName(request, entity);
        checkTableAccessible(request, tableName);
        RestResponseBuilder builder = RestResponseBuilder.builderFromRequest(format, jsonp);
        reqs.restDMLService.getAllEntities(builder, tableName, depth);
        return builder.build();
    }

    @GET
    @Path("/" + IDENTIFIERS_MULTI)
    @Produces(MediaType.APPLICATION_JSON)
    public Response retrieveEntity(@Context HttpServletRequest request,
                                   @QueryParam("format") String format,
                                   @QueryParam("jsonp") String jsonp,
                                   @PathParam("entity") String entity,
                                   @QueryParam("depth") Integer depth,
                                   @Context UriInfo uri) throws Exception {
        TableName tableName = parseTableName(request, entity);
        checkTableAccessible(request, tableName);
        RestResponseBuilder builder = RestResponseBuilder.builderFromRequest(format, jsonp);
        reqs.restDMLService.getEntities(builder, tableName, depth, getPKString(uri));
        return builder.build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createEntity(@Context HttpServletRequest request,
                                 @QueryParam("format") String format,
                                 @QueryParam("jsonp") String jsonp,
                                 @PathParam("entity") String entity,
                                 byte[] entityBytes) throws Exception {
        TableName tableName = parseTableName(request, entity);
        checkTableAccessible(request, tableName);
        ObjectMapper m = new ObjectMapper();
        JsonNode node = m.readTree(entityBytes);
        RestResponseBuilder builder = RestResponseBuilder.builderFromRequest(format, jsonp);
        reqs.restDMLService.insert(builder, tableName, node);
        return builder.build();
    }

    @PUT
    @Path("/" + IDENTIFIERS_MULTI)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateEntity(@Context HttpServletRequest request,
                                 @QueryParam("format") String format,
                                 @QueryParam("jsonp") String jsonp,
                                 @PathParam("entity") String entity,
                                 byte[] entityBytes,
                                 @Context UriInfo uri) throws Exception {
        TableName tableName = parseTableName(request, entity);
        checkTableAccessible(request, tableName);
        ObjectMapper m = new ObjectMapper();
        JsonNode node = m.readTree(entityBytes);
        RestResponseBuilder builder = RestResponseBuilder.builderFromRequest(format, jsonp);
        reqs.restDMLService.update(builder, tableName, getPKString(uri), node);
        return builder.build();
    }

    @DELETE
    @Path("/" + IDENTIFIERS_MULTI)
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteEntity(@Context HttpServletRequest request,
                                 @QueryParam("format") String format,
                                 @QueryParam("jsonp") String jsonp,
                                 @PathParam("entity") String entity,
                                 @Context UriInfo uri) throws Exception {
        TableName tableName = parseTableName(request, entity);
        checkTableAccessible(request, tableName);
        RestResponseBuilder builder = RestResponseBuilder.builderFromRequest(format, jsonp);
        reqs.restDMLService.delete(builder, tableName, getPKString(uri));
        return builder.build();
    }

    private void checkTableAccessible(HttpServletRequest request, TableName name) {
        if(!reqs.securityService.isAccessible(request, name.getSchemaName())) {
            throw new WebApplicationException(RestResponseBuilder.FORBIDDEN_RESPONSE);
        }
    }

    private static String getPKString(UriInfo uri) {
        String pks[] = uri.getPath(false).split("/");
        assert pks.length > 0: uri;
        return pks[pks.length - 1];
    }

    static TableName parseTableName(HttpServletRequest request, String name) {
        Principal user = request.getUserPrincipal();
        String schema = (user == null) ? "" : user.getName();
        return TableName.parse(schema, name);
    }
}

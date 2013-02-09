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


import com.akiban.rest.ResponseHelper;
import com.akiban.server.service.restdml.RestDMLService;
import com.google.inject.Inject;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Implementation of REST-oriented Get, Multi-Get, Create, Update, Delete and
 * Multi-Delete.
 */
@Path("/{schema}.{table}")
public class DataAccessOperationsResource {
    
    @Inject
    RestDMLService dmlService;
    
    JsonFactory jsonFactory = new JsonFactory();
    
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response retrieveEntity(@QueryParam("format") String format,
                                   @QueryParam("jsoncallback") String jsonp,
                                   @PathParam("schema") String schema,
                                   @PathParam("table") String table,
                                   @QueryParam("depth") Integer depth,
                                   @QueryParam("offset") Integer offset,
                                   @QueryParam("limit") Integer limit) throws Exception {
        return dmlService.getAllEntities(schema, table, depth);
    }

    @GET
    @Path("{identifiers:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response retrieveEntity(@QueryParam("format") String format,
                                   @QueryParam("jsoncallback") String jsonp,
                                   @PathParam("schema") String schema,
                                   @PathParam("table") String table,
                                   @QueryParam("depth") Integer depth,
                                   @Context UriInfo uri) throws Exception {
        String[] pks = uri.getPath(false).split("/");
        assert pks.length > 0 : uri;
        return dmlService.getEntities(schema, table, depth, pks[pks.length-1]);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createEntity(@PathParam("schema") final String schema,
                                 @PathParam("table") final String table,
                                 final byte[] entityBytes) throws Exception {
        ObjectMapper m = new ObjectMapper();
        JsonNode node = m.readTree(entityBytes);
        
        return dmlService.insert(schema, table, node);
    }

    @PUT
    @Path("{identifiers:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateEntity(@PathParam("schema") final String schema,
                                 @PathParam("table") final String table,
                                 final byte[] entityBytes,
                                 @Context final UriInfo uri) throws Exception {
        return ResponseHelper.buildNotYetImplemented();
    }

    @DELETE
    @Path("{identifiers:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteEntity(@PathParam("schema") String schema,
                                 @PathParam("table") String table,
                                 @Context UriInfo uri) throws Exception {
        String[] pks = uri.getPath(false).split("/");
        assert pks.length > 0 : uri;
        return dmlService.delete(schema, table, pks[pks.length-1]);
    }
}

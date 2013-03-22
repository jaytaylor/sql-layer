
package com.akiban.rest.resources;

import com.akiban.ais.model.IndexName;
import com.akiban.rest.ResourceRequirements;
import com.akiban.rest.RestResponseBuilder;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.PrintWriter;

import static com.akiban.rest.resources.ResourceHelper.MEDIATYPE_JSON_JAVASCRIPT;

/**
 * Full text query against index.
 */
@Path("/text/{table}/{index}")
public class FullTextResource {
    private final ResourceRequirements reqs;

    public FullTextResource(ResourceRequirements reqs) {
        this.reqs = reqs;
    }

    @GET
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response textSearch(@Context final HttpServletRequest request,
                               @PathParam("table") String table,
                               @PathParam("index") String index,
                               @QueryParam("q") final String query,
                               @QueryParam("depth") final Integer depth,
                               @QueryParam("size") final Integer limit) throws Exception {
        final IndexName indexName = new IndexName(ResourceHelper.parseTableName(request, table), index);
        ResourceHelper.checkSchemaAccessible(reqs.securityService, request, indexName.getSchemaName());
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        reqs.restDMLService.fullTextSearch(writer, indexName, depth, query, limit);
                    }
                })
                .build();
    }

    @POST
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response textSearch(@Context final HttpServletRequest request,
                               @PathParam("table") String table,
                               @PathParam("index") String index) throws Exception {
        final IndexName indexName = new IndexName(ResourceHelper.parseTableName(request, table), index);
        ResourceHelper.checkSchemaAccessible(reqs.securityService, request, indexName.getSchemaName());
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        reqs.restDMLService.refreshFullTextIndex(writer, indexName);
                    }
                })
                .build();
    }
}

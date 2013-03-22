package com.akiban.rest.resources;

import static com.akiban.rest.resources.ResourceHelper.MEDIATYPE_JSON_JAVASCRIPT;
import static com.akiban.rest.resources.ResourceHelper.checkTableAccessible;
import static com.akiban.rest.resources.ResourceHelper.parseTableName;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Collections;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import com.akiban.ais.model.TableName;
import com.akiban.rest.ResourceRequirements;
import com.akiban.rest.RestResponseBuilder;
import com.akiban.server.error.WrongExpressionArityException;

@Path("/view/{view}")

public class ViewResource {
    private final ResourceRequirements reqs;
    
    public ViewResource(ResourceRequirements reqs) {
        this.reqs = reqs;
    }

    @GET
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response retrieveEntity(@Context final HttpServletRequest request,
                                   @PathParam("view") String view,
                                   @Context final UriInfo uri) {
        final TableName tableName = parseTableName(request, view);
        checkTableAccessible(reqs.securityService, request, tableName);
        StringBuilder query = new StringBuilder();
        query.append("SELECT * FROM ");
        query.append(tableName.toString());
        List<String> params = new ArrayList<>();
        boolean first = true;
        
        for (Map.Entry<String,List<String>> entry : uri.getQueryParameters().entrySet()) {
            if (entry.getValue().size() != 1)
                throw new WrongExpressionArityException(1, entry.getValue().size());
            if (ResourceHelper.JSONP_ARG_NAME.equals(entry.getKey()))
                continue;
            if (first) {
                query.append(" WHERE ");
                first = false;
            } else {
                query.append(" AND ");
            }
            query.append (entry.getKey());
            query.append("=?");
            params.add(entry.getValue().get(0));
        }

        final String queryFinal = query.toString();
        final List<String> parameters = Collections.unmodifiableList(params);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        reqs.restDMLService.runSQLParameter(writer, request, queryFinal, parameters);
                    }
                })
                .build();
    }
}

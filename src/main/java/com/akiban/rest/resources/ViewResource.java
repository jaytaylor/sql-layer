/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
import com.akiban.http.SimpleHandlerList;
import com.akiban.rest.ResourceRequirements;
import com.akiban.rest.RestResponseBuilder;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.util.tap.InOutTap;

@Path("/view/{view}")

public class ViewResource {
    private final ResourceRequirements reqs;
    private static final InOutTap VIEW_GET = SimpleHandlerList.REST_TAP.createSubsidiaryTap("rest: view GET");
    
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
        VIEW_GET.in();
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
        try {        
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        reqs.restDMLService.runSQLParameter(writer, request, queryFinal, parameters);
                    }
                })
                .build();
        } finally {
            VIEW_GET.out();
        }
    }
}

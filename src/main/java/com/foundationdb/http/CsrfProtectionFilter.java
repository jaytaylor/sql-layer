/**
 * Copyright (C) 2009-2014 FoundationDB, LLC
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

package com.foundationdb.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.net.URI;

/**
 * This is similar to the builtin CsrfProtectionFilter, but does not allow GET requests either, because
 * those can execute any SQL query.
 */
@Provider
@Priority(Priorities.AUTHENTICATION)
public class CsrfProtectionFilter implements ContainerRequestFilter {
    private static final Logger logger = LoggerFactory.getLogger(CsrfProtectionFilter.class);

    public static final String REFERER_HEADER = "Referer";
    @Override
    public void filter(ContainerRequestContext containerRequestContext) throws IOException {
        String referer = containerRequestContext.getHeaderString(REFERER_HEADER);
        try {
            URI uri = URI.create(referer);
            // TODO switch to server properties
            // and write TESTS
            if (!"http".equals(uri.getScheme()) || !"localhost".equals(uri.getHost()) || 4567 !=uri.getPort()) {

                logger.debug("CSRF attempt blocked due to invalid uri {} {}",
                        containerRequestContext.getUriInfo().getAbsolutePath(),
                        containerRequestContext.getHeaders());
                containerRequestContext.abortWith(Response.status(Response.Status.BAD_REQUEST).build());
            }
        } catch (NullPointerException|IllegalArgumentException e) {
            logger.debug("CSRF attempt blocked due to invalid uri {} {} - {}",
                    containerRequestContext.getUriInfo().getAbsolutePath(),
                    containerRequestContext.getHeaders(),
                    e.getMessage());
            containerRequestContext.abortWith(Response.status(Response.Status.BAD_REQUEST).build());
        }
    }
}

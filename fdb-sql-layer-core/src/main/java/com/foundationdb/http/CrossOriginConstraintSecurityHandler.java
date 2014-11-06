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

package com.foundationdb.http;

import org.eclipse.jetty.http.HttpMethods;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.RoleInfo;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.servlets.CrossOriginFilter;

/**
 * <p>
 *     An subclass of {@link ConstraintSecurityHandler} that allows a CORS preflight through without authentication.
 * </p>
 * <p>
 *     Preflight processing is required for an non-simple request, which includes <i>any</i> request that requires
 *     authentication. It also <b>cannot</b> be authentication itself. This handler allows just the preflight OPTIONS
 *     request to come through un-authenticated, which allows the proper {@link CrossOriginFilter} handling to kick in.
 * </p>
 * <p>
 *     See the <a href="http://www.w3.org/TR/cors/#simple-header">CORS spec</a> for reference, particularly the
 *     <a href="http://www.w3.org/TR/cors/#resource-preflight-requests">Preflight</a> and
 *     <a href="http://www.w3.org/TR/cors/#simple-header">Simple Header</a> sections.
 * </p>
 */
public class CrossOriginConstraintSecurityHandler extends ConstraintSecurityHandler {
    private static final String ORIGIN_HEADER = "Origin";

    @Override
    protected boolean isAuthMandatory(Request baseRequest, Response baseResponse, Object constraintInfo) {
        if(constraintInfo == null) {
            return false;
        }
        if(isPreFlightRequest(baseRequest)) {
            return false;
        }
        return ((RoleInfo)constraintInfo).isChecked();
    }

    private static boolean isPreFlightRequest(Request request) {
        if(HttpMethods.OPTIONS.equalsIgnoreCase(request.getMethod())) {
            // If the origin does not match allowed the filter will skip anyway so don't bother checking it.
            if(request.getHeader(ORIGIN_HEADER) != null &&
               request.getHeader(CrossOriginFilter.ACCESS_CONTROL_REQUEST_METHOD_HEADER) != null) {
                return true;
            }
        }
        return false;
    }
}

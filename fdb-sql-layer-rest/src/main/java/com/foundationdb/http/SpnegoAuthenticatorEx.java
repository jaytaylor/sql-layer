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

import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.security.authentication.DeferredAuthentication;
import org.eclipse.jetty.security.authentication.SpnegoAuthenticator;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Don't treat configuration / protocol errors as <code>Authentication.UNAUTHENTICATED</code> when mandatory.
 */
public class SpnegoAuthenticatorEx extends SpnegoAuthenticator 
{
    private static final Logger LOG = Log.getLogger(SpnegoAuthenticatorEx.class);
    
    @Override
    public Authentication validateRequest(ServletRequest request, ServletResponse response, boolean mandatory) throws ServerAuthException {
        Authentication result = super.validateRequest(request, response, mandatory);
        if ((result == Authentication.UNAUTHENTICATED) &&
            mandatory &&
            !DeferredAuthentication.isDeferred((HttpServletResponse)response)) {
            LOG.debug("SpengoAuthenticatorEx: unauthenticated -> forbidden");
            try {
                ((HttpServletResponse)response).sendError(Response.SC_FORBIDDEN,
                                                          "negotiation failure");
            }
            catch (IOException ex) {
                throw new ServerAuthException(ex);
            }
            result = Authentication.SEND_FAILURE;
        }
        return result;
    }
}

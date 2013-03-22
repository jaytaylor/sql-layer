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

package com.akiban.rest;

import com.akiban.http.HttpConductor;
import com.akiban.rest.resources.*;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.restdml.RestDMLService;
import com.akiban.server.service.security.SecurityService;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.transaction.TransactionService;
import com.google.inject.Inject;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;

import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.Arrays;

public class RestServiceImpl implements RestService, Service {
    private final ConfigurationService configService;
	private final HttpConductor http;
    // Used by various resources
    private final RestDMLService restDMLService;
    private final SessionService sessionService;
    private final TransactionService transactionService;
    private final SecurityService securityService;
    private final DXLService dxlService;

	private volatile ServletContextHandler handler;
	

	@Inject
    public RestServiceImpl(ConfigurationService configService,
                           HttpConductor http,
                           RestDMLService restDMLService,
                           SessionService sessionService,
                           TransactionService transactionService,
                           SecurityService securityService,
                           DXLService dxlService) {
        this.configService = configService;
		this.http = http;
        this.restDMLService = restDMLService;
        this.sessionService = sessionService;
        this.transactionService = transactionService;
        this.securityService = securityService;
        this.dxlService = dxlService;
    }

    @Override
    public String getContextPath() {
        return configService.getProperty("akserver.rest.context_path");
    }

	@Override
	public void start() {
		registerConnector(http);
	}

	@Override
	public void stop() {
        http.unregisterHandler(handler);
        this.handler = null;
	}

	@Override
	public void crash() {
		stop();
	}

	private void registerConnector(HttpConductor http) {
        handler = new ServletContextHandler();
        handler.setContextPath(getContextPath());
        handler.addServlet(new ServletHolder(new ServletContainer(createResourceConfigV1())), "/*");
        http.registerHandler(handler);
	}

    private ResourceConfig createResourceConfigV1() {
        DefaultResourceConfig config = new DefaultResourceConfig();
        ResourceRequirements reqs = new ResourceRequirements(
                dxlService, restDMLService, securityService, sessionService, transactionService, this
        );
        config.getSingletons().addAll(Arrays.asList(
                new EntityResource(reqs),
                new FullTextResource(reqs),
                new ModelResource(reqs),
                new ProcedureCallResource(reqs),
                new SecurityResource(reqs),
                new SQLResource(reqs),
                new VersionResource(reqs),
                new DirectResource(reqs),
                new ViewResource(reqs),
                // This must be last to capture anything not handled above 
                new DefaultResource(reqs)
        ));
        return config;
    }
}

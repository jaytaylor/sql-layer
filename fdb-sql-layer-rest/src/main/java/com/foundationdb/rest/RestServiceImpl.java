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

package com.foundationdb.rest;

import com.foundationdb.http.HttpConductor;
import com.foundationdb.rest.dml.RestDMLService;
import com.foundationdb.rest.resources.*;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.dxl.DXLService;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.Store;
import com.google.inject.Inject;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.HashSet;
import java.util.Set;

public class RestServiceImpl implements RestService, Service {
    private final ConfigurationService configService;
	private final HttpConductor http;
    // Used by various resources
    private final ResourceRequirements reqs;

	private volatile ServletHolder servletHolder;
	
	private static final String RESOURCE_LIST = "fdbsql.rest.resource";
	

	@Inject
    public RestServiceImpl(ConfigurationService configService,
                           HttpConductor http,
                           RestDMLService restDMLService,
                           SessionService sessionService,
                           TransactionService transactionService,
                           SecurityService securityService,
                           DXLService dxlService,
                           Store store) {
        this.configService = configService;
		this.http = http;
        this.reqs = new ResourceRequirements(
            dxlService,
            restDMLService,
            securityService,
            sessionService,
            transactionService,
            store,
            configService
        );
    }

    @Override
    public String getContextPath() {
        return configService.getProperty("fdbsql.rest.context_path");
    }

	@Override
	public void start() {
		registerConnector(http);
	}

	@Override
	public void stop() {
        http.unregisterHandler(servletHolder);
        this.servletHolder = null;
	}

	@Override
	public void crash() {
		stop();
	}

	private void registerConnector(HttpConductor http) {
        String path = getContextPath() + "/*";
        servletHolder = new ServletHolder(new ServletContainer(createResourceConfigV1()));
        http.registerHandler(servletHolder, path);
	}

    private ResourceConfig createResourceConfigV1() {
        String resource_list = configService.getProperty(RESOURCE_LIST);

        
        Set<Object> resources = new HashSet<>();
        if (resource_list.contains("entity")) {
            resources.add(new EntityResource(reqs));
        }
        if (resource_list.contains("fulltext")) {
            resources.add(new FullTextResource(reqs));
        }
        if (resource_list.contains("procedurecall")) {
            resources.add(new ProcedureCallResource(reqs));
        }
        if (resource_list.contains("security")) {
            resources.add(new SecurityResource(reqs));
        }
        if (resource_list.contains("sql")) {
            resources.add(new SQLResource(reqs));
        }
        if (resource_list.contains("version")) {
            resources.add(new VersionResource(reqs));
        }
        if (resource_list.contains("view")) {
            resources.add(new ViewResource(reqs));
        }
        // This must be last to capture anything not handled above
        resources.add(new DefaultResource());


        ResourceConfig config = new ResourceConfig();
        config.registerInstances(resources);
        return config;
    }
}

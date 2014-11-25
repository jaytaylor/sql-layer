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

import com.foundationdb.rest.dml.RestDMLService;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.dxl.DXLService;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.Store;

public class ResourceRequirements {
    public final DXLService dxlService;
    public final RestDMLService restDMLService;
    public final SecurityService securityService;
    public final SessionService sessionService;
    public final TransactionService transactionService;
    public final Store store;
    public final ConfigurationService configService;

    public ResourceRequirements(DXLService dxlService,
                                RestDMLService restDMLService,
                                SecurityService securityService,
                                SessionService sessionService,
                                TransactionService transactionService,
                                Store store,
                                ConfigurationService configService) {
        this.dxlService = dxlService;
        this.restDMLService = restDMLService;
        this.securityService = securityService;
        this.sessionService = sessionService;
        this.transactionService = transactionService;
        this.store = store;
        this.configService = configService;
    }
}

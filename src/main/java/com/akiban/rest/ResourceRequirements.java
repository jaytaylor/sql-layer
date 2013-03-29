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

import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.restdml.RestDMLService;
import com.akiban.server.service.routines.RoutineLoader;
import com.akiban.server.service.security.SecurityService;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.Store;

public class ResourceRequirements {
    public final DXLService dxlService;
    public final RestDMLService restDMLService;
    public final SecurityService securityService;
    public final SessionService sessionService;
    public final TransactionService transactionService;
    public final Store store;
    public final TreeService treeService;
    public final ConfigurationService configService;
    public final RoutineLoader routineLoader;

    public ResourceRequirements(DXLService dxlService,
                                RestDMLService restDMLService,
                                SecurityService securityService,
                                SessionService sessionService,
                                TransactionService transactionService,
                                Store store,
                                TreeService treeService,
                                ConfigurationService configService,
                                RoutineLoader routineLoader) {
        this.dxlService = dxlService;
        this.restDMLService = restDMLService;
        this.securityService = securityService;
        this.sessionService = sessionService;
        this.transactionService = transactionService;
        this.store = store;
        this.treeService = treeService;
        this.configService = configService;
        this.routineLoader = routineLoader;
    }
}

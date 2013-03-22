
package com.akiban.rest;

import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.restdml.RestDMLService;
import com.akiban.server.service.security.SecurityService;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.transaction.TransactionService;

public class ResourceRequirements {
    public final DXLService dxlService;
    public final RestDMLService restDMLService;
    public final SecurityService securityService;
    public final SessionService sessionService;
    public final TransactionService transactionService;
    public final RestService restService;

    public ResourceRequirements(DXLService dxlService,
                                RestDMLService restDMLService,
                                SecurityService securityService,
                                SessionService sessionService,
                                TransactionService transactionService,
                                RestService restService) {
        this.dxlService = dxlService;
        this.restDMLService = restDMLService;
        this.securityService = securityService;
        this.sessionService = sessionService;
        this.transactionService = transactionService;
        this.restService = restService;
    }
}

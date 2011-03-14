/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.mttests.mthapi.ddlandhapi;

import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.mttests.mthapi.base.sais.SaisTable;
import com.akiban.server.mttests.mthapi.common.BasicHapiSuccess;

import java.util.Collections;
import java.util.EnumSet;

abstract class OptionallyWorkingReadThread extends BasicHapiSuccess {
    private final EnumSet<HapiRequestException.ReasonCode> validErrors;
    protected OptionallyWorkingReadThread(String schema, SaisTable root, HapiRequestException.ReasonCode... validErrors) {
        super(schema, root);
        this.validErrors = EnumSet.noneOf(HapiRequestException.ReasonCode.class);
        Collections.addAll(this.validErrors, validErrors);
    }

    @Override
    protected void validateErrorResponse(HapiGetRequest request, Throwable exception)
            throws UnexpectedException
    {
        if (exception instanceof HapiRequestException) {
            HapiRequestException hre = (HapiRequestException) exception;
            if (validErrors.contains(hre.getReasonCode())) {
                return;
            }
        }
        super.validateErrorResponse(request, exception);
    }
}

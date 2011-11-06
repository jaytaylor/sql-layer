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

package com.akiban.sql.optimizer.plan;

import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.qp.exec.UpdatePlannable;

import static com.akiban.server.expression.std.EnvironmentExpression.EnvironmentValue;

import java.util.Arrays;
import java.util.List;

/** Physical INSERT/UPDATE/DELETE statement */
public class PhysicalUpdate extends BasePlannable
{
    public PhysicalUpdate(UpdatePlannable updatePlannable,
                          DataTypeDescriptor[] parameterTypes,
                          List<EnvironmentValue> environmentValues) {
        super(updatePlannable, parameterTypes, environmentValues);
    }

    public UpdatePlannable getUpdatePlannable() {
        return (UpdatePlannable)getPlannable();
    }

    @Override
    public boolean isUpdate() {
        return true;
    }

    @Override
    protected String withIndentedExplain(StringBuilder str) {
        if (getParameterTypes() != null)
            str.append(Arrays.toString(getParameterTypes()));
        if (getEnvironmentValues() != null)
            str.append(getEnvironmentValues());
        return super.withIndentedExplain(str);
    }

}

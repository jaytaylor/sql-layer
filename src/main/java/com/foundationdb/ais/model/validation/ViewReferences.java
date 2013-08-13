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

package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Columnar;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.View;
import com.akiban.server.error.BadAISReferenceException;

import java.util.Collection;
import java.util.Map;

/**
 * Validates references from a view exist.
 *
 */
class ViewReferences implements AISValidation {
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (View view : ais.getViews().values()) {
            for (Map.Entry<TableName,Collection<String>> entry : view.getTableColumnReferences().entrySet()) {
                Columnar table = ais.getColumnar(entry.getKey());
                if (table == null) {
                    output.reportFailure(new AISValidationFailure(new BadAISReferenceException ("view", view.getName().toString(), "table", entry.getKey().toString())));
                }
                else {
                    for (String colname : entry.getValue()) {
                        Column column = table.getColumn(colname);
                        if (column == null) {
                            output.reportFailure(new AISValidationFailure(new BadAISReferenceException ("view", view.getName().toString(), "column", entry.getKey() + "." + colname)));
                        }
                    }
                }
            }
        }
    }
}

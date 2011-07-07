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
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.message.ErrorCode;

public class SupportedColumnTypes implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (UserTable table : ais.getUserTables().values()) {
            for (Column column : table.getColumnsIncludingInternal()) {
                if (!ais.isTypeSupported(column.getType().name())) {
                    output.reportFailure(new AISValidationFailure (ErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Table %s has column %s with unsupported data type %s", 
                            table.getName().toString(), column.getName(), column.getType().name()));
                }
            }
            for (TableIndex index : table.getIndexesIncludingInternal()) {
                for (IndexColumn column : index.getColumns()) {
                    if (!ais.isTypeSupportedAsIndex(column.getColumn().getType().name())) {
                        output.reportFailure(new AISValidationFailure (ErrorCode.UNSUPPORTED_INDEX_DATA_TYPE,
                                "Index %s has column %s with unspported index data type %s",
                                index.getIndexName().toString(), column.getColumn().getName(), 
                                column.getColumn().getType().name()));
                    }
                }
            }
        }
        for (Group group : ais.getGroups().values()) {
            for (GroupIndex index : group.getIndexes()) {
                for (IndexColumn column : index.getColumns()) {
                    if (!ais.isTypeSupportedAsIndex(column.getColumn().getType().name())) {
                        output.reportFailure(new AISValidationFailure (ErrorCode.UNSUPPORTED_INDEX_DATA_TYPE,
                                "Index %s has column %s with unspported index data type %s",
                                index.getIndexName().toString(), column.getColumn().getName(), 
                                column.getColumn().getType().name()));
                    }
                }
            }
        }
    }
}

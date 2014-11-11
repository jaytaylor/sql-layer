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
package com.foundationdb.sql.aisddl;

import com.foundationdb.ais.model.AISBuilder;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.error.NoSuchSequenceException;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.sql.parser.CreateSequenceNode;
import com.foundationdb.sql.parser.DropSequenceNode;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.sql.types.TypeId;

import static com.foundationdb.sql.aisddl.DDLHelper.skipOrThrow;

public class SequenceDDL {
    private SequenceDDL() { }
    
    public static void createSequence (DDLFunctions ddlFunctions,
                                    Session session,
                                    String defaultSchemaName,
                                    CreateSequenceNode node) {
        final TableName seqName = DDLHelper.convertName(defaultSchemaName, node.getObjectName());
        // Implementation defined if unspecified
        long minValue = (node.getMinValue() != null) ? node.getMinValue() : 1;
        long maxValue = (node.getMaxValue() != null) ? node.getMaxValue() : Long.MAX_VALUE;
        // Standard compliant defaults
        long startWith = (node.getStartWith() != null) ? node.getStartWith() : minValue;
        long incBy = (node.getIncrementBy() != null) ? node.getIncrementBy() : 1;
        boolean isCycle = (node.isCycle() != null) ? node.isCycle() : false;
        // Sequence doesn't have a backing SQL data type so just limit the max if one was given
        if((node.getMaxValue() == null) && (node.getDataType() != null)) {
            TypeId typeId = node.getDataType().getTypeId();
            if(typeId == TypeId.TINYINT_ID) {
                maxValue = Byte.MAX_VALUE;
            } else if(typeId == TypeId.SMALLINT_ID) {
                maxValue = Short.MAX_VALUE;
            } else if(typeId == TypeId.INTEGER_ID) {
                maxValue = Integer.MAX_VALUE;
            }
            // else keep long max
        }
        AISBuilder builder = new AISBuilder();
        builder.sequence(seqName.getSchemaName(), seqName.getTableName(), startWith, incBy, minValue, maxValue, isCycle);
        Sequence sequence = builder.akibanInformationSchema().getSequence(seqName);
        if (node.getStorageFormat() != null) {
            TableDDL.setStorage(ddlFunctions, sequence, node.getStorageFormat());
        }
        ddlFunctions.createSequence(session, sequence);
    }
    
    public static void dropSequence (DDLFunctions ddlFunctions,
                                        Session session,
                                        String defaultSchemaName,
                                        DropSequenceNode dropSequence,
                                        QueryContext context) {
        final TableName sequenceName = DDLHelper.convertName(defaultSchemaName, dropSequence.getObjectName());

        Sequence sequence = ddlFunctions.getAIS(session).getSequence(sequenceName);
        if((sequence == null) &&
           skipOrThrow(context, dropSequence.getExistenceCheck(), sequence, new NoSuchSequenceException(sequenceName))) {
            return;
        }

        ddlFunctions.dropSequence(session, sequenceName);
    }
}

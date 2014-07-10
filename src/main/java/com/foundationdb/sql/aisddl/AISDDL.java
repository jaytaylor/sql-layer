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

import com.foundationdb.sql.parser.AlterTableNode;
import com.foundationdb.sql.parser.CreateAliasNode;
import com.foundationdb.sql.parser.CreateIndexNode;
import com.foundationdb.sql.parser.CreateSequenceNode;
import com.foundationdb.sql.parser.CreateTableNode;
import com.foundationdb.sql.parser.CreateSchemaNode;
import com.foundationdb.sql.parser.CreateViewNode;
import com.foundationdb.sql.parser.DropAliasNode;
import com.foundationdb.sql.parser.DropGroupNode;
import com.foundationdb.sql.parser.DropIndexNode;
import com.foundationdb.sql.parser.DropSequenceNode;
import com.foundationdb.sql.parser.DropTableNode;
import com.foundationdb.sql.parser.DropSchemaNode;
import com.foundationdb.sql.parser.DDLStatementNode;
import com.foundationdb.sql.parser.DropViewNode;
import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.RenameNode;
import com.foundationdb.sql.server.ServerQueryContext;
import com.foundationdb.sql.server.ServerSession;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.types.common.types.TypesTranslator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AISDDL
{
    private static final Logger logger = LoggerFactory.getLogger(AISDDL.class);

    private AISDDL() {}
    
    public static void execute(DDLStatementNode ddl, String sql, 
                               ServerQueryContext<?> context) {
        ServerSession server = context.getServer();
        AkibanInformationSchema ais = server.getAIS();
        String schema = server.getDefaultSchemaName();
        logger.info("DDL in {}: {}", schema, sql);
        DDLFunctions ddlFunctions = server.getDXL().ddlFunctions();
        Session session = server.getSession();
        switch (ddl.getNodeType()) {
        case NodeTypes.CREATE_SCHEMA_NODE:
            SchemaDDL.createSchema(ais, schema, (CreateSchemaNode)ddl, context);
            return;
        case NodeTypes.DROP_SCHEMA_NODE:
            SchemaDDL.dropSchema(ddlFunctions, session, (DropSchemaNode)ddl, context);
            return;
        case NodeTypes.CREATE_TABLE_NODE:
            TableDDL.createTable(ddlFunctions, session, schema, (CreateTableNode)ddl, context);
            return;
        case NodeTypes.DROP_TABLE_NODE:
            TableDDL.dropTable(ddlFunctions, session, schema, (DropTableNode)ddl, context);
            return;
        case NodeTypes.DROP_GROUP_NODE:
            TableDDL.dropGroup(ddlFunctions, session, schema, (DropGroupNode)ddl, context);
            return;
        case NodeTypes.CREATE_VIEW_NODE:
            ViewDDL.createView(ddlFunctions, session, schema, (CreateViewNode)ddl,
                               server.getBinderContext(), context);
            return;
        case NodeTypes.DROP_VIEW_NODE:
            ViewDDL.dropView(ddlFunctions, session, schema, (DropViewNode)ddl,
                             server.getBinderContext(), context);
            return;
        case NodeTypes.CREATE_INDEX_NODE:
            IndexDDL.createIndex(ddlFunctions, session, schema, (CreateIndexNode)ddl, context);
            return;
        case NodeTypes.DROP_INDEX_NODE:
            IndexDDL.dropIndex(ddlFunctions, session, schema, (DropIndexNode)ddl, context);
            return;
        case NodeTypes.ALTER_TABLE_NODE:
            AlterTableDDL.alterTable(ddlFunctions, server.getDXL().dmlFunctions(), session, schema, (AlterTableNode)ddl, context);
            return;
        case NodeTypes.RENAME_NODE:
            switch (((RenameNode)ddl).getRenameType()) {
            case INDEX:
                IndexDDL.renameIndex(ddlFunctions, session, schema, (RenameNode)ddl);
                return;
            case TABLE:
                TableDDL.renameTable(ddlFunctions, session, schema, (RenameNode)ddl);
                return;
            }
            break;
        case NodeTypes.CREATE_SEQUENCE_NODE:
            SequenceDDL.createSequence(ddlFunctions, session, schema, (CreateSequenceNode)ddl);
            return;
        case NodeTypes.DROP_SEQUENCE_NODE:
            SequenceDDL.dropSequence(ddlFunctions, session, schema, (DropSequenceNode)ddl, context);
            return;
        case NodeTypes.CREATE_ALIAS_NODE:
            switch (((CreateAliasNode)ddl).getAliasType()) {
            case PROCEDURE:
            case FUNCTION:
                RoutineDDL.createRoutine(ddlFunctions, server.getRoutineLoader(), session, schema, (CreateAliasNode)ddl);
                return;
            }
            break;
        case NodeTypes.DROP_ALIAS_NODE:
            switch (((DropAliasNode)ddl).getAliasType()) {
            case PROCEDURE:
            case FUNCTION:
                RoutineDDL.dropRoutine(ddlFunctions, server.getRoutineLoader(), session, schema, (DropAliasNode)ddl, context);
                return;
            }
            break;
        }
        throw new UnsupportedSQLException(ddl.statementToString(), ddl);
    }
}

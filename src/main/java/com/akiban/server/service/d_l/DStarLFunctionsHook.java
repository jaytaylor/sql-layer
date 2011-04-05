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

package com.akiban.server.service.d_l;

import com.akiban.server.service.session.Session;

public interface DStarLFunctionsHook {
    static enum DStarLType {
        DDL_FUNCTIONS_WRITE,
        DDL_FUNCTIONS_READ,
        DML_FUNCTIONS,
    }

    static enum DDLFunction {
        CREATE_TABLE(DStarLType.DDL_FUNCTIONS_WRITE),
        DROP_TABLE(DStarLType.DDL_FUNCTIONS_WRITE),
        DROP_SCHEMA(DStarLType.DDL_FUNCTIONS_WRITE),
        DROP_GROUP(DStarLType.DDL_FUNCTIONS_WRITE),
        GET_AIS(DStarLType.DDL_FUNCTIONS_READ),
        GET_TABLE_ID(DStarLType.DDL_FUNCTIONS_READ),
        GET_TABLE_BY_ID(DStarLType.DDL_FUNCTIONS_READ),
        GET_TABLE_BY_NAME(DStarLType.DDL_FUNCTIONS_READ),
        GET_USER_TABLE_BY_NAME(DStarLType.DDL_FUNCTIONS_READ),
        GET_USER_TABLE_BY_ID(DStarLType.DDL_FUNCTIONS_READ),
        GET_ROWDEF(DStarLType.DDL_FUNCTIONS_READ),
        GET_DDLS(DStarLType.DDL_FUNCTIONS_READ),
        GET_SCHEMA_ID(DStarLType.DDL_FUNCTIONS_READ),
        FORCE_GENERATION_UPDATE(DStarLType.DDL_FUNCTIONS_WRITE),
        CREATE_INDEXES(DStarLType.DDL_FUNCTIONS_WRITE),
        DROP_INDEXES(DStarLType.DDL_FUNCTIONS_WRITE),

        GET_TABLE_STATISTICS(DStarLType.DML_FUNCTIONS),
        OPEN_CURSOR(DStarLType.DML_FUNCTIONS),
        GET_CURSOR_STATE(DStarLType.DML_FUNCTIONS),
        SCAN_SOME(DStarLType.DML_FUNCTIONS),
        CLOSE_CURSOR(DStarLType.DML_FUNCTIONS),
        GET_CURSORS(DStarLType.DML_FUNCTIONS),
        CONVERT_NEW_ROW(DStarLType.DML_FUNCTIONS),
        CONVERT_ROW_DATA(DStarLType.DML_FUNCTIONS),
        CONVERT_ROW_DATAS(DStarLType.DML_FUNCTIONS),
        WRITE_ROW(DStarLType.DML_FUNCTIONS),
        DELETE_ROW(DStarLType.DML_FUNCTIONS),
        UPDATE_ROW(DStarLType.DML_FUNCTIONS),
        TRUNCATE_TABLE(DStarLType.DML_FUNCTIONS),
        ;

        private final DStarLType type;

        DDLFunction(DStarLType type) {
            this.type = type;
        }

        public DStarLType getType() {
            return type;
        }
    }
    void hookFunctionIn(Session session, DDLFunction function);
    void hookFunctionCatch(Session session, DDLFunction function, Throwable throwable);
    void hookFunctionFinally(Session session, DDLFunction function);
}

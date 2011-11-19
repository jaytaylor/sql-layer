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

package com.akiban.server.service.dxl;

import com.akiban.server.service.session.Session;

public interface DXLFunctionsHook {
    static enum DXLType {
        DDL_FUNCTIONS_WRITE,
        DDL_FUNCTIONS_READ,
        DML_FUNCTIONS_WRITE,
        DML_FUNCTIONS_READ
    }

    static enum DXLFunction {
        CREATE_TABLE(DXLType.DDL_FUNCTIONS_WRITE),
        RENAME_TABLE(DXLType.DDL_FUNCTIONS_WRITE),
        DROP_TABLE(DXLType.DDL_FUNCTIONS_WRITE),
        DROP_SCHEMA(DXLType.DDL_FUNCTIONS_WRITE),
        DROP_GROUP(DXLType.DDL_FUNCTIONS_WRITE),
        GET_AIS(DXLType.DDL_FUNCTIONS_READ),
        GET_TABLE_ID(DXLType.DDL_FUNCTIONS_READ),
        GET_TABLE_BY_ID(DXLType.DDL_FUNCTIONS_READ),
        GET_TABLE_BY_NAME(DXLType.DDL_FUNCTIONS_READ),
        GET_USER_TABLE_BY_NAME(DXLType.DDL_FUNCTIONS_READ),
        GET_USER_TABLE_BY_ID(DXLType.DDL_FUNCTIONS_READ),
        GET_ROWDEF(DXLType.DDL_FUNCTIONS_READ),
        GET_DDLS(DXLType.DDL_FUNCTIONS_READ),
        GET_SCHEMA_ID(DXLType.DDL_FUNCTIONS_READ),
        FORCE_GENERATION_UPDATE(DXLType.DDL_FUNCTIONS_WRITE),
        CREATE_INDEXES(DXLType.DDL_FUNCTIONS_WRITE),
        DROP_INDEXES(DXLType.DDL_FUNCTIONS_WRITE),

        GET_TABLE_STATISTICS(DXLType.DML_FUNCTIONS_READ),
        OPEN_CURSOR(DXLType.DML_FUNCTIONS_READ),
        GET_CURSOR_STATE(DXLType.DML_FUNCTIONS_READ),
        SCAN_SOME(DXLType.DML_FUNCTIONS_READ),
        CLOSE_CURSOR(DXLType.DML_FUNCTIONS_READ),
        GET_CURSORS(DXLType.DML_FUNCTIONS_READ),
        CONVERT_NEW_ROW(DXLType.DML_FUNCTIONS_READ),
        CONVERT_ROW_DATA(DXLType.DML_FUNCTIONS_READ),
        CONVERT_ROW_DATAS(DXLType.DML_FUNCTIONS_READ),
        WRITE_ROW(DXLType.DML_FUNCTIONS_WRITE),
        DELETE_ROW(DXLType.DML_FUNCTIONS_WRITE),
        UPDATE_ROW(DXLType.DML_FUNCTIONS_WRITE),
        TRUNCATE_TABLE(DXLType.DML_FUNCTIONS_WRITE),
        UPDATE_TABLE_STATISTICS(DXLType.DML_FUNCTIONS_WRITE),
        
        // For use by Postgres
        UNSPECIFIED_DDL_WRITE(DXLType.DDL_FUNCTIONS_WRITE),
        UNSPECIFIED_DDL_READ(DXLType.DDL_FUNCTIONS_READ),
        UNSPECIFIED_DML_WRITE(DXLType.DML_FUNCTIONS_WRITE),
        UNSPECIFIED_DML_READ(DXLType.DML_FUNCTIONS_READ),
        ;

        private final DXLType type;

        DXLFunction(DXLType type) {
            this.type = type;
        }

        public DXLType getType() {
            return type;
        }
    }
    void hookFunctionIn(Session session, DXLFunction function);
    void hookFunctionCatch(Session session, DXLFunction function, Throwable throwable);
    void hookFunctionFinally(Session session, DXLFunction function, Throwable throwable);
}

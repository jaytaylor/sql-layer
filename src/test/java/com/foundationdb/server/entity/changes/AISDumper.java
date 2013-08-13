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

package com.foundationdb.server.entity.changes;

import com.foundationdb.ais.AISCloner;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.ais.protobuf.ProtobufWriter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.foundationdb.ais.protobuf.ProtobufWriter.WriteSelector;
import static com.foundationdb.ais.protobuf.ProtobufWriter.SingleSchemaSelector;

public class AISDumper {
    public static String dumpDeterministicAIS(AkibanInformationSchema ais, String schema) {
        WriteSelector selector = new SingleSchemaSelector(schema);
        AkibanInformationSchema clone = AISCloner.clone(ais, selector);
        Map<Sequence,Column> identityColumns = new HashMap<>();
        for(UserTable table : clone.getUserTables().values()) {
            table.setTableId(-1);
            table.setOrdinal(null);
            table.setVersion(null);
            for(Column column : table.getColumnsIncludingInternal()) {
                column.clearMaxAndPrefixSize();
                if(column.getName().endsWith("_ref") || column.getName().endsWith("_ref$1")) {
                    column.setUuid(null);
                }
                if(column.getIdentityGenerator() != null) {
                    identityColumns.put(column.getIdentityGenerator(), column);
                }
            }
            for(Index index : table.getIndexesIncludingInternal()) {
                index.setIndexId(-1);
                index.setTreeName(null);
            }
        }
        for(Group group : clone.getGroups().values()) {
            group.setTreeName(null);
            for(Index index : group.getIndexes()) {
                index.setIndexId(-1);
                index.setTreeName(null);
            }
        }
        // Get rid of generated tree and sequence names
        List<Sequence> sequences = new ArrayList<>(clone.getSequences().values());
        for(int i = 0; i < sequences.size(); ++i) {
            Sequence s = sequences.get(i);
            clone.removeSequence(s.getSequenceName());
            Sequence newSeq = Sequence.create(clone, s.getSchemaName(), "_sequence-" + i,
                                              s.getStartsWith(), s.getIncrement(), s.getMinValue(), s.getMaxValue(), s.isCycle());
            Column identity = identityColumns.get(s);
            if(identity != null) {
                identity.setIdentityGenerator(newSeq);
            }
        }
        return new ProtobufWriter(selector).save(clone).toString();
    }
}

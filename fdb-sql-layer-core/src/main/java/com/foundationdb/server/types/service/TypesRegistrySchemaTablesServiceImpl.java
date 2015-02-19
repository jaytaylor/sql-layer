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
package com.foundationdb.server.types.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.ais.model.aisb2.NewTableBuilder;
import com.foundationdb.qp.virtualadapter.BasicFactoryBase;
import com.foundationdb.qp.virtualadapter.VirtualAdapter;
import com.foundationdb.qp.virtualadapter.VirtualGroupCursor;
import com.foundationdb.qp.virtualadapter.SimpleVirtualGroupScan;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.is.SchemaTablesService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.texpressions.TValidatedOverload;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;

public class TypesRegistrySchemaTablesServiceImpl
extends SchemaTablesService
implements Service, TypesRegistrySchemaTablesService {
    
    private final TypesRegistryService typesRegistry;
    public static final TableName AK_OVERLOADS = TableName.create(TableName.INFORMATION_SCHEMA, "ak_overloads");
    public static final TableName AK_CASTS = TableName.create(TableName.INFORMATION_SCHEMA, "ak_casts");
    @Inject
    public TypesRegistrySchemaTablesServiceImpl(SchemaManager schemaManager, 
            TypesRegistryService typesRegistry) {
        super(schemaManager);
        this.typesRegistry = typesRegistry;
    }

    @Override
    public void start() {
        OverloadsTableFactory overloadsFactory = new OverloadsTableFactory(AK_OVERLOADS);
        attach(overloadsFactory.table(schemaManager.getTypesTranslator()),  overloadsFactory);
        
        CastsTableFactory castsFactory = new CastsTableFactory(AK_CASTS);
        attach (castsFactory.table(schemaManager.getTypesTranslator()), castsFactory);
    }

    @Override
    public void stop() {
        // Nothing
    }

    @Override
    public void crash() {
        // Nothing
    }
    
    public TypesRegistryService getTypeRegistry() {
        return typesRegistry;
    }
    
    private abstract class MemTableBase extends BasicFactoryBase {

        protected abstract void buildTable(NewTableBuilder builder);

        public Table table(TypesTranslator typesTranslator) {
            NewAISBuilder builder = AISBBasedBuilder.create(typesTranslator);
            buildTable(builder.table(getName()));
            return builder.ais(false).getTable(getName());
        }

        protected MemTableBase(TableName tableName) {
            super(tableName);
        }
    }

    private class CastsTableFactory extends MemTableBase {

        @Override
        protected void buildTable(NewTableBuilder builder) {
            builder.colString("source_bundle", DESCRIPTOR_MAX, false)
                    .colString("source_type", DESCRIPTOR_MAX, false)
                    .colString("target_bundle", DESCRIPTOR_MAX, false)
                    .colString("target_type", DESCRIPTOR_MAX, false)
                    .colString("is_strong", YES_NO_MAX, false)
                    .colString("is_derived", YES_NO_MAX, false);
        }

        @Override
        public VirtualGroupCursor.GroupScan getGroupScan(VirtualAdapter adapter, Group group) {
            Collection<Map<TClass, TCast>> castsBySource = getTypeRegistry().getCastsResolver().castsBySource();
            Collection<TCast> castsCollections = new ArrayList<>(castsBySource.size());
            for (Map<?, TCast> castMap : castsBySource) {
                castsCollections.addAll(castMap.values());
            }
            
            return new SimpleVirtualGroupScan<TCast>(group.getAIS(), getName(), castsCollections.iterator()) {
                @Override
                protected Object[] createRow(TCast data, int hiddenPk) {
                    return new Object[] {
                            data.sourceClass().name().bundleId().name(),
                            data.sourceClass().name().unqualifiedName(),
                            data.targetClass().name().bundleId().name(),
                            data.targetClass().name().unqualifiedName(),
                            boolResult(getTypeRegistry().getCastsResolver().isStrong(data)),
                            boolResult(data instanceof TCastsRegistry.ChainedCast),
                            hiddenPk
                    };
                }
            };
        }

        @Override
        public long rowCount(Session session) {
            long count = 0;
            for (Map<?,?> castsBySource : getTypeRegistry().getCastsResolver().castsBySource()) {
                count += castsBySource.size();
            }
            return count;
        }

        private CastsTableFactory(TableName tableName) {
            super(tableName);
        }
    }

    private class OverloadsTableFactory extends MemTableBase {

        @Override
        protected void buildTable(NewTableBuilder builder) {
            builder.colString("name", IDENT_MAX, false)
                   .colBigInt("priority_order", false)
                   .colString("inputs", IDENT_MAX * 2, false)
                   .colString("output", IDENT_MAX * 2, false)
                   .colString("internal_impl", IDENT_MAX * 2, false);
        }

        @Override
        public VirtualGroupCursor.GroupScan getGroupScan(VirtualAdapter adapter, Group group) {
            Iterator<? extends TValidatedOverload> allOverloads = Iterators.concat(
                    getTypeRegistry().getScalarsResolver().getRegistry().iterator(),
                    getTypeRegistry().getAggregatesResolver().getRegistry().iterator());
            return new SimpleVirtualGroupScan<TValidatedOverload>(group.getAIS(), getName(), allOverloads) {
                @Override
                protected Object[] createRow(TValidatedOverload data, int hiddenPk) {
                    return new Object[] {
                            data.displayName().toLowerCase(),
                            lowest(data.getPriorities()),
                            data.describeInputs(),
                            data.resultStrategy().toString(true),
                            data.id(),
                            hiddenPk
                    };
                }
            };
        }

        @Override
        public long rowCount(Session session) {
            return countOverloads(getTypeRegistry().getAggregatesResolver().getRegistry()) 
                    + countOverloads(getTypeRegistry().getScalarsResolver().getRegistry());
        }

        private long countOverloads(ResolvablesRegistry<?> registry) {
            long count = 0;
            for (ScalarsGroup<?> group : registry.allScalarsGroups()) {
                count += group.getOverloads().size();
            }
            return count;
        }

        public OverloadsTableFactory(TableName tableName) {
            super(tableName);
        }
    }

    private static int lowest(int[] ints) {
        int result = ints[0];
        for (int i = 1; i < ints.length; ++i)
            result = Math.min(result, ints[i]);
        return result;
    }
    
}

package com.akiban.server.api.hapi;

import com.akiban.ais.model.TableName;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiPredicate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DefaultHapiGetRequest implements HapiGetRequest {

    private final TableName predicatesTable;
    private final String rootTable;
    private final List<HapiPredicate> predicates;

    public static HapiGetRequestBuilder forTables(String schema, String rootTable, String predicateTable) {
        return new HapiGetRequestBuilder(schema, rootTable, predicateTable);
    }

    DefaultHapiGetRequest(String rootTable, TableName predicatesTable, List<HapiPredicate> predicates) {
        this.predicatesTable = predicatesTable;
        this.rootTable = rootTable;
        this.predicates = Collections.unmodifiableList(new ArrayList<HapiPredicate>(predicates));
    }

    DefaultHapiGetRequest(String rootTable, TableName predicatesTable, HapiPredicate predicate) {
        this.predicatesTable = predicatesTable;
        this.rootTable = rootTable;
        List<HapiPredicate> tmp = new ArrayList<HapiPredicate>(1);
        tmp.add(predicate);
        this.predicates = Collections.unmodifiableList(tmp);
    }

    @Override
    public String getSchema() {
        return predicatesTable.getSchemaName();
    }

    @Override
    public String getTable() {
        return rootTable;
    }

    @Override
    public TableName getUsingTable() {
        return predicatesTable;
    }

    @Override
    public List<HapiPredicate> getPredicates() {
        return predicates;
    }

    @Override
    public String toString() {
        return HapiUtils.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof HapiGetRequest && HapiUtils.equals(this, (HapiGetRequest) o);
    }

    @Override
    public int hashCode() {
        return HapiUtils.hashCode(this);
    }
}

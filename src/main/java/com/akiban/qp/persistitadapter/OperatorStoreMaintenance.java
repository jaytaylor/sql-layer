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

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.expression.RowBasedUnboundExpressions;
import com.akiban.qp.expression.UnboundExpressions;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.ArrayBindings;
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.ConstantColumnSelector;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.VariableExpression;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDataValueSource;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.conversion.Converters;
import com.akiban.util.Tap;

import java.util.*;

final class OperatorStoreMaintenance {

    public void run(OperatorStoreGIHandler.Action action, PersistitHKey hKey, RowData forRow, StoreAdapter adapter, OperatorStoreGIHandler handler) {
        ALL_TAP.in();
        Operator planOperator = rootOperator(action);
        if (planOperator == null)
            return;
        Bindings bindings = new ArrayBindings(1);
        final List<Column> lookupCols;
        UserTable userTable = rowType.userTable();
        if (usePKs(action)) {
            // use PKs
            lookupCols = userTable.getPrimaryKey().getColumns();
        }
        else {
            // use FKs
            lookupCols = new ArrayList<Column>();
            for (JoinColumn joinColumn : userTable.getParentJoin().getJoinColumns()) {
                lookupCols.add(joinColumn.getChild());
            }
        }

        bindings.set(OperatorStoreMaintenance.HKEY_BINDING_POSITION, hKey);

        // Copy the values into the array bindings
        ToObjectValueTarget target = new ToObjectValueTarget();
        RowDataValueSource source = new RowDataValueSource();
        for (int i=0; i < lookupCols.size(); ++i) {
            int bindingsIndex = i+1;
            Column col = lookupCols.get(i);
            source.bind((FieldDef)col.getFieldDef(), forRow);
            target.expectType(col.getType().akType());
            bindings.set(bindingsIndex, Converters.convert(source, target).lastConvertedValue());
        }

        Cursor cursor = API.cursor(planOperator, adapter);
        RUN_TAP.in();
        cursor.open(bindings);
        try {
            Row row;
            while ((row = cursor.next()) != null) {
                if (row.rowType().equals(planOperator.rowType())) {
                    Tap.InOutTap actionTap = actionTap(action);
                    actionTap.in();
                    handler.handleRow(groupIndex, row, action);
                    actionTap.out();
                }
            }
        } finally {
            cursor.close();
            RUN_TAP.out();
        }
        ALL_TAP.out();
    }

    private Operator rootOperator(OperatorStoreGIHandler.Action action) {
        switch (action) {
        case STORE:
            return storePlan;
        case DELETE:
            return deletePlan;
        default: throw new AssertionError(action.name());
        }
    }

    private boolean usePKs(OperatorStoreGIHandler.Action action) {
        switch (action) {
        case STORE:
            return usePksStore;
        case DELETE:
            return usePksDelete;
        default: throw new AssertionError(action.name());
        }
    }

    private Tap.InOutTap actionTap(OperatorStoreGIHandler.Action action) {
        if (action == null)
            return OTHER_TAP;
        switch (action) {
            case STORE:     return STORE_TAP;
            case DELETE:    return DELETE_TAP;
            default:        return OTHER_TAP;
        }
    }

    public OperatorStoreMaintenance(BranchTables branchTables,
                                    GroupIndex groupIndex,
                                    UserTableRowType rowType)
    {
        PlanCreationStruct storePlan = createGroupIndexMaintenancePlan(branchTables, groupIndex, rowType, true);
        this.storePlan = storePlan.rootOperator;
        this.usePksStore = storePlan.usePKs;
        PlanCreationStruct deletePlan = createGroupIndexMaintenancePlan(branchTables, groupIndex, rowType, false);
        this.deletePlan = deletePlan.rootOperator;
        this.usePksDelete = deletePlan.usePKs;

        this.rowType = rowType;
        this.groupIndex = groupIndex;
    }

    private final Operator storePlan;
    private final Operator deletePlan;
    private final GroupIndex groupIndex;
    private final boolean usePksStore;
    private final boolean usePksDelete;
    private final UserTableRowType rowType;

    enum Relationship {
        PARENT,
        SELF, CHILD
    }

    // for use in this class

    /**
     * <p>Creates a struct that describes how GI maintenance works for a row of a given type. This struct contains
     * two elements: a parameterized SELECT plan, and a boolean that specifies whether those parameters come from
     * the row's PK or its grouping FK.</p>
     *
     * <p>Any action has three steps: GI cleanup, action, and GI (re)write. For instance, in an INSERT of an order when
     * there's a C-O left join GI, we would have to:
     * <ol>
     *     <li><b>cleanup:</b> if this is the first order under this customer, delete the GI entry with NULL'ed out
     *     order info</li>
     *     <li><b>action:</b> do the insert, including maintenance of any table indexes</li>
     *     <li><b>(re)write:</b> write the new GI entry</li>
     * </ol>
     * </p>
     *
     * <p>In a perfect world, we would only clean up those entries which we knew would be removed by the end, and we
     * would only write those entries which we knew didn't exist before. Instead, we'll be deleting some rows that
     * we'll soon need to re-write (which is why that third step isn't just called "write"). The level to which we can
     * cut down on superfluous deletes and stores will have performance repercussions and is a potential point
     * of optimization.</p>
     *
     * <p>For now, here's how it works. We start a scan from some table+index (more on this below), then do a deep
     * branch lookup to the group table, then flatten all of the rows. The table we start from is either the incoming
     * row's table, its parent or its child.
     *
     * <ul>
     *     <li>if incoming row is <strong>below the GI</strong>, ignore this event</sup></li>
     *     <li>if incoming row is <strong>above the GI (is an ancestor of the GI's rootmost table),</strong> always
     *     look childward</li>
     *     <li>if the incoming row is <strong>within the GI</strong>
     *      <ul>
     *          <li>in a <strong>LEFT JOIN</strong>, look parentward, capped at the GI's rootmost table</li>
     *          <li>in a <strong>RIGHT JOIN</strong>, look childward, capped at the GI's leafmost table</li>
     *      </ul>
     *     </li>
     * </ul>
     * </p>
     *
     * <p>Motivating reasons:</p>
     *
     * <p>First, it's worth calling out everything an action may do to a GI. The actions apply only to INSERT and
     * DELETE actions; UPDATE is treated as DELETE+INSERT.
     * <ul>
     *     <li>add an entry</li>
     *     <li>... and possibly remove an outer GI entry in the process (an outer entry is one that represents the result
     *     of an outer join that would not have been present in an inner join -- that is, a GI for which one of the rows
     *     doesn't actually exist)</li>
     *     <li>remove an entry</li>
     *     <li>... and possibly create an outer GI entry in the process</li>
     *     <li>update 1 or more rows' HKey segments (via adoption or orphaning)</li>
     * </ul>
     * </p>
     *
     * <p>With that in mind, here's an informal look at the motivation behind each of the above scenarios.</p>
     *
     * <h3>Incoming row is below the GI</h3>
     *
     * <p>Such a row can't affect the GI's HKey, since adoption and orphaning can only happen childward of a given row.
     * It also can't add or remove an entry, since entries are agnostic to tables outside of the GI segment (other
     * than for hkey maintenance). So this is a no-op in all situations.</p>
     *
     * <h3>Incoming row is above the GI</h3>
     *
     * <p>Such an action can only affect HKey maintenance. For an insert's cleanup, we have to start the scan at the
     * incoming row's child, since the incoming row itself doesn't exist (if the child doesn't exist either, there's no
     * HKey maintenance to do that will trickle up to the GI). For a delete's (re)write, we also have to start the
     * scan at the newly-deleted row's child, since that row doesn't exist anymore. For the other two situations
     * (insert's (re)write phase or delete's cleanup), we could start on the incoming row's table if we want, but
     * because of RIGHT JOIN semantics there's no harm in starting on the child. In fact, it's a bit more selective
     * and thus efficient.
     *
     * <h4>Incoming row is within the GI</h4>
     *
     * <p>Such an action can:<ul>
     *     <li>insert a GI entry, possibly removing an outer GI entry</li>
     *     <li>remove a GI entry, possibly inserting an outer GI entry</li>
     * </ul>
     * </p>
     *
     * <p>For <strong>LEFT JOIN GIs</strong>, we need to start at the incoming row's parent to clean up or insert any
     * outer GIs (as part of an insert's cleanup or remove's (re)write, respectivley). On the other hand, if the
     * incoming row is the rootmost table in the GI, then going to its parent isn't necessary for outer GI maintenance
     * (there's nothing to do, since there can't be an outer GI), and it's wrong for regular GI maintenance, since it
     * means that if hat parent row doesn't exist, we won't get any GI entries even though we should have at least one.
     * Note that this problem doesn't hold true for tables further down the GI branch: if their parent doesn't exist,
     * then there's nothing to do because of LEFT JOIN semantics, and starting the scan at the parent is fine (the scan
     * will correctly return no rows). So we have the following situations:</p>
     *
     * <p>Putting all that together, we want to go to the incoming row's parent, unless the incoming row is of the
     * rootmost table in the GI, in which we want to stay on that table. So, we want to go parentward but capped at
     * the GI border.</p>
     *
     * <p>For </strong>RIGHT JOIN GIs</strong>, the situation is similar but reversed. Outer GI entries come from the
     * child, so we need to look childward in order to maintain them. On the other hand, we can't look past the GI
     * border, because if we did we'd be missing rows in the case of the leafmost table of a GI not having any children.
     * So, we should be scanning starting from the child, but again capped at the GI.</p>
     *
     * @param branchTables 
     * @param groupIndex
     * @param rowType
     * @param forStoring
     * @return
     */
    private static PlanCreationStruct createGroupIndexMaintenancePlan(
            BranchTables branchTables,
            GroupIndex groupIndex,
            UserTableRowType rowType,
            boolean forStoring)
    {
        if (branchTables.isEmpty()) {
            throw new RuntimeException("group index has empty branch: " + groupIndex);
        }
        if (!branchTables.fromRoot().contains(rowType)) {
            throw new RuntimeException(rowType + " not in branch for " + groupIndex + ": " + branchTables);
        }

        PlanCreationStruct result = new PlanCreationStruct();
        // compute if this is a no-op
        if (!forStoring && rowType.userTable().getDepth() == 0 && rowType != branchTables.rootMost()) {
            return result;
        }

        Schema schema = rowType.schema();

        final Relationship scanFrom;
        // this is a cleanup scan
        int index = branchTables.fromRootMost().indexOf(rowType);
        if (index < 0) {
            // incoming row is above the GI, so look downward
            scanFrom = Relationship.CHILD;
        }
        else if (groupIndex.getJoinType() == Index.JoinType.LEFT) {
            int indexWithinGI = branchTables.fromRootMost().indexOf(rowType) - 1;
            scanFrom = indexWithinGI < 0
                    ? Relationship.SELF
                    : Relationship.PARENT;
        }
        else if (groupIndex.getJoinType() == Index.JoinType.RIGHT) {
            int indexWithinGI = branchTables.fromRootMost().indexOf(rowType) + 1;
            scanFrom = indexWithinGI < branchTables.fromRootMost().size()
                    ? Relationship.CHILD
                    : Relationship.SELF;
        }
        else {
            throw new AssertionError(groupIndex + " has join " + groupIndex.getJoinType());
        }


        final UserTableRowType startFromRowType;
        TableIndex startFromIndex;
        final boolean usePKs;

        switch (scanFrom) {
        case PARENT:
            usePKs = false;
            startFromRowType = schema.userTableRowType(rowType.userTable().parentTable());
            startFromIndex = startFromRowType.userTable().getPrimaryKey().getIndex();
            break;
        case SELF:
            usePKs = true;
            startFromRowType = rowType;
            startFromIndex = startFromRowType.userTable().getPrimaryKey().getIndex();
            break;
        case CHILD:
            usePKs = true;
            int idIndex = branchTables.fromRoot().indexOf(rowType);
            startFromRowType = branchTables.fromRoot().get(idIndex+1);
            startFromIndex = fkIndex(startFromRowType.userTable(), rowType.userTable());
            break;
        default:
            throw new AssertionError(scanFrom.name());
        }

        result.usePKs = usePKs;
        UserTable startFrom = startFromRowType.userTable();
        List<Expression> pkExpressions = new ArrayList<Expression>(startFromIndex.getColumns().size());
        for (int i=0; i < startFrom.getPrimaryKey().getColumns().size(); ++i) {
            int fieldPos = i + 1; // position 0 is already claimed in OperatorStore
            Column col = startFrom.getPrimaryKey().getColumns().get(i);
            Expression pkField = new VariableExpression(col.getType().akType(), fieldPos);
            pkExpressions.add(pkField);
        }
        UnboundExpressions unboundExpressions = new RowBasedUnboundExpressions(startFromRowType, pkExpressions);
        IndexBound bound = new IndexBound(unboundExpressions, ConstantColumnSelector.ALL_ON);
        IndexRowType startFromIndexRowType = schema.indexRowType(startFromIndex);
        IndexKeyRange range = IndexKeyRange.bounded(startFromIndexRowType, bound, true, bound, true);

        // the scan, main table and flattens

        Operator plan;
        plan = API.indexScan_Default(startFromIndexRowType, false, range);
        plan = API.branchLookup_Default(
                plan,
                startFrom.getGroup().getGroupTable(),
                startFromIndexRowType,
                branchTables.allTablesForBranch.get(0),
                API.LookupOption.DISCARD_INPUT
        );

        // RIGHT JOIN until the GI, and then the GI's join types

        RowType parentRowType = null;
        API.JoinType joinType = API.JoinType.RIGHT_JOIN;
        int branchStartDepth = branchTables.rootMost().userTable().getDepth() - 1;
        boolean withinBranch = branchStartDepth == -1;
        API.JoinType withinBranchJoin = operatorJoinType(groupIndex);
        for (UserTableRowType branchRowType : branchTables.fromRoot()) {
            if (parentRowType == null) {
                parentRowType = branchRowType;
            }
            else {
                plan = API.flatten_HKeyOrdered(plan, parentRowType, branchRowType, joinType);
                parentRowType = plan.rowType();
            }
            if (branchRowType.userTable().getDepth() == branchStartDepth) {
                withinBranch = true;
            } else if (withinBranch) {
                joinType = withinBranchJoin;
            }
        }
        result.rootOperator = plan;
        return result;
    }

    private static API.JoinType operatorJoinType(Index index) {
        switch (index.getJoinType()) {
        case LEFT:
            return API.JoinType.LEFT_JOIN;
        case RIGHT:
            return API.JoinType.RIGHT_JOIN;
        default:
            throw new AssertionError(index.getJoinType().name());
        }
    }

    /**
     * Given a table with a FK column and a table with a PK column, gets the index from the FK table that
     * connects to the PK table.
     * @param fkTable the child table
     * @param pkTable the 
     * @return the fkTable's FK index
     */
    private static TableIndex fkIndex(UserTable fkTable, UserTable pkTable) {
        if (fkTable.getParentJoin() == null || fkTable.getParentJoin().getParent() != pkTable)
            throw new IllegalArgumentException(pkTable + " is not a parent of " + fkTable);
        List<Column> fkIndexCols = new ArrayList<Column>();
        for(JoinColumn joinColumn : fkTable.getParentJoin().getJoinColumns()) {
            fkIndexCols.add(joinColumn.getChild());
        }
        List<Column> candidateIndexColumns = new ArrayList<Column>();
        for (TableIndex index : fkTable.getIndexes()) {
            for (IndexColumn indexCol : index.getColumns()) {
                candidateIndexColumns.add(indexCol.getColumn());
            }
            if (candidateIndexColumns.equals(fkIndexCols)) {
                return index;
            }
            candidateIndexColumns.clear();
        }
        throw new AssertionError("no index found for columns: " + fkIndexCols);
    }

    // package consts

    private static final int HKEY_BINDING_POSITION = 0;
    private static final Tap.InOutTap ALL_TAP = Tap.createTimer("GI maintenance: all");
    private static final Tap.InOutTap RUN_TAP = Tap.createTimer("GI maintenance: run");
    private static final Tap.InOutTap STORE_TAP = Tap.createTimer("GI maintenance: STORE");
    private static final Tap.InOutTap DELETE_TAP = Tap.createTimer("GI maintenance: DELETE");
    private static final Tap.InOutTap OTHER_TAP = Tap.createTimer("GI maintenance: OTHER");

    // nested classes

    static class BranchTables {

        // BranchTables interface

        public List<UserTableRowType> fromRoot() {
            return allTablesForBranch;
        }

        public List<UserTableRowType> fromRootMost() {
            return onlyBranch;
        }

        public boolean isEmpty() {
            return fromRootMost().isEmpty();
        }

        public UserTableRowType rootMost() {
            return onlyBranch.get(0);
        }

        public BranchTables(Schema schema, GroupIndex groupIndex) {
            List<UserTableRowType> localTables = new ArrayList<UserTableRowType>();
            UserTable rootmost = groupIndex.rootMostTable();
            int branchRootmostIndex = -1;
            for (UserTable table = groupIndex.leafMostTable(); table != null; table = table.parentTable()) {
                if (table.equals(rootmost)) {
                    assert branchRootmostIndex == -1 : branchRootmostIndex;
                    branchRootmostIndex = table.getDepth();
                }
                localTables.add(schema.userTableRowType(table));
            }
            if (branchRootmostIndex < 0) {
                throw new RuntimeException("branch root not found! " + rootmost + " within " + localTables);
            }
            Collections.reverse(localTables);
            this.allTablesForBranch = Collections.unmodifiableList(localTables);
            this.onlyBranch = branchRootmostIndex == 0
                    ? allTablesForBranch
                    : allTablesForBranch.subList(branchRootmostIndex, allTablesForBranch.size());
        }

        // object state
        private final List<UserTableRowType> allTablesForBranch;
        private final List<UserTableRowType> onlyBranch;
    }

    static class PlanCreationStruct {
        public Operator rootOperator;
        public boolean usePKs;
    }
}

/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.test.it.qp;

import com.akiban.ais.model.Group;
import com.akiban.qp.operator.ExpressionGenerator;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.CaseConvertExpression;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.types3.Types3Switch;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.akiban.qp.operator.API.*;
import static com.akiban.server.test.ExpressionGenerators.*;

// Inspired by bug 1026668, but also tests various ways of enforcing distinctness, in addition to Distinct_Partial.

public class Distinct_Partial_CaseInsensitive_IT extends OperatorITBase
{
    @Before
    public void before()
    {
        t = createTable(
            "s", "t",
            "id int not null",
            "cs varchar(10)", // case sensitive
            "ci varchar(10) collate latin1_swedish_ci", // case insensitive
            "ns int"); // non-string
        schema = new Schema(ais());
        tRowType = schema.userTableRowType(userTable(t));
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        group = group(t);
        caseSensitiveCollator = tRowType.userTable().getColumn("cs").getCollator();
        caseInsensitiveCollator = tRowType.userTable().getColumn("ci").getCollator();
        db = new NewRow[]{
            createNewRow(t, 0L, "aa_cs", "aa_ci", 0),
            createNewRow(t, 1L, "bb_cs", "bb_ci", 0),
            createNewRow(t, 2L, "aA_cs", "aA_ci", 0),
            createNewRow(t, 3L, "bB_cs", "bB_ci", 0),
            createNewRow(t, 4L, "Aa_cs", "Aa_ci", 0),
            createNewRow(t, 5L, "Bb_cs", "Bb_ci", 0),
            createNewRow(t, 6L, "AA_cs", "AA_ci", 0),
            createNewRow(t, 7L, "BB_cs", "BB_ci", 0),
            // make sure that all columns have to be examined
            createNewRow(t, 7L, "x", "x", 0),
            createNewRow(t, 8L, "x", "x", 0),
            createNewRow(t, 9L, "x", "x", 0),
            createNewRow(t, 10L, "x", "x", 0),
        };
        use(db);
    }

    @Test
    public void testCaseSensitiveUsingSortTree()
    {
        Operator project = project_Default(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 1)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(projectRowType, 0), true, caseSensitiveCollator);
        Operator plan =
            sort_Tree(
                project,
                projectRowType,
                ordering,
                SortOption.SUPPRESS_DUPLICATES);
        RowBase[] expected = new RowBase[] {
            row(projectRowType, "AA_cs"),
            row(projectRowType, "Aa_cs"),
            row(projectRowType, "BB_cs"),
            row(projectRowType, "Bb_cs"),
            row(projectRowType, "aA_cs"),
            row(projectRowType, "aa_cs"),
            row(projectRowType, "bB_cs"),
            row(projectRowType, "bb_cs"),
            row(projectRowType, "x"),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCaseInsensitiveUsingSortTree()
    {
        Operator project = project_Default(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 2)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(project.rowType(), 0), true, caseInsensitiveCollator);
        List<ExpressionGenerator> convertToUpper = Arrays.asList(toUpper(field(projectRowType, 0)));
        Operator plan =
            project_Default(
                sort_Tree(
                    project,
                    projectRowType,
                    ordering,
                    SortOption.SUPPRESS_DUPLICATES),
                projectRowType,
                convertToUpper);
        RowBase[] expected = new RowBase[] {
            row(projectRowType, "AA_CI"),
            row(projectRowType, "BB_CI"),
            row(projectRowType, "X"),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testNonStringUsingSortTree()
    {
        Operator project = project_Default(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 3)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(project.rowType(), 0), true);
        Operator plan =
            sort_Tree(
                project,
                projectRowType,
                ordering,
                SortOption.SUPPRESS_DUPLICATES);
        RowBase[] expected = new RowBase[] {
            row(projectRowType, 0L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testMultipleColumnsUsingSortTree()
    {
        Operator project = project_Default(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 1),
                          field(tRowType, 2),
                          field(tRowType, 3)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(projectRowType, 0), true, caseSensitiveCollator);
        ordering.append(field(projectRowType, 1), true, caseInsensitiveCollator);
        ordering.append(field(projectRowType, 2), true);
        List<ExpressionGenerator> convertCaseInsensitiveToUpper =
            Arrays.asList(field(projectRowType, 0),
                          toUpper(field(projectRowType, 1)),
                          field(projectRowType, 2));
        Operator plan =
            project_Default(
                sort_Tree(
                    project,
                    projectRowType,
                    ordering,
                    SortOption.SUPPRESS_DUPLICATES),
                projectRowType,
                convertCaseInsensitiveToUpper);
        RowBase[] expected = new RowBase[] {
            row(projectRowType, "AA_cs", "AA_CI", 0),
            row(projectRowType, "Aa_cs", "AA_CI", 0),
            row(projectRowType, "BB_cs", "BB_CI", 0),
            row(projectRowType, "Bb_cs", "BB_CI", 0),
            row(projectRowType, "aA_cs", "AA_CI", 0),
            row(projectRowType, "aa_cs", "AA_CI", 0),
            row(projectRowType, "bB_cs", "BB_CI", 0),
            row(projectRowType, "bb_cs", "BB_CI", 0),
            row(projectRowType, "x", "X", 0),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCaseSensitiveUsingSortInsertionLimited()
    {
        Operator project = project_Default(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 1)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(projectRowType, 0), true, caseSensitiveCollator);
        Operator plan =
            sort_InsertionLimited(
                project,
                projectRowType,
                ordering,
                SortOption.SUPPRESS_DUPLICATES,
                db.length);
        RowBase[] expected = new RowBase[] {
            row(projectRowType, "AA_cs"),
            row(projectRowType, "Aa_cs"),
            row(projectRowType, "BB_cs"),
            row(projectRowType, "Bb_cs"),
            row(projectRowType, "aA_cs"),
            row(projectRowType, "aa_cs"),
            row(projectRowType, "bB_cs"),
            row(projectRowType, "bb_cs"),
            row(projectRowType, "x"),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCaseInsensitiveUsingSortInsertionLimited()
    {
        Operator project = project_Default(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 2)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(projectRowType, 0), true, caseInsensitiveCollator);
        List<ExpressionGenerator> convertToUpper =
            Arrays.asList(toUpper(field(projectRowType, 0)));
        Operator plan =
            project_Default(
                sort_InsertionLimited(
                    project,
                    projectRowType,
                    ordering,
                    SortOption.SUPPRESS_DUPLICATES,
                    db.length),
                projectRowType,
                convertToUpper);
        RowBase[] expected = new RowBase[] {
            row(projectRowType, "AA_CI"),
            row(projectRowType, "BB_CI"),
            row(projectRowType, "X"),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testNonStringUsingSortInsertionLimited()
    {
        Operator project = project_Default(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 3)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(project.rowType(), 0), true);
        Operator plan =
            sort_InsertionLimited(
                project,
                projectRowType,
                ordering,
                SortOption.SUPPRESS_DUPLICATES,
                db.length);
        RowBase[] expected = new RowBase[] {
            row(projectRowType, 0L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testMultipleColumnsUsingSortInsertionLimited()
    {
        Operator project = project_Default(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 1),
                          field(tRowType, 2),
                          field(tRowType, 3)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(projectRowType, 0), true, caseSensitiveCollator);
        ordering.append(field(projectRowType, 1), true, caseInsensitiveCollator);
        ordering.append(field(projectRowType, 2), true);
        List<ExpressionGenerator> convertCaseInsensitiveToUpper =
            Arrays.asList(field(projectRowType, 0),
                          toUpper(field(projectRowType, 1)),
                          field(projectRowType, 2));
        Operator plan =
            project_Default(
                sort_InsertionLimited(
                    project,
                    projectRowType,
                    ordering,
                    SortOption.SUPPRESS_DUPLICATES,
                    db.length),
                projectRowType,
                convertCaseInsensitiveToUpper);
        RowBase[] expected = new RowBase[] {
            row(projectRowType, "AA_cs", "AA_CI", 0),
            row(projectRowType, "Aa_cs", "AA_CI", 0),
            row(projectRowType, "BB_cs", "BB_CI", 0),
            row(projectRowType, "Bb_cs", "BB_CI", 0),
            row(projectRowType, "aA_cs", "AA_CI", 0),
            row(projectRowType, "aa_cs", "AA_CI", 0),
            row(projectRowType, "bB_cs", "BB_CI", 0),
            row(projectRowType, "bb_cs", "BB_CI", 0),
            row(projectRowType, "x", "X", 0),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCaseSensitiveUsingDistinctPartial()
    {
        Operator project = project_Default(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 1)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(projectRowType, 0), true, caseSensitiveCollator);
        // Sort, preserving duplicates, so that we can test Distinct_Partial.
        Operator plan =
            distinct_Partial(
                sort_InsertionLimited(
                    project,
                    projectRowType,
                    ordering,
                    SortOption.PRESERVE_DUPLICATES,
                    db.length),
                projectRowType,
                Arrays.asList(caseSensitiveCollator),
                Types3Switch.ON);
        RowBase[] expected = new RowBase[] {
            row(projectRowType, "AA_cs"),
            row(projectRowType, "Aa_cs"),
            row(projectRowType, "BB_cs"),
            row(projectRowType, "Bb_cs"),
            row(projectRowType, "aA_cs"),
            row(projectRowType, "aa_cs"),
            row(projectRowType, "bB_cs"),
            row(projectRowType, "bb_cs"),
            row(projectRowType, "x"),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCaseInsensitiveUsingDistinctPartial()
    {
        Operator project = project_Default(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 2)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(projectRowType, 0), true, caseInsensitiveCollator);
        List<ExpressionGenerator> convertToUpper = Arrays.asList(toUpper(field(projectRowType, 0)));
        // Sort, preserving duplicates, so that we can test Distinct_Partial.
        Operator plan =
            project_Default(
                distinct_Partial(
                    sort_InsertionLimited(
                        project,
                        projectRowType,
                        ordering,
                        SortOption.PRESERVE_DUPLICATES,
                        db.length),
                    projectRowType,
                    Arrays.asList(caseInsensitiveCollator),
                    Types3Switch.ON),
                projectRowType,
                convertToUpper);
        RowBase[] expected = new RowBase[] {
            row(projectRowType, "AA_CI"),
            row(projectRowType, "BB_CI"),
            row(projectRowType, "X"),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testNonStringUsingDistinctPartial()
    {
        Operator project = project_Default(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 3)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(project.rowType(), 0), true);
        Operator plan =
            distinct_Partial(
                sort_InsertionLimited(
                    project,
                    projectRowType,
                    ordering,
                    SortOption.PRESERVE_DUPLICATES,
                    db.length),
                projectRowType,
                Arrays.asList((AkCollator)null),
                Types3Switch.ON);
        RowBase[] expected = new RowBase[] {
            row(projectRowType, 0L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testMultipleColumnsUsingDistinctPartial()
    {
        Operator project = project_Default(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 1),
                          field(tRowType, 2),
                          field(tRowType, 3)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(projectRowType, 0), true, caseSensitiveCollator);
        ordering.append(field(projectRowType, 1), true, caseInsensitiveCollator);
        ordering.append(field(projectRowType, 2), true);
        List<ExpressionGenerator> convertCaseInsensitiveToUpper =
            Arrays.asList(field(projectRowType, 0),
                          toUpper(field(projectRowType, 1)),
                          field(projectRowType, 2));
        Operator plan =
            project_Default(
                distinct_Partial(
                    sort_InsertionLimited(
                        project,
                        projectRowType,
                        ordering,
                        SortOption.PRESERVE_DUPLICATES,
                        db.length),
                    projectRowType,
                    Arrays.asList(caseSensitiveCollator, caseInsensitiveCollator, null),
                    Types3Switch.ON),
                projectRowType,
                convertCaseInsensitiveToUpper);
        RowBase[] expected = new RowBase[] {
            row(projectRowType, "AA_cs", "AA_CI", 0),
            row(projectRowType, "Aa_cs", "AA_CI", 0),
            row(projectRowType, "BB_cs", "BB_CI", 0),
            row(projectRowType, "Bb_cs", "BB_CI", 0),
            row(projectRowType, "aA_cs", "AA_CI", 0),
            row(projectRowType, "aa_cs", "AA_CI", 0),
            row(projectRowType, "bB_cs", "BB_CI", 0),
            row(projectRowType, "bb_cs", "BB_CI", 0),
            row(projectRowType, "x", "X", 0),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    private int t;
    private UserTableRowType tRowType;
    private Group group;
    private AkCollator caseSensitiveCollator;
    private AkCollator caseInsensitiveCollator;
}

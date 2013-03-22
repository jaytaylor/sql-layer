
package com.akiban.sql.optimizer.rule;

import java.util.Arrays;
import java.util.List;

public class DefaultRules
{
    /** These are the rules that get run for the new types compilation. */
    public static final List<BaseRule> DEFAULT_RULES_NEWTYPES = Arrays.asList(
            // These aren't singletons because someday they will have options.
            new ASTStatementLoader(),
            new AggregateMapper(),
            new AggregateToDistinctMapper(),
            new OverloadAndTInstanceResolver(),
            new ConstantFolder(true),
            new OuterJoinPromoter(),
            new ColumnEquivalenceFinder(),
            new GroupJoinFinder(),
            new InConditionReverser(),
            new JoinAndIndexPicker(),
            new NestedLoopMapper(),
            new BranchJoiner(),
            new SelectPreponer(),
            new AggregateSplitter(),
            new SortSplitter(),
            new MapFolder(),
            new ExpressionCompactor(),
            new HalloweenRecognizer(),
            new OperatorAssembler(true)
    );

    /** These are the rules that get run old types compilation. */
    public static final List<BaseRule> DEFAULT_RULES_OLDTYPES = Arrays.asList(
        // These aren't singletons because someday they will have options.
        new ASTStatementLoader(),
        new AggregateMapper(),
        new AggregateToDistinctMapper(),
        new ConstantFolder(false),
        new OuterJoinPromoter(),
        new ColumnEquivalenceFinder(),
        new GroupJoinFinder(),
        new InConditionReverser(),
        new JoinAndIndexPicker(),
        new NestedLoopMapper(),
        new BranchJoiner(),
        new SelectPreponer(),
        new AggregateSplitter(),
        new SortSplitter(),
        new MapFolder(),
        new ExpressionCompactor(),
        new HalloweenRecognizer(),
        new OperatorAssembler(false)
     );

    private DefaultRules() {
    }
}

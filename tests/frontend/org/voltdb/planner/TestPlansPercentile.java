/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.planner;

import static org.voltdb.types.ExpressionType.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.types.ExpressionType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Mostly here we're concerned that an PERCENTILE_X aggregate function is
 * handled correctly in both single- and multi-partition contexts.  In a single partition context,
 * we expect PERCENTILE_X to be appear in the plan.  This is the simplest case.
 *
 * For multi-part plans, there are two possibilities:
 * - PERCENTILE_X is accompanied by other aggregates that cannot be pushed down
 *   (e.g., count(distinct col)), in this case, we must ship all the rows to the coordinator,
 *   so we expect to just evaluate PERCENTILE_X on the coordinator.
 * - PERCENTILE_X appears as the only aggregate on the select list, or all the other
 *   aggregates can be pushed down.  In this case, we "split" the aggregate function to two:
 *   - ROWS_TO_TDIGEST, which produces a t-digest for each partition
 *   - One coordinator, TDIGEST_TO_PERCENTILE_X which produces the estimate (as a double)
 *
 */
@RunWith(Parameterized.class)
public class TestPlansPercentile extends PlannerTestCase {
    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"median", AGGREGATE_MEDIAN, AGGREGATE_TDIGEST_TO_MEDIAN}
                ,{"percentile_1", AGGREGATE_PERCENTILE_1, AGGREGATE_TDIGEST_TO_PERCENTILE_1}
                ,{"percentile_5", AGGREGATE_PERCENTILE_5, AGGREGATE_TDIGEST_TO_PERCENTILE_5}
                ,{"percentile_25", AGGREGATE_PERCENTILE_25, AGGREGATE_TDIGEST_TO_PERCENTILE_25}
                ,{"percentile_75", AGGREGATE_PERCENTILE_75, AGGREGATE_TDIGEST_TO_PERCENTILE_75}
                ,{"percentile_95", AGGREGATE_PERCENTILE_95, AGGREGATE_TDIGEST_TO_PERCENTILE_95}
                ,{"percentile_99", AGGREGATE_PERCENTILE_99, AGGREGATE_TDIGEST_TO_PERCENTILE_99}
        });
    }

    private final String function;
    private final ExpressionType expressionType;
    private final ExpressionType pushDownExpressionType;
    private final ExpressionType pullUpExpressionType;

    public TestPlansPercentile(String function, ExpressionType expressionType, ExpressionType pullUpExpressionType) {
        this.function = function;
        this.expressionType = expressionType;
        this.pushDownExpressionType = AGGREGATE_VALUES_TO_TDIGEST;
        this.pullUpExpressionType = pullUpExpressionType;
    }

    private static final int COORDINATOR_FRAG = 0;
    private static final int PARTITION_FRAG = 1;

    @Before
    public void setUp() throws Exception {
        setupSchema(getClass().getResource("testplans-count-ddl.sql"),
                "testcount", false);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    private void assertAggPlanNodeContainsFunctions(AggregatePlanNode node, ExpressionType[] expectedAggFns) {
        List<ExpressionType> actualAggFns = node.getAggregateTypes();

        assertEquals("Wrong number of aggregate functions in plan", expectedAggFns.length, actualAggFns.size());

        int i = 0;
        for (ExpressionType expectedAggFn : expectedAggFns) {
            assertEquals("Found unexpected agg function", expectedAggFn, actualAggFns.get(i));
            ++i;
        }
    }

    private void assertFragContainsAggWithFunctions(AbstractPlanNode frag, ExpressionType... expectedAggFns) {
        List<AbstractPlanNode> aggNodes = findAllAggPlanNodes(frag);
        assertFalse("No aggregation node in fragment!", 0 == aggNodes.size());
        assertEquals("More than one aggregation node in fragment!", 1, aggNodes.size());

        AggregatePlanNode aggNode = (AggregatePlanNode)aggNodes.get(0);
        assertAggPlanNodeContainsFunctions(aggNode, expectedAggFns);
    }

    private void assertFragContainsTwoAggsWithFunctions(AbstractPlanNode frag,
                                                        ExpressionType[] expectedAggFnsFirst,
                                                        ExpressionType[] expectedAggFnsSecond) {
        List<AbstractPlanNode> aggNodes = findAllAggPlanNodes(frag);
        assertEquals("Wrong number of aggregation nodes in fragment!", 2, aggNodes.size());

        assertAggPlanNodeContainsFunctions((AggregatePlanNode)aggNodes.get(0), expectedAggFnsFirst);
        assertAggPlanNodeContainsFunctions((AggregatePlanNode)aggNodes.get(1), expectedAggFnsSecond);
    }

    private void assertFragContainsNoAggPlanNodes(AbstractPlanNode node) {
        List<AbstractPlanNode> aggNodes = findAllAggPlanNodes(node);
        assertEquals("Found an aggregation node in fragment, but didn't expect to!", 0, aggNodes.size());
    }

    @Test
    public void testSinglePartitionTableAgg() throws Exception {
        List<AbstractPlanNode> pn = compileToFragments("SELECT " + function + "(age) from T1");
        assertEquals(1,  pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG), expressionType);

        pn = compileToFragments("select " + function + "(age), sum(points) from t1");
        assertEquals(1,  pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                expressionType,
                AGGREGATE_SUM);

        pn = compileToFragments("select " + function + "(age), sum(distinct points) from t1");
        assertEquals(1,  pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                expressionType,
                AGGREGATE_SUM);

    }

    @Test
    public void testSinglePartitionWithGroupBy() throws Exception {
        List<AbstractPlanNode> pn = compileToFragments(
                "SELECT id, " + function + "(age) "
                        + "from T1 "
                        + "group by id");
        assertEquals(1,  pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG), expressionType);

        pn = compileToFragments(
                "select age, " + function + "(points), max(username) "
                        + "from t2 "
                        + "group by age");
        assertEquals(1,  pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                expressionType,
                AGGREGATE_MAX);

        pn = compileToFragments(
                "select username, " + function + "(age), avg(distinct points) "
                        + "from t2 "
                        + "group by username");
        assertEquals(1,  pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                expressionType,
                AGGREGATE_AVG);
    }

    @Test
    public void testMultiPartitionTableAgg() throws Exception {
        List<AbstractPlanNode> pn = compileToFragments("SELECT " + function + "(num) from P1");
        assertEquals(2,  pn.size());

        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG), pullUpExpressionType);
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG), pushDownExpressionType);

        // Two push-down-able aggs.
        pn = compileToFragments("SELECT " + function + "(num), count(ratio) from P1");
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                pullUpExpressionType,
                AGGREGATE_SUM);
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                pushDownExpressionType,
                AGGREGATE_COUNT);

        // Three push-down-able aggs.
        pn = compileToFragments("SELECT " + function + "(num), min(desc), max(ratio) from P1");
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                pullUpExpressionType,
                AGGREGATE_MIN, AGGREGATE_MAX);
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                pushDownExpressionType,
                AGGREGATE_MIN, AGGREGATE_MAX);

        // With an agg that can be pushed down, but only because its argument is a partition key.
        pn = compileToFragments("SELECT " + function + "(num), count(distinct id) from P1");
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                pullUpExpressionType,
                AGGREGATE_SUM);
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                pushDownExpressionType,
                AGGREGATE_COUNT);

        // With an agg that can be pushed down, but only because its argument is a partition key.
        // Also, with compact count distinct with partition key as argument.
        pn = compileToFragments("SELECT " + function + "(id), count(distinct id) from P1");
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                pullUpExpressionType,
                AGGREGATE_SUM);
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                pushDownExpressionType,
                AGGREGATE_COUNT);

        // With an agg that cannot be pushed down,
        pn = compileToFragments("SELECT sum(distinct ratio), " + function + "(num) from P1");
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                AGGREGATE_SUM,
                expressionType);
        assertFragContainsNoAggPlanNodes(pn.get(PARTITION_FRAG));
    }

    @Test
    public void testMultiPartitionWithGroupBy() throws Exception {
        List<AbstractPlanNode> pn = compileToFragments(
                "SELECT desc as modid, " + function + "(num) "
                        + "from P1 "
                        + "group by desc");
        assertEquals(2,  pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG), pullUpExpressionType);
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG), pushDownExpressionType);

        // Two push-down-able aggs.
        pn = compileToFragments("SELECT desc, " + function + "(num), count(ratio) "
                + "from P1 "
                + "group by desc");
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                pullUpExpressionType,
                AGGREGATE_SUM);
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                pushDownExpressionType,
                AGGREGATE_COUNT);

        // A case similar to above.
        pn = compileToFragments(
                "SELECT desc, " + function + "(num), max(ratio) "
                        + "from P1 "
                        + "group by desc");
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                pullUpExpressionType,
                AGGREGATE_MAX);
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                pushDownExpressionType,
                AGGREGATE_MAX);

        // With an agg that can be pushed down, but only because its argument is a partition key.
        pn = compileToFragments(
                "SELECT ratio, " + function + "(num), count(distinct id) "
                        + "from P1 "
                        + "group by ratio");
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                pullUpExpressionType,
                AGGREGATE_SUM);
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                pushDownExpressionType,
                AGGREGATE_COUNT);

        // With an agg that can be pushed down, but only because its argument is a partition key.
        // Also, with compact count distinct with partition key as argument.
        pn = compileToFragments(
                "SELECT desc, " + function + "(id), count(distinct id) "
                        + "from P1 "
                        + "group by desc");
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                pullUpExpressionType,
                AGGREGATE_SUM);
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                pushDownExpressionType,
                AGGREGATE_COUNT);

        // With partition key as group by key.
        // In this case, all aggregation can be done on partitions,
        // coordinator just concatenates result
        pn = compileToFragments(
                "SELECT id, sum(distinct ratio), " + function + "(num) "
                        + "from P1 "
                        + "group by id");
        assertFragContainsNoAggPlanNodes(pn.get(COORDINATOR_FRAG));
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                AGGREGATE_SUM,
                expressionType);
    }

    @Test
    public void testWithSubqueries() throws Exception {

        // Single-partition statement with a subquery (table agg)
        List<AbstractPlanNode> pn = compileToFragments(
                "select * "
                        + "from "
                        + "  T1, "
                        + "  (select " + function + "(age) from t1) as subq");
        assertEquals(1,  pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                expressionType);

        // Single-partition statement with a subquery (with group by)
        pn = compileToFragments(
                "select * "
                        + "from "
                        + "  (select username, " + function + "(age), avg(distinct points) "
                        + "   from t2 "
                        + "   group by username) as subq"
                        + "  inner join t2 "
                        + "  on t2.username = subq.username;");
        assertEquals(1,  pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                expressionType,
                AGGREGATE_AVG);

        // multi-partition table agg
        pn = compileToFragments(
                "select * "
                        + "from "
                        + "t1, "
                        + "(SELECT sum(distinct ratio), " + function + "(num) from P1) as subq");
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                AGGREGATE_SUM,
                expressionType);
        assertFragContainsNoAggPlanNodes(pn.get(PARTITION_FRAG));

        // single-part plan on partitioned tables, with GB in subquery
        pn = compileToFragments(
                "select * "
                        + "from p1 "
                        + "inner join "
                        + "(SELECT id, sum(distinct ratio), " + function + "(num) "
                        + "from P1 "
                        + "where id = 10 "
                        + "group by id) as subq "
                        + "on subq.id = p1.id");
        assertEquals(1, pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                AGGREGATE_SUM,
                expressionType);

        // multi-part plan on partitioned tables, with GB in subquery
        pn = compileToFragments(
                "select * "
                        + "from t1 "
                        + "inner join "
                        + "(SELECT id, " + function + "(num) "
                        + "from P1 "
                        + "group by id) as subq "
                        + "on subq.id = t1.id");
        for (AbstractPlanNode n : pn) {
            System.out.println(n.toExplainPlanString());
        }
        assertEquals(2, pn.size());
        assertFragContainsNoAggPlanNodes(pn.get(COORDINATOR_FRAG));
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                expressionType);
    }

    @Test
    public void testSubqueriesWithMultipleAggs() throws Exception {
        List<AbstractPlanNode> pn;

        // In this query, one agg plan node is distributed across fragments (p1),
        // but the other is not (t1).
        pn = compileToFragments("select " + function + "(num) "
                + "from (select " + function + "(points) from t1) as repl_subquery,"
                + "  p1");
        assertEquals(2, pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                pullUpExpressionType);
        assertFragContainsTwoAggsWithFunctions(pn.get(PARTITION_FRAG),
                new ExpressionType[] {expressionType},
                new ExpressionType[] {pushDownExpressionType});

        // Like above but with some more aggregate functions
        // (which breaks the push-down-ability of distributed agg)
        pn = compileToFragments("select " + function + "(num), sum(distinct num) "
                + "from (select " + function + "(points) from t1) as repl_subquery,"
                + "  p1");
        assertEquals(2, pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                expressionType,
                AGGREGATE_SUM);
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                expressionType);

        // As above but partitioned and replicated tables are swapped.
        pn = compileToFragments("select " + function + "(points) "
                + "from (select " + function + "(num) from p1) as repl_subquery,"
                + "  t1");
        assertEquals(2, pn.size());
        assertFragContainsTwoAggsWithFunctions(pn.get(COORDINATOR_FRAG),
                new ExpressionType[] {pullUpExpressionType},
                new ExpressionType[] {expressionType});
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                pushDownExpressionType);

        // Like above but with some more aggregate functions
        // (which breaks the push-down-ability of distributed agg)
        pn = compileToFragments("select " + function + "(points) "
                + "from (select " + function + "(num), sum(distinct num) from p1) as repl_subquery,"
                + "  t1");
        assertEquals(2, pn.size());
        assertFragContainsTwoAggsWithFunctions(pn.get(COORDINATOR_FRAG),
                new ExpressionType[] {expressionType, AGGREGATE_SUM},
                new ExpressionType[] {expressionType});
        assertFragContainsNoAggPlanNodes(pn.get(PARTITION_FRAG));
    }
}
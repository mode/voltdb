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

package org.voltdb.regressionsuites;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.StreamSupport;

import org.junit.Assert;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestJoinsSuite extends RegressionSuite {
    private static final long NULL_VALUE = Long.MIN_VALUE;

    private static final String[] SEQ_TABLES =
            new String[] { "R1", "R2", "P1" };

    private static final String[] INDEXED_TABLES =
            new String[] { "R1", "R2", "R3", "R4", "P2", "P3", "P4" };

    // Operators that should be safe and effective for use as partition key
    // filters to minimally enable partition table joins.
    private static final String[] JOIN_OPS = {"=", "IS NOT DISTINCT FROM"};

    public TestJoinsSuite(String name) {
        super(name);
    }

    public void testSeqJoins() throws Exception {
        Client client = getClient();
        for (String joinOp : JOIN_OPS) {
            truncateTables(client, SEQ_TABLES);
            subtestTwoTableSeqInnerJoin(client, joinOp);
            truncateTables(client, SEQ_TABLES);
            subtestTwoTableSeqInnerWhereJoin(client, joinOp);
            truncateTables(client, SEQ_TABLES);
            subtestTwoTableSeqInnerFunctionJoin(client, joinOp);
            truncateTables(client, SEQ_TABLES);
            subtestTwoTableSeqInnerMultiJoin(client, joinOp);
            truncateTables(client, SEQ_TABLES);
            subtestThreeTableSeqInnerMultiJoin(client, joinOp);
            truncateTables(client, SEQ_TABLES);
            subtestSeqOuterJoin(client, joinOp);
        }
        truncateTables(client, SEQ_TABLES);
        subtestSelfJoin(client);
    }

    /**
     * Two table NLJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestTwoTableSeqInnerJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R1.INSERT", 1, 1, 1); // 1,1,1,3
        client.callProcedure("R1.INSERT", 1, 1, 1); // 1,1,1,3
        client.callProcedure("R1.INSERT", 2, 2, 2); // Eliminated
        client.callProcedure("R1.INSERT", 3, 3, 3); // 3,3,3,4

        client.callProcedure("R2.INSERT", 1, 3); // 1,1,1,3
        client.callProcedure("R2.INSERT", 3, 4); // 3,3,3,4

        String query;

        query = "SELECT * FROM R1 JOIN R2 " +
                "ON R1.A " + joinOp + " R2.A;";
        validateRowCount(client, query, 3);
        query = "SELECT * FROM R1 JOIN R2 USING(A);";
        validateRowCount(client, query, 3);
        client.callProcedure("P1.INSERT", 1, 1); // 1,1,1,3
        client.callProcedure("P1.INSERT", 1, 1); // 1,1,1,3
        client.callProcedure("P1.INSERT", 2, 2); // Eliminated
        client.callProcedure("P1.INSERT", 3, 3); // 3,3,3,4
        query = "SELECT * FROM P1 JOIN R2 " +
                "ON P1.A " + joinOp + " R2.A;";
        validateRowCount(client, query, 3);

        // Insert some null values to validate the difference between "="
        // and "IS NOT DISTINCT FROM".
        query = "SELECT * FROM P1 JOIN R2 ON P1.C " + joinOp + " R2.C";

        final int BASELINE_COUNT = 1;
        // Validate a baseline result without null join key values.
        validateRowCount(client, query, BASELINE_COUNT);

        client.callProcedure("P1.INSERT", 4, null);
        client.callProcedure("P1.INSERT", 5, null);
        final int LHS_NULL_COUNT = 2;

        // With nulls on just ONE one side, the joinOp makes no difference.
        // The result still matches the baseline.
        validateRowCount(client, query, BASELINE_COUNT);

        // With N nulls on one side and M nulls on the other,
        // expect "=" to continue returning the baseline result while
        // "IS NOT DISTINCT FROM" matches NxM more matches.
        client.callProcedure("R2.INSERT", 6, null);
        client.callProcedure("R2.INSERT", 7, null);
        client.callProcedure("R2.INSERT", 8, null);
        final int RHS_NULL_COUNT = 3;
        validateRowCount(client, query, joinOp.equals("=") ?
                BASELINE_COUNT :
                (BASELINE_COUNT + LHS_NULL_COUNT*RHS_NULL_COUNT));
    }

    /**
     * Two table NLJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestTwoTableSeqInnerWhereJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R1.INSERT", 1, 5, 1); // 1,5,1,1,3
        client.callProcedure("R1.INSERT", 1, 1, 1); // eliminated by WHERE
        client.callProcedure("R1.INSERT", 2, 2, 2); // Eliminated by JOIN
        client.callProcedure("R1.INSERT", 3, 3, 3); // eliminated by WHERE
        client.callProcedure("R2.INSERT", 1, 3); // 1,5,1,1,3
        client.callProcedure("R2.INSERT", 3, 4); // eliminated by WHERE

        String query;

        query = "SELECT * FROM R1 JOIN R2 " +
                "ON R1.A " + joinOp + " R2.A WHERE R1.C > R2.C;";
        validateRowCount(client, query, 1);
        query = "SELECT * FROM R1 JOIN R2 " +
                "ON R1.A " + joinOp + " R2.A WHERE R1.C > R2.C;";
        validateRowCount(client, query, 1);
        query = "SELECT * FROM R1 INNER JOIN R2 " +
                "ON R1.A " + joinOp + " R2.A WHERE R1.C > R2.C;";
        validateRowCount(client, query, 1);
        query = "SELECT * FROM R1, R2 WHERE R1.A " + joinOp + " R2.A AND R1.C > R2.C;";
        validateRowCount(client, query, 1);

        client.callProcedure("P1.INSERT", 1, 5); // 1,5,1,1,3
        client.callProcedure("P1.INSERT", 1, 1); // eliminated by WHERE
        client.callProcedure("P1.INSERT", 2, 2); // Eliminated by JOIN
        client.callProcedure("P1.INSERT", 3, 3); // eliminated by WHERE
        query = "SELECT * FROM P1 JOIN R2 " +
                "ON P1.A " + joinOp + " R2.A WHERE P1.C > R2.C;";
        validateRowCount(client, query, 1);
    }

    /**
     * Two table NLJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestTwoTableSeqInnerFunctionJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R1.INSERT", -1, 5, 1); //  -1,5,1,1,3
        client.callProcedure("R1.INSERT", 1, 1, 1); // 1,1,1,1,3
        client.callProcedure("R1.INSERT", 2, 2, 2); // Eliminated by JOIN
        client.callProcedure("R1.INSERT", 3, 3, 3); // 3,3,3,3,4

        client.callProcedure("R2.INSERT", 1, 3); // 1,1,1,1,3
        client.callProcedure("R2.INSERT", 3, 4); // 3,3,3,3,4

        String query;

        query = "SELECT * FROM R1 JOIN R2 " +
                "ON ABS(R1.A) " + joinOp + " R2.A;";
        validateRowCount(client, query, 3);
    }

    /**
     * Two table NLJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestTwoTableSeqInnerMultiJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R1.INSERT", 1, 1, 1); // 1,1,1,1,1
        client.callProcedure("R1.INSERT", 2, 2, 2); // Eliminated by JOIN
        client.callProcedure("R1.INSERT", 3, 3, 3); // Eliminated by JOIN
        client.callProcedure("R2.INSERT", 1, 1); // 1,1,1,1,1
        client.callProcedure("R2.INSERT", 1, 3); // Eliminated by JOIN
        client.callProcedure("R2.INSERT", 3, 4); // Eliminated by JOIN

        String query;

        query = "SELECT * FROM R1 JOIN R2 " +
                "ON R1.A " + joinOp + " R2.A AND R1.C " + joinOp + " R2.C;";
        validateRowCount(client, query, 1);
        query = "SELECT * FROM R1, R2 " +
                "WHERE R1.A " + joinOp + " R2.A AND R1.C " + joinOp + " R2.C;";
        validateRowCount(client, query, 1);
        query = "SELECT * FROM R1 JOIN R2 USING (A,C);";
        validateRowCount(client, query, 1);
        query = "SELECT * FROM R1 JOIN R2 USING (A,C) WHERE A > 0;";
        validateRowCount(client, query, 1);
        query = "SELECT * FROM R1 JOIN R2 USING (A,C) WHERE A > 4;";
        validateRowCount(client, query, 0);

        client.callProcedure("P1.INSERT", 1, 1); // 1,1,1,1,1
        client.callProcedure("P1.INSERT", 2, 2); // Eliminated by JOIN
        client.callProcedure("P1.INSERT", 3, 3); // Eliminated by JOIN
        query = "SELECT * FROM P1 JOIN R2 USING (A,C);";
        validateRowCount(client, query, 1);
        query = "SELECT * FROM P1 JOIN R2 USING (A,C) WHERE A > 0;";
        validateRowCount(client, query, 1);
        query = "SELECT * FROM P1 JOIN R2 USING (A,C) WHERE A > 4;";
        validateRowCount(client, query, 0);
    }

    /**
     * Three table NLJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestThreeTableSeqInnerMultiJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R1.INSERT", 1, 1, 1); // 1,3,1,1,1,1,3
        client.callProcedure("R1.INSERT", 2, 2, 2); // Eliminated by P1 R1 JOIN
        client.callProcedure("R1.INSERT", -1, 3, 3); // -1,0,-1,3,3,4,0 Eliminated by WHERE

        client.callProcedure("R2.INSERT", 1, 1); // Eliminated by P1 R2 JOIN
        client.callProcedure("R2.INSERT", 1, 3); // 1,3,1,1,1,1,3
        client.callProcedure("R2.INSERT", 3, 4); // Eliminated by P1 R2 JOIN
        client.callProcedure("R2.INSERT", 4, 0); // Eliminated by WHERE

        client.callProcedure("P1.INSERT", 1, 3); // 1,3,1,1,1,1,3
        client.callProcedure("P1.INSERT", -1, 0); // Eliminated by WHERE
        client.callProcedure("P1.INSERT", 8, 4); // Eliminated by P1 R1 JOIN

        String query;

        query = "SELECT * FROM P1 JOIN R1 " +
                "ON P1.A " + joinOp + " R1.A JOIN R2 " +
                "ON P1.C " + joinOp + " R2.C WHERE P1.A > 0";
        validateRowCount(client, query, 1);
    }

    /**
     * Self Join table NLJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestSelfJoin(Client client) throws Exception {
        client.callProcedure("R1.INSERT", 1, 2, 7);
        client.callProcedure("R1.INSERT", 2, 2, 7);
        client.callProcedure("R1.INSERT", 4, 3, 2);
        client.callProcedure("R1.INSERT", 5, 6, null);

        String query;

        // 2,2,1,1,2,7
        // 2,2,1,2,2,7
        query = "SELECT * FROM R1 A JOIN R1 B " +
                "ON A.A = B.C;";
        validateRowCount(client, query, 2);

        // 1,2,7,NULL,NULL,NULL
        // 2,2,7,4,3,2
        // 4,3,2,NULL,NULL,NULL
        // 5,6,NULL,NULL,NULL,NULL
        query = "SELECT * FROM R1 A LEFT JOIN R1 B " +
                "ON A.A = B.D;";
        validateRowCount(client, query, 4);
    }

    public void testIndexJoins() throws Exception {
        Client client = getClient();
        for (String joinOp : JOIN_OPS) {
            truncateTables(client, INDEXED_TABLES);
            subtestTwoTableIndexInnerJoin(client, joinOp);
            truncateTables(client, INDEXED_TABLES);
            subtestTwoTableIndexInnerWhereJoin(client, joinOp);
            truncateTables(client, INDEXED_TABLES);
            subtestThreeTableIndexInnerMultiJoin(client, joinOp);
            truncateTables(client, INDEXED_TABLES);
            subtestIndexOuterJoin(client, joinOp);
            truncateTables(client, INDEXED_TABLES);
            subtestDistributedOuterJoin(client, joinOp);
        }
    }
    /**
     * Two table NLIJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestTwoTableIndexInnerJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R2.INSERT", 1, 3); // 1,1,1,3
        client.callProcedure("R2.INSERT", 3, 4); // 3,3,3,4

        client.callProcedure("R3.INSERT", 1, 1); // 1,1,1,3
        client.callProcedure("R3.INSERT", 1, 1); // 1,1,1,3
        client.callProcedure("R3.INSERT", 2, 2); // Eliminated
        client.callProcedure("R3.INSERT", 3, 3); // 3,3,3,4

        client.callProcedure("R4.INSERT", 1, 1); // 1,1,1,3
        client.callProcedure("R4.INSERT", 1, 1); // 1,1,1,3
        client.callProcedure("R4.INSERT", 2, 2); // Eliminated
        client.callProcedure("R4.INSERT", 3, 4); // 3,3,3,4
        // Add null values to match null R2 values to be inserted later.
        // Each may match from 2 to 4 R2 rows depending on the ON clause.
        client.callProcedure("R4.INSERT", null, 21);
        client.callProcedure("R4.INSERT", 22, null);
        client.callProcedure("R4.INSERT", null, null);

        client.callProcedure("P3.INSERT", 1, 1); // 1,1,1,3
        client.callProcedure("P3.INSERT", 2, 2); // Eliminated
        client.callProcedure("P3.INSERT", 3, 3); // 3,3,3,4

        client.callProcedure("P4.INSERT", 1, 1); // 1,1,1,3
        client.callProcedure("P4.INSERT", 2, 2); // Eliminated
        client.callProcedure("P4.INSERT", 3, 4); // 3,3,3,4
        // Add null values to match null R2 values to be inserted later.
        // Each may match from 2 to 4 R2 rows depending on the ON clause.
        client.callProcedure("P4.INSERT", 22, null);

        String query;

        // loop once before inserting non-matched R2 nulls and once after.
        for (int ii = 0; ii < 2; ++ii) {
            query = "SELECT * FROM R3 JOIN R2 " +
                    "ON R3.A " + joinOp + " R2.A;";
            validateRowCount(client, query, 3);

            query = "SELECT * FROM P3 JOIN R2 " +
                    "ON P3.A " + joinOp + " R2.A;";
            validateRowCount(client, query, 2);

            // Add null values that both joinOps must initially ignore
            // as not having any matches but that only "=" should ignore
            // when tables with null matches are queried.
            // It's OK for the second iteration to generate duplicates.
            client.callProcedure("R2.INSERT", null, 21);
            client.callProcedure("R2.INSERT", 22, null);
            client.callProcedure("R2.INSERT", null, null);
        }

        String anotherJoinOp;

        //
        // Joins with R4's matching nulls.
        //
        query = "SELECT * FROM R4 JOIN R2 " +
                "ON R4.A " + joinOp + " R2.A;";
        if ("=".equals(joinOp)) {
            validateRowCount(client, query, 5);
        }
        else {
            //* enable to debug */ System.out.println("Result: " + client.callProcedure("@AdHoc", query).getResults()[0]);
            validateRowCount(client, query, 13);
        }

        query = "SELECT * FROM R4 JOIN R2 " +
                "ON R4.G " + joinOp + " R2.C;";
        if ("=".equals(joinOp)) {
            validateRowCount(client, query, 3);
        }
        else {
            // "IS NOT DISTINCT FROM" in the end expression, but not in the search expression (range scan).
            validateRowCount(client, query, 11);
        }

        anotherJoinOp = JOIN_OPS[0];
        query = "SELECT * FROM R4 JOIN R2 " +
                "ON R4.A " + joinOp + " R2.A " +
                "AND R4.G " + anotherJoinOp + " R2.C;";
        if ("=".equals(joinOp)) {
            validateRowCount(client, query, 1);
        }
        else {
            validateRowCount(client, query, 3);
        }

        anotherJoinOp = JOIN_OPS[1];
        query = "SELECT * FROM R4 JOIN R2 " +
                "ON R4.A " + joinOp + " R2.A " +
                "AND R4.G " + anotherJoinOp + " R2.C;";
        if ("=".equals(joinOp)) {
            validateRowCount(client, query, 3);
        }
        else {
            validateRowCount(client, query, 7);
        }

        //
        // Joins with P4's matching nulls.
        //
        query = "SELECT * FROM P4 JOIN R2 " +
                "ON P4.A " + joinOp + " R2.A;";
        validateRowCount(client, query, 4);

        query = "SELECT * FROM P4 JOIN R2 " +
                "ON P4.G " + joinOp + " R2.C;";
        if ("=".equals(joinOp)) {
            validateRowCount(client, query, 1);
        }
        else {
            validateRowCount(client, query, 5);
        }

        anotherJoinOp = JOIN_OPS[0];
        query = "SELECT * FROM P4 JOIN R2 " +
                        "ON P4.A " + joinOp + " R2.A " +
                        "AND P4.G " + anotherJoinOp + " R2.C;";
        validateRowCount(client, query, 1);

        anotherJoinOp = JOIN_OPS[1];
        query = "SELECT * FROM P4 JOIN R2 " +
                "ON P4.A " + joinOp + " R2.A " +
                "AND P4.G " + anotherJoinOp + " R2.C;";
        validateRowCount(client, query, 3);

    }

    /**
     * Two table NLIJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestTwoTableIndexInnerWhereJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R3.INSERT", 1, 5); // eliminated by WHERE
        client.callProcedure("R3.INSERT", 1, 1); // eliminated by WHERE
        client.callProcedure("R3.INSERT", 2, 2); // Eliminated by JOIN
        client.callProcedure("R3.INSERT", 3, 3); // eliminated by WHERE
        client.callProcedure("R3.INSERT", 4, 5); // 4,5,4,2
        client.callProcedure("R2.INSERT", 1, 3); // 1,5,1,1,3
        client.callProcedure("R2.INSERT", 3, 4); // eliminated by WHERE
        client.callProcedure("R2.INSERT", 4, 2); // 4,5,4,2

        String query;

        query = "SELECT * FROM R3 JOIN R2 " +
                "ON R3.A " + joinOp + " R2.A WHERE R3.A > R2.C;";
        validateRowCount(client, query, 1);

        client.callProcedure("P3.INSERT", 1, 5); // eliminated by WHERE
        client.callProcedure("P3.INSERT", 2, 2); // Eliminated by JOIN
        client.callProcedure("P3.INSERT", 3, 3); // eliminated by WHERE
        client.callProcedure("P3.INSERT", 4, 3); // 4,3,4,2
        query = "SELECT * FROM P3 JOIN R2 " +
                "ON P3.A " + joinOp + " R2.A WHERE P3.A > R2.C;";
        validateRowCount(client, query, 1);
    }

    /**
     * Three table NLIJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestThreeTableIndexInnerMultiJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R1.INSERT", 1, 1, 1); // 1,3,1,1,1,1,3
        client.callProcedure("R1.INSERT", 2, 2, 2); // Eliminated by P3 R1 JOIN
        client.callProcedure("R1.INSERT", -1, 3, 3); // -1,0,-1,3,3,4,0 Eliminated by WHERE

        client.callProcedure("R2.INSERT", 1, 1); // Eliminated by P3 R2 JOIN
        client.callProcedure("R2.INSERT", 1, 3); // 1,3,1,1,1,1,3
        client.callProcedure("R2.INSERT", 3, 4); // Eliminated by P3 R2 JOIN
        client.callProcedure("R2.INSERT", 4, 0); // Eliminated by WHERE

        client.callProcedure("P3.INSERT", 1, 3); // 1,3,1,1,1,1,3
        client.callProcedure("P3.INSERT", -1, 0); // Eliminated by WHERE
        client.callProcedure("P3.INSERT", 8, 4); // Eliminated by P3 R1 JOIN

        String query;

        query = "SELECT * FROM P3 JOIN R1 " +
                "ON P3.A " + joinOp + " R1.A JOIN R2 " +
                "ON P3.F " + joinOp + " R2.C WHERE P3.A > 0";
        validateRowCount(client, query, 1);
    }

    /**
     * Two table left and right NLJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestSeqOuterJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R1.INSERT", 1, 1, 1);
        client.callProcedure("R1.INSERT", 1, 2, 1);
        client.callProcedure("R1.INSERT", 2, 2, 2);
        client.callProcedure("R1.INSERT", -1, 3, 3);
        // R1 1st joined with R2 null
        // R1 2nd joined with R2 null
        // R1 3rd joined with R2 null
        // R1 4th joined with R2 null

        String query;
        VoltTable result;

        query = "SELECT * FROM R1 LEFT JOIN R2 " +
                "ON R1.A " + joinOp + " R2.C ORDER BY R1.A DESC, R1.C DESC";
        // Execution result:
        // 2, 2, 2, NULL, NULL
        // 1, 2, 1, NULL, NULL
        // 1, 1, 1, NULL, NULL
        // -1, 3, 3, NULL, NULL
        result = client.callProcedure("@AdHoc", query).getResults()[0];
        //* enable to debug */ System.out.println(result);
        assertEquals(4, result.getRowCount());
        VoltTableRow row = result.fetchRow(1);
        assertEquals(1, row.getLong(0));
        assertEquals(2, row.getLong(1));
        assertEquals(1, row.getLong(2));

        client.callProcedure("R2.INSERT", 1, 1);
        client.callProcedure("R2.INSERT", 1, 3);
        client.callProcedure("R2.INSERT", 3, null);
        // R1 1st joined with R2 1st
        // R1 2nd joined with R2 1st
        // R1 3rd joined with R2 null
        // R1 4th joined with R2 null
        query = "SELECT * FROM R1 LEFT JOIN R2 " +
                "ON R1.A " + joinOp + " R2.C";
        validateRowCount(client, query, 4);
        query = "SELECT * FROM R2 RIGHT JOIN R1 " +
                "ON R1.A " + joinOp + " R2.C";
        validateRowCount(client, query, 4);

        // Same as above but with partitioned table
        client.callProcedure("P1.INSERT", 1, 1);
        client.callProcedure("P1.INSERT", 1, 2);
        client.callProcedure("P1.INSERT", 2, 2);
        client.callProcedure("P1.INSERT", -1, 3);
        query = "SELECT * FROM P1 LEFT JOIN R2 " +
                "ON P1.A " + joinOp + " R2.C";
        validateRowCount(client, query, 4);

        // R1 1st joined with R2 with R2 1st
        // R1 2nd joined with R2 null (failed R1.C = 1)
        // R1 3rd joined with R2 null (failed  R1.A " + joinOp + " R2.C)
        // R1 4th3rd joined with R2 null (failed  R1.A " + joinOp + " R2.C)
        query = "SELECT * FROM R1 LEFT JOIN R2 " +
                "ON R1.A " + joinOp + " R2.C AND R1.C = 1";
        validateRowCount(client, query, 4);
        query = "SELECT * FROM R2 RIGHT JOIN R1 " +
                "ON R1.A " + joinOp + " R2.C AND R1.C = 1";
        validateRowCount(client, query, 4);
        // Same as above but with partitioned table
        query = "SELECT * FROM R2 RIGHT JOIN P1 " +
                "ON P1.A " + joinOp + " R2.C AND P1.C = 1";
        validateRowCount(client, query, 4);

        // R1 1st joined with R2 null - eliminated by the second join condition
        // R1 2nd joined with R2 null
        // R1 3rd joined with R2 null
        // R1 4th joined with R2 null
        query = "SELECT * FROM R1 LEFT JOIN R2 " +
                "ON R1.A " + joinOp + " R2.C AND R2.A = 100";
        validateRowCount(client, query, 4);

        // R1 1st - joined with R2 not null and eliminated by the filter condition
        // R1 2nd - joined with R2 not null and eliminated by the filter condition
        // R1 3rd - joined with R2 null
        // R1 4th - joined with R2 null
        query = "SELECT * FROM R1 LEFT JOIN R2 " +
                "ON R1.A " + joinOp + " R2.C WHERE R2.A IS NULL";
        validateRowCount(client, query, 2);
        // Same as above but with partitioned table
        query = "SELECT * FROM P1 LEFT JOIN R2 " +
                "ON P1.A " + joinOp + " R2.C WHERE R2.A IS NULL";
        validateRowCount(client, query, 2);

        // R1 1st - joined with R2 1st row
        // R1 2nd - joined with R2 null eliminated by the filter condition
        // R1 3rd - joined with R2 null eliminated by the filter condition
        // R1 4th - joined with R2 null eliminated by the filter condition
        query = "SELECT * FROM R1 LEFT JOIN R2 " +
                "ON R1.A " + joinOp + " R2.C WHERE R1.C = 1";
        validateRowCount(client, query, 1);

        // R1 1st - eliminated by the filter condition
        // R1 2nd - eliminated by the filter condition
        // R1 3rd - eliminated by the filter condition
        // R1 3rd - joined with the R2 null
        query = "SELECT * FROM R1 LEFT JOIN R2 " +
                "ON R1.A " + joinOp + " R2.C WHERE R1.A = -1";
        validateRowCount(client, query, 1);
        //* enable to debug */ System.out.println(result);
        // Same as above but with partitioned table
        query = "SELECT * FROM P1 LEFT JOIN R2 " +
                "ON P1.A " + joinOp + " R2.C WHERE P1.A = -1";
        validateRowCount(client, query, 1);
        //* enable to debug */ System.out.println(result);

        // R1 1st - joined with the R2
        // R1 1st - joined with the R2
        // R1 2nd - eliminated by the filter condition
        // R1 3rd - eliminated by the filter condition
        query = "SELECT * FROM R1 LEFT JOIN R2 " +
                "ON R1.A " + joinOp + " R2.C WHERE R1.A = 1";
        validateRowCount(client, query, 2);

        // R1 1st - eliminated by the filter condition
        // R1 2nd - eliminated by the filter condition
        // R1 3rd - joined with R2 null and pass the filter
        // R1 4th - joined with R2 null and pass the filter
        query = "SELECT * FROM R1 LEFT JOIN R2 " +
                "ON R1.A " + joinOp + " R2.C WHERE R2.A is NULL";
        validateRowCount(client, query, 2);
    }

    /**
     * Two table left and right NLIJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestIndexOuterJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R2.INSERT", 1, 1);
        client.callProcedure("R2.INSERT", 2, 2);
        client.callProcedure("R2.INSERT", 3, 3);
        client.callProcedure("R2.INSERT", 4, 4);

        String query;
        VoltTable result;

        // R2 1st joined with R3 null
        // R2 2nd joined with R3 null
        // R2 3rd joined with R3 null
        // R2 4th joined with R3 null
        query = "SELECT * FROM R2 LEFT JOIN R3 " +
                "ON R3.A " + joinOp + " R2.A " +
                "ORDER BY R2.A";
        result = client.callProcedure("@AdHoc", query).getResults()[0];
        assertEquals(4, result.getRowCount());
        VoltTableRow row = result.fetchRow(2);
        assertEquals(3, row.getLong(1));

        client.callProcedure("R3.INSERT", 1, 1);
        client.callProcedure("R3.INSERT", 2, 2);
        client.callProcedure("R3.INSERT", 5, 5);

        // R2 1st joined with R3 1st
        // R2 2nd joined with R3 2nd
        // R2 3rd joined with R3 null
        // R2 4th joined with R3 null
        query = "SELECT * FROM R2 LEFT JOIN R3 " +
                "ON R3.A " + joinOp + " R2.A";
        validateRowCount(client, query, 4);
        query = "SELECT * FROM R3 RIGHT JOIN R2 " +
                "ON R3.A " + joinOp + " R2.A";
        validateRowCount(client, query, 4);

        // Same as above but with partitioned table
        client.callProcedure("P2.INSERT", 1, 1);
        client.callProcedure("P2.INSERT", 2, 2);
        client.callProcedure("P2.INSERT", 3, 3);
        client.callProcedure("P2.INSERT", 4, 4);
        query = "SELECT * FROM P2 LEFT JOIN R3 " +
                "ON R3.A = P2.A";
        validateRowCount(client, query, 4);

        // R2 1st joined with R3 NULL R2.C < 0
        // R2 2nd joined with R3 null R2.C < 0
        // R2 3rd joined with R3 null R2.C < 0
        // R2 4th joined with R3 null R2.C < 0
        query = "SELECT * FROM R2 LEFT JOIN R3 " +
                "ON R3.A " + joinOp + " R2.A AND R2.C < 0";
        validateRowCount(client, query, 4);
        query = "SELECT * FROM R3 RIGHT JOIN R2 " +
                "ON R3.A " + joinOp + " R2.A AND R2.C < 0";
        validateRowCount(client, query, 4);
        // Same as above but with partitioned table
        query = "SELECT * FROM P2 LEFT JOIN R3 " +
                "ON R3.A " + joinOp + " P2.A AND P2.E < 0";

        // R2 1st joined with R3 null eliminated by R3.A > 1
        // R2 2nd joined with R3 2nd
        // R2 3rd joined with R3 null
        // R2 4th joined with R3 null
        query = "SELECT * FROM R2 LEFT JOIN R3 " +
                "ON R3.A " + joinOp + " R2.A AND R3.A > 1";
        validateRowCount(client, query, 4);
        query = "SELECT * FROM R3 RIGHT JOIN R2 " +
                "ON R3.A " + joinOp + " R2.A AND R3.A > 1";
        validateRowCount(client, query, 4);

        // R2 1st joined with R3 1st  but eliminated by  R3.A IS NULL
        // R2 2nd joined with R3 2nd  but eliminated by  R3.A IS NULL
        // R2 3rd joined with R3 null
        // R2 4th joined with R3 null
        query = "SELECT * FROM R2 LEFT JOIN R3 " +
                "ON R3.A " + joinOp + " R2.A WHERE R3.A IS NULL";
        if (joinOp.equals("=") || ! isHSQL()) {
            validateRowCount(client, query, 2); //// PENDING HSQL flaw investigation
        }
        else {
            result = client.callProcedure("@AdHoc", query).getResults()[0];
            System.out.println("Ignoring erroneous(?) HSQL baseline: " + result);
            if (2 == result.getRowCount()) {
                System.out.println("The HSQL error MAY have been solved. Consider simplifying this test.");
            }
        }
        query = "SELECT * FROM R3 RIGHT JOIN R2 " +
                "ON R3.A " + joinOp + " R2.A WHERE R3.A IS NULL";
        if (isHSQL()) { //// PENDING HSQL flaw investigation
            result = client.callProcedure("@AdHoc", query).getResults()[0];
            System.out.println("Ignoring erroneous(?) HSQL baseline: " + result);
            if (2 == result.getRowCount()) {
                System.out.println("The HSQL error MAY have been solved. Consider simplifying this test.");
            }
        }
        else {
            validateRowCount(client, query, 2);
        }
        // Same as above but with partitioned table
        query = "SELECT * FROM R3 RIGHT JOIN P2 " +
                "ON R3.A " + joinOp + " P2.A WHERE R3.A IS NULL";
        if (isHSQL()) { //// PENDING HSQL flaw investigation
            result = client.callProcedure("@AdHoc", query).getResults()[0];
            System.out.println("Ignoring erroneous(?) HSQL baseline: " + result);
            if (2 == result.getRowCount()) {
                System.out.println("The HSQL error MAY have been solved. Consider simplifying this test.");
            }
        }
        else {
            validateRowCount(client, query, 2);
        }

        // R2 1st eliminated by R2.C < 0
        // R2 2nd eliminated by R2.C < 0
        // R2 3rd eliminated by R2.C < 0
        // R2 4th eliminated by R2.C < 0
        query = "SELECT * FROM R2 LEFT JOIN R3 " +
                "ON R3.A " + joinOp + " R2.A WHERE R2.C < 0";
        validateRowCount(client, query, 0);
        // Same as above but with partitioned table
        query = "SELECT * FROM P2 LEFT JOIN R3 " +
                "ON R3.A " + joinOp + " P2.A WHERE P2.E < 0";
        validateRowCount(client, query, 0);

        // Outer table index scan
        // R3 1st eliminated by R3.A > 0 where filter
        // R3 2nd joined with R3 2
        // R3 3rd joined with R2 null
        query = "select * FROM R3 LEFT JOIN R2 " +
                "ON R3.A " + joinOp + " R2.A WHERE R3.A > 1";
        validateRowCount(client, query, 2);
    }

    /**
     * Two table left and right NLIJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestDistributedOuterJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("P2.INSERT", 1, 1);
        client.callProcedure("P2.INSERT", 2, 2);
        client.callProcedure("P2.INSERT", 3, 3);
        client.callProcedure("P2.INSERT", 4, 4);

        client.callProcedure("R3.INSERT", 1, 1);
        client.callProcedure("R3.INSERT", 2, 2);
        client.callProcedure("R3.INSERT", 4, 4);
        client.callProcedure("R3.INSERT", 5, 5);
        // R3 1st joined with P2 not null and eliminated by P2.A IS NULL
        // R3 2nd joined with P2 not null and eliminated by P2.A IS NULL
        // R3 3rd joined with P2 null (P2.A < 3)
        // R3 4th joined with P2 null

        String query;

        query = "SELECT * FROM P2 RIGHT JOIN R3 " +
                "ON R3.A " + joinOp + " P2.A AND P2.A < 3 WHERE P2.A IS NULL";
        if (isHSQL()) { //// PENDING HSQL flaw investigation
            VoltTable result = client.callProcedure("@AdHoc", query).getResults()[0];
            System.out.println("Ignoring erroneous(?) HSQL: " + result);
            if (2 == result.getRowCount()) {
                System.out.println("The HSQL error MAY have been solved. Consider simplifying this test.");
            }
        }
        else {
            validateRowCount(client, query, 2);
        }

        client.callProcedure("P3.INSERT", 1, 1);
        client.callProcedure("P3.INSERT", 2, 2);
        client.callProcedure("P3.INSERT", 4, 4);
        client.callProcedure("P3.INSERT", 5, 5);
        // P3 1st joined with P2 not null and eliminated by P2.A IS NULL
        // P3 2nd joined with P2 not null and eliminated by P2.A IS NULL
        // P3 3rd joined with P2 null (P2.A < 3)
        // P3 4th joined with P2 null
        query = "select *  FROM P2 RIGHT JOIN P3 " +
                "ON P3.A " + joinOp + " P2.A AND P2.A < 3 WHERE P2.A IS NULL";
        if (isHSQL()) { //// PENDING HSQL flaw investigation
            VoltTable result = client.callProcedure("@AdHoc", query).getResults()[0];
            System.out.println("Ignoring erroneous(?) HSQL baseline: " + result);
            if (2 == result.getRowCount()) {
                System.out.println("The HSQL error MAY have been solved. Consider simplifying this test.");
            }
        }
        else {
            validateRowCount(client, query, 2);
        }
        // Outer table index scan
        // P3 1st eliminated by P3.A > 0 where filter
        // P3 2nd joined with P2 2
        // P3 3nd joined with P2 4
        // R3 4th joined with P2 null
        query = "select * FROM P3 LEFT JOIN P2 " +
                "ON P3.A " + joinOp + " P2.A WHERE P3.A > 1";
        //* enable to debug */ System.out.println(result);
        validateRowCount(client, query, 3);

        // NLJ join of (P2, P2) on a partition column P2.A
        query = "SELECT LHS.A, LHS.E, RHS.A, RHS.E " +
                "FROM P2 LHS LEFT JOIN P2 RHS " +
                "ON LHS.A " + joinOp + " RHS.A AND " +
                "LHS.A < 2 AND RHS.E = 1 " +
                "ORDER BY 1, 2, 3, 4";
        //* enable to debug */ System.out.println(client.callProcedure("@Explain", query).getResults()[0]);
        validateTableOfLongs(client, query, new long[][]{
            {1, 1, 1, 1},
            {2, 2, NULL_VALUE, NULL_VALUE},
            {3, 3, NULL_VALUE, NULL_VALUE},
            {4, 4, NULL_VALUE, NULL_VALUE}
        });

        query = "SELECT LHS.A, LHS.E, RHS.A, RHS.E " +
                "FROM P2 LHS RIGHT JOIN P2 RHS " +
                "ON LHS.A " + joinOp + " RHS.A AND " +
                "LHS.A < 2 AND RHS.E = 1 " +
                "ORDER BY 1, 2, 3, 4";
        //* enable to debug */ System.out.println(client.callProcedure("@Explain", query).getResults()[0]);
        validateTableOfLongs(client, query, new long[][]{
            {NULL_VALUE, NULL_VALUE, 2, 2},
            {NULL_VALUE, NULL_VALUE, 3, 3},
            {NULL_VALUE, NULL_VALUE, 4, 4},
            {1, 1, 1, 1}
        });

        // NLJ join of (P2, P2) on a partition column P1.A
        // and a constant partition key pseudo-filter
        query = "SELECT LHS.A, LHS.E, RHS.A, RHS.E " +
                "FROM P2 LHS LEFT JOIN P2 RHS " +
                "ON LHS.A " + joinOp + " RHS.A AND LHS.A = 1 AND RHS.E = 1 " +
                "ORDER BY 1, 2, 3, 4";
        //* enable to debug */ System.out.println(client.callProcedure("@Explain", query).getResults()[0]);
        validateTableOfLongs(client, query, new long[][]{
            {1, 1, 1, 1},
            {2, 2, NULL_VALUE, NULL_VALUE},
            {3, 3, NULL_VALUE, NULL_VALUE},
            {4, 4, NULL_VALUE, NULL_VALUE}
        });

        // NLJ join of (P2, P2) on a partition column P1.A
        // and a constant partition key pseudo-filter
        query = "SELECT LHS.A, LHS.E, RHS.A, RHS.E " +
                "FROM P2 LHS RIGHT JOIN P2 RHS " +
                "ON LHS.A " + joinOp + " RHS.A AND LHS.A = 1 AND RHS.E = 1 " +
                "ORDER BY 1, 2, 3, 4";
        validateTableOfLongs(client, query, new long[][]{
            {NULL_VALUE, NULL_VALUE, 2, 2},
            {NULL_VALUE, NULL_VALUE, 3, 3},
            {NULL_VALUE, NULL_VALUE, 4, 4},
            {1, 1, 1, 1}
        });

        // NLIJ join of (P2, P3) on partition columns
        query = "SELECT P2.A, P2.E, P3.A, P3.F " +
                "FROM P2 LEFT JOIN P3 " +
                "ON P2.A = P3.A AND P2.A < 2 AND P3.F = 1 " +
                "ORDER BY 1, 2, 3, 4";
        //* enable to debug */ System.out.println(client.callProcedure("@Explain", query).getResults()[0]);
        validateTableOfLongs(client, query, new long[][]{
            {1, 1, 1, 1},
            {2, 2, NULL_VALUE, NULL_VALUE},
            {3, 3, NULL_VALUE, NULL_VALUE},
            {4, 4, NULL_VALUE, NULL_VALUE}
        });

        // NLIJ join of (P2, P3) on partition columns
        query = "SELECT P2.A, P2.E, P3.A, P3.F " +
                "FROM P2 RIGHT JOIN P3 " +
                "ON P2.A = P3.A AND P2.A < 2 AND P3.F = 1 " +
                "ORDER BY 1, 2, 3, 4";
        //* enable to debug */ System.out.println(client.callProcedure("@Explain", query).getResults()[0]);
        validateTableOfLongs(client, query, new long[][]{
            {NULL_VALUE, NULL_VALUE, 2, 2},
            {NULL_VALUE, NULL_VALUE, 4, 4},
            {NULL_VALUE, NULL_VALUE, 5, 5},
            {1, 1, 1, 1},
        });

        // NLIJ join of (P2, P3) on partition columns
        query = "SELECT P2.A, P2.E, P3.A, P3.F " +
                "FROM P2 LEFT JOIN P3 " +
                "ON P2.A = P3.A AND P2.A = 1 AND P3.F = 1 " +
                "ORDER BY 1, 2, 3, 4";
        //* enable to debug */ System.out.println(client.callProcedure("@Explain", query).getResults()[0]);
        validateTableOfLongs(client, query, new long[][]{
            {1, 1, 1, 1},
            {2, 2, NULL_VALUE, NULL_VALUE},
            {3, 3, NULL_VALUE, NULL_VALUE},
            {4, 4, NULL_VALUE, NULL_VALUE}
        });

        // NLIJ join of (P2, P3) on partition columns
        query = "SELECT P2.A, P2.E, P3.A, P3.F " +
                "FROM P2 RIGHT JOIN P3 " +
                "ON P2.A = P3.A AND P2.A = 1 AND P3.F = 1 " +
                "ORDER BY 1, 2, 3, 4";
        //* enable to debug */ System.out.println(client.callProcedure("@Explain", query).getResults()[0]);
        validateTableOfLongs(client, query, new long[][]{
            {NULL_VALUE, NULL_VALUE, 2, 2},
            {NULL_VALUE, NULL_VALUE, 4, 4},
            {NULL_VALUE, NULL_VALUE, 5, 5},
            {1, 1, 1, 1},
        });
}

    /**
     * IN LIST JOIN/WHERE Expressions
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testInListJoin(String joinOp) throws Exception {
        Client client = this.getClient();
        client.callProcedure("R1.INSERT", 1, 1, 1);
        client.callProcedure("R1.INSERT", 2, 2, 2);
        client.callProcedure("R1.INSERT", 3, 3, 3);
        client.callProcedure("R1.INSERT", 4, 4, 4);

        client.callProcedure("R3.INSERT", 1, 1);
        client.callProcedure("R3.INSERT", 2, 2);
        client.callProcedure("R3.INSERT", 4, 4);
        client.callProcedure("R3.INSERT", 5, 5);
        client.callProcedure("R3.INSERT", 6, 6);

        String query;

        // Outer join - IN LIST is outer table join index expression
        query = "SELECT * FROM R3 LEFT JOIN R1 " +
                "ON R3.A " + joinOp + " R1.A AND R3.A IN (1,2)";
        validateRowCount(client, query, 5);
        // Outer join - IN LIST is outer table join non-index expression
        query = "SELECT * FROM R3 LEFT JOIN R1 " +
                "ON R3.A " + joinOp + " R1.A AND R3.C IN (1,2)";
        validateRowCount(client, query, 5);
        // Inner join - IN LIST is join index expression
        query = "SELECT * FROM R3 JOIN R1 " +
                "ON R3.A " + joinOp + " R1.A and R3.A in (1,2)";
        validateRowCount(client, query, 2);
        // Outer join - IN LIST is inner table join index expression
        query = "SELECT * FROM R1 LEFT JOIN R3 " +
                "ON R3.A " + joinOp + " R1.A and R3.A in (1,2)";
        validateRowCount(client, query, 4);
        // Outer join - IN LIST is inner table join scan expression
        query = "SELECT * FROM R1 LEFT JOIN R3 " +
                "ON R3.A " + joinOp + " R1.A and R3.C in (1,2)";
        validateRowCount(client, query, 4);
        // Outer join - IN LIST is outer table where index expression
        query = "SELECT * FROM R3 LEFT JOIN R1 " +
                "ON R3.A " + joinOp + " R1.A WHERE R3.A in (1,2)";
        //* enable to debug */ System.out.println(result);
        validateRowCount(client, query, 2);
        // Outer join - IN LIST is outer table where scan expression
        query = "SELECT * FROM R3 LEFT JOIN R1 " +
                "ON R3.A " + joinOp + " R1.A WHERE R3.C in (1,2)";
        validateRowCount(client, query, 2);
        // Inner join - IN LIST is where index expression
        query = "SELECT * FROM R3 JOIN R1 " +
                "ON R3.A " + joinOp + " R1.A WHERE R3.A in (1,2)";
        validateRowCount(client, query, 2);
        // Inner join - IN LIST is where scan expression
        query = "SELECT * FROM R3 JOIN R1 " +
                "ON R3.A " + joinOp + " R1.A WHERE R3.C in (1,2)";
        validateRowCount(client, query, 2);
        // Outer join - IN LIST is inner table where index expression
        query = "SELECT * FROM R1 LEFT JOIN R3 " +
                "ON R3.A " + joinOp + " R1.A WHERE R3.A in (1,2)";
        validateRowCount(client, query, 2);
    }

    /**
     * Multi table outer join
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testOuterJoin() throws Exception {
        Client client = getClient();
        for (String joinOp : JOIN_OPS) {
            subtestOuterJoinMultiTable(client, joinOp);
            subtestOuterJoinENG8692(client, joinOp);
        }
    }

    private void subtestOuterJoinMultiTable(Client client, String joinOp) throws Exception {
        client.callProcedure("R1.INSERT", 11, 11, 11);
        client.callProcedure("R1.INSERT", 12, 12, 12);
        client.callProcedure("R1.INSERT", 13, 13, 13);

        client.callProcedure("R2.INSERT", 21, 21);
        client.callProcedure("R2.INSERT", 22, 22);
        client.callProcedure("R2.INSERT", 12, 12);

        client.callProcedure("R3.INSERT", 31, 31);
        client.callProcedure("R3.INSERT", 32, 32);
        client.callProcedure("R3.INSERT", 33, 21);

        String query;

        query = "SELECT * FROM R1 RIGHT JOIN R2 " +
                "ON R1.A " + joinOp + " R2.A " +
                "LEFT JOIN R3 " +
                "ON R3.C " + joinOp + " R2.C";
        validateRowCount(client, query, 3);

        query = "SELECT * FROM R1 RIGHT JOIN R2 " +
                "ON R1.A " + joinOp + " R2.A " +
                "LEFT JOIN R3 " +
                "ON R3.C " + joinOp + " R2.C WHERE R1.C > 0";
         validateRowCount(client, query, 1);

         // truncate tables
         client.callProcedure("@AdHoc", "truncate table R1;");
         client.callProcedure("@AdHoc", "truncate table R2;");
         client.callProcedure("@AdHoc", "truncate table R3;");
    }

    private void subtestOuterJoinENG8692(Client client, String joinOp) throws Exception {
        client.callProcedure("@AdHoc", "truncate table t1;");
        client.callProcedure("@AdHoc", "truncate table t2;");
        client.callProcedure("@AdHoc", "truncate table t3;");
        client.callProcedure("@AdHoc", "truncate table t4;");
        client.callProcedure("@AdHoc", "INSERT INTO t1 VALUES(1);");
        client.callProcedure("@AdHoc", "INSERT INTO t2 VALUES(1);");
        client.callProcedure("@AdHoc", "INSERT INTO t3 VALUES(1);");
        client.callProcedure("@AdHoc", "INSERT INTO t4 VALUES(1);");
        client.callProcedure("@AdHoc", "INSERT INTO t4 VALUES(null);");

        String query;

        // case 1: missing join expression
        query = "SELECT * FROM t1 INNER JOIN t2 " +
                "ON t1.i1 " + joinOp + " t2.i2 " +
                "RIGHT OUTER JOIN t3 " +
                "ON t1.i1 = 1000;";
        validateTableOfLongs(client, query, new long[][]{{NULL_VALUE, NULL_VALUE, 1}});

        // case 2: more than 5 table joins
        query = "SELECT * FROM t1 INNER JOIN t2 AS t2_copy1 " +
                "ON t1.i1 " + joinOp + " t2_copy1.i2 " +
                "INNER JOIN t2 AS t2_copy2 " +
                "ON t1.i1 " + joinOp + " t2_copy2.i2 " +
                "INNER JOIN t2 AS t2_copy3 " +
                "ON t1.i1 " + joinOp + " t2_copy3.i2 " +
                "INNER JOIN t2 AS t2_copy4 " +
                "ON t1.i1 " + joinOp + " t2_copy4.i2 " +
                "RIGHT OUTER JOIN t3 " +
                "ON t1.i1 " + joinOp + " t3.i3 AND t3.i3 < -1000;";
        validateTableOfLongs(client, query, new long[][]{{NULL_VALUE, NULL_VALUE, NULL_VALUE, NULL_VALUE, NULL_VALUE, 1}});

        // case 3: reverse scan with null data
        query = "SELECT * FROM t1 INNER JOIN t2 " +
                "ON t1.i1 " + joinOp + " t2.i2 INNER JOIN t4 " +
                "ON t4.i4 < 45;";
        validateTableOfLongs(client, query, new long[][]{{1, 1, 1}});
    }

    public void testFullJoins() throws Exception {
        Client client = getClient();
        truncateTables(client, SEQ_TABLES);
        subtestNonEqualityFullJoin(client);
        truncateTables(client, SEQ_TABLES);
        subtestUsingFullJoin(client);

        for (String joinOp : JOIN_OPS) {
            truncateTables(client, SEQ_TABLES);
            subtestTwoReplicatedTableFullNLJoin(client, joinOp);
            truncateTables(client, INDEXED_TABLES);
            subtestTwoReplicatedTableFullNLIJoin(client, joinOp);
            truncateTables(client, SEQ_TABLES);
            truncateTables(client, INDEXED_TABLES);
            subtestDistributedTableFullJoin(client, joinOp);
            truncateTables(client, SEQ_TABLES);
            subtestLimitOffsetFullNLJoin(client, joinOp);
            truncateTables(client, SEQ_TABLES);
            truncateTables(client, INDEXED_TABLES);
            subtestMultipleFullJoins(client, joinOp);
            truncateTables(client, SEQ_TABLES);
            truncateTables(client, INDEXED_TABLES);
            subtestFullJoinOrderBy(client, joinOp);
        }
    }

    private void subtestTwoReplicatedTableFullNLJoin(Client client, String joinOp) throws Exception {
        String query;

        // case: two empty tables
        query = "SELECT R1.A, R1.D, R2.A, R2.C FROM R1 FULL JOIN R2 " +
                "ON R1.A " + joinOp + " R2.A AND R1.D " + joinOp + " R2.C " +
                "ORDER BY 1, 2, 3, 4";
        validateTableOfLongs(client, query, new long[][]{});

        client.callProcedure("R1.INSERT", 1, 1, null);
        client.callProcedure("R1.INSERT", 1, 2, 2);
        client.callProcedure("R1.INSERT", 2, 1, 1);
        client.callProcedure("R1.INSERT", 2, 4, 4);
        client.callProcedure("R1.INSERT", 3, 3, 3);
        client.callProcedure("R1.INSERT", 4, 4, 4);
        // Delete one row to have non-active tuples in the table
        client.callProcedure("@AdHoc", "DELETE FROM R1 WHERE A = 2 AND C = 4 AND D = 4;");

        // case: Right table is empty
        query = "SELECT R1.A, R1.D, R2.A, R2.C FROM R1 FULL JOIN R2 " +
                "ON R1.A " + joinOp + " R2.A AND R1.D " + joinOp + " R2.C " +
                "ORDER BY 1, 2, 3, 4";
        validateTableOfLongs(client, query, new long[][]{
            {1, NULL_VALUE, NULL_VALUE, NULL_VALUE},
            {1, 2, NULL_VALUE, NULL_VALUE},
            {2, 1, NULL_VALUE, NULL_VALUE},
            {3, 3, NULL_VALUE, NULL_VALUE},
            {4, 4, NULL_VALUE, NULL_VALUE},
        });

        // case: Left table is empty
        query = "SELECT R1.A, R1.D, R2.A, R2.C FROM R2 FULL JOIN R1 " +
                "ON R1.A " + joinOp + " R2.A AND R1.D " + joinOp + " R2.C " +
                "ORDER BY R1.A, R1.D, R2.A, R2.C";
        validateTableOfLongs(client, query, new long[][]{
            {1, NULL_VALUE, NULL_VALUE, NULL_VALUE},
            {1, 2, NULL_VALUE, NULL_VALUE},
            {2, 1, NULL_VALUE, NULL_VALUE},
            {3, 3, NULL_VALUE, NULL_VALUE},
            {4, 4, NULL_VALUE, NULL_VALUE},
        });

        client.callProcedure("R2.INSERT", 1, 1);
        client.callProcedure("R2.INSERT", 2, 1);
        client.callProcedure("R2.INSERT", 2, 2);
        client.callProcedure("R2.INSERT", 3, 3);
        client.callProcedure("R2.INSERT", 4, 4);
        client.callProcedure("R2.INSERT", 5, 5);
        client.callProcedure("R2.INSERT", 5, null);
        // Delete one row to have non-active tuples in the table
        client.callProcedure("@AdHoc", "DELETE FROM R2 WHERE A = 4 AND C = 4;");

        // case 1: equality join on two columns
        query = "SELECT R1.A, R1.D, R2.A, R2.C FROM R1 FULL JOIN R2 " +
                "ON R1.A " + joinOp + " R2.A AND R1.D " + joinOp + " R2.C " +
                "ORDER BY 1, 2, 3, 4";
        validateTableOfLongs(client, query, new long[][]{
            {NULL_VALUE, NULL_VALUE, 1, 1},
            {NULL_VALUE, NULL_VALUE, 2, 2},
            {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
            {NULL_VALUE, NULL_VALUE, 5, 5},
            {1, NULL_VALUE, NULL_VALUE, NULL_VALUE},
            {1, 2, NULL_VALUE, NULL_VALUE},
            {2, 1, 2, 1},
            {3, 3, 3, 3},
            {4, 4, NULL_VALUE, NULL_VALUE}
        });

        // case 2: equality join on two columns plus outer join expression
        query = "SELECT R1.A, R1.D, R2.A, R2.C FROM R1 FULL JOIN R2 " +
                "ON R1.A " + joinOp + " R2.A AND R1.D " + joinOp + " R2.C AND R1.C = 1 " +
                "ORDER BY 1, 2, 3, 4";
        validateTableOfLongs(client, query, new long[][]{
                {NULL_VALUE, NULL_VALUE, 1, 1},
                {NULL_VALUE, NULL_VALUE, 2, 2},
                {NULL_VALUE, NULL_VALUE, 3, 3},
                {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
                {NULL_VALUE, NULL_VALUE, 5, 5},
                {1, NULL_VALUE, NULL_VALUE, NULL_VALUE},
                {1, 2, NULL_VALUE, NULL_VALUE},
                {2, 1, 2, 1},
                {3, 3, NULL_VALUE, NULL_VALUE},
                {4, 4, NULL_VALUE, NULL_VALUE}
                });

        // case 5: equality join on single column
        query = "SELECT R1.A, R1.D, R2.A, R2.C FROM R1 FULL JOIN R2 " +
                "ON R1.A " + joinOp + " R2.A " +
                "ORDER BY 1, 2, 3, 4";
        validateTableOfLongs(client, query, new long[][]{
                {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
                {NULL_VALUE, NULL_VALUE, 5, 5},
                {1, NULL_VALUE, 1, 1},
                {1, 2, 1, 1},
                {2, 1, 2, 1},
                {2, 1, 2, 2},
                {3, 3, 3, 3},
                {4, 4, NULL_VALUE, NULL_VALUE}
                });

        // case 6: equality join on single column and WHERE inner expression
        query = "SELECT R1.A, R1.D, R2.A, R2.C FROM R1 FULL JOIN R2 " +
                "ON R1.A " + joinOp + " R2.A " +
                "WHERE R2.C = 3 OR R2.C IS NULL " +
                "ORDER BY 1, 2, 3, 4";
        validateTableOfLongs(client, query, new long[][]{
            {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
            {3, 3, 3, 3},
            {4, 4, NULL_VALUE, NULL_VALUE}
        });

        query = "SELECT R1.A, R1.D, R2.A, R2.C FROM R1 FULL JOIN R2 " +
                "ON R1.A " + joinOp + " R2.A " +
                "WHERE R1.A = 3 OR R1.A IS NULL " +
                "ORDER BY 1, 2, 3, 4";
        if (isHSQL()) {
            VoltTable result = client.callProcedure("@AdHoc", query).getResults()[0];
            System.out.println("Ignoring erroneous(?) HSQL baseline: " + result);

            // HSQL incorrectly returns
            //   NULL,NULL,1,1
            //   NULL,NULL,2,1
            //   NULL,NULL,2,2
            //   NULL,NULL,5,NULL
            //   NULL,NULL,5,5
            //   3,3,3,3
        }
        else {
            // case 7: equality join on single column and WHERE outer expression

            validateTableOfLongs(client, query, new long[][]{
                {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
                {NULL_VALUE, NULL_VALUE, 5, 5},
                {3, 3, 3, 3}
            });
        }

        // case 8: equality join on single column and WHERE inner-outer expression
        query = "SELECT R1.A, R1.D, R2.A, R2.C FROM R1 FULL JOIN R2 " +
                "ON R1.A " + joinOp + " R2.A " +
                "WHERE R1.A = 3 OR R2.C IS NULL " +
                "ORDER BY 1, 2, 3, 4";
        validateTableOfLongs(client, query, new long[][]{
            {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
            {3, 3, 3, 3},
            {4, 4, NULL_VALUE, NULL_VALUE}
        });
    }

    private void subtestLimitOffsetFullNLJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R1.INSERT", 1, 1, null);
        client.callProcedure("R1.INSERT", 1, 2, 2);
        client.callProcedure("R1.INSERT", 2, 3, 1);
        client.callProcedure("R1.INSERT", 3, 4, 3);
        client.callProcedure("R1.INSERT", 4, 5, 4);

        client.callProcedure("R2.INSERT", 1, 1);
        client.callProcedure("R2.INSERT", 2, 2);
        client.callProcedure("R2.INSERT", 2, 3);
        client.callProcedure("R2.INSERT", 3, 4);
        client.callProcedure("R2.INSERT", 5, 5);
        client.callProcedure("R2.INSERT", 5, 6);

        client.callProcedure("R3.INSERT", 1, 1);
        client.callProcedure("R3.INSERT", 2, 2);
        client.callProcedure("R3.INSERT", 2, 3);
        client.callProcedure("R3.INSERT", 3, 4);
        client.callProcedure("R3.INSERT", 5, 5);
        client.callProcedure("R3.INSERT", 5, 6);

        String query;

        // NLJ SELECT R1.A, R1.C, R2.A, R2.C FROM R1 FULL JOIN R2 ON R1.A " + joinOp + " R2.A
        //  1,1,1,1             outer-inner match
        //  1,2,1,1             outer-inner match
        //  2,3,2,2             outer-inner match
        //  2,3,2,3             outer-inner match
        //  3,4,3,4             outer-inner match
        //  4,5,NULL,NULL       outer no match
        //  NULL,NULL,5,5       inner no match
        //  NULL,NULL,5,6       inner no match
        query = "SELECT R1.A, R1.C, R2.A, R2.C FROM R1 FULL JOIN R2 " +
                "ON R1.A " + joinOp + " R2.A " +
                "ORDER BY R1.A, R2.C LIMIT 2 OFFSET 5";
        validateTableOfLongs(client, query, new long[][]{
            {2, 3, 2, 3},
            {3, 4, 3, 4}
        });

        query = "SELECT R1.A, R1.C, R2.A, R2.C FROM R1 FULL JOIN R2 " +
                "ON R1.A " + joinOp + " R2.A " +
                "ORDER BY R1.A, R2.C LIMIT 2 OFFSET 6";
        validateTableOfLongs(client, query, new long[][]{
            {3, 4, 3, 4},
            {4,5, NULL_VALUE, NULL_VALUE}
        });

        query = "SELECT R1.A, R1.C, R2.A, R2.C FROM R1 FULL JOIN R2 " +
                "ON R1.A " + joinOp + " R2.A " +
                "ORDER BY COALESCE(R1.C, 10), R2.C LIMIT 3 OFFSET 4";
        validateTableOfLongs(client, query, new long[][]{
            {3, 4, 3, 4},
            {4,5, NULL_VALUE, NULL_VALUE},
            {NULL_VALUE, NULL_VALUE, 5, 5}
        });

        query = "SELECT MAX(R1.C), R1.A, R2.A FROM R1 FULL JOIN R2 " +
                "ON R1.A " + joinOp + " R2.A " +
                "GROUP BY R1.A, R2.A LIMIT 2 OFFSET 2";
        validateRowCount(client, query, 2);

        // NLIJ SELECT R1.A, R1.C, R3.A, R3.C FROM R1 FULL JOIN R3 on R1.A " + joinOp + " R3.A
        //  1,1,1,1             outer-inner match
        //  1,2,1,1             outer-inner match
        //  2,3,2,2             outer-inner match
        //  2,3,2,3             outer-inner match
        //  3,4,3,4             outer-inner match
        //  4,5,NULL,NULL       outer no match
        //  NULL,NULL,5,5       inner no match
        //  NULL,NULL,5,6       inner no match

        query = "SELECT R1.A, R1.C, R3.A, R3.C FROM R1 FULL JOIN R3 " +
                "ON R1.A " + joinOp + " R3.A " +
                "ORDER BY COALESCE(R1.A, 10), R3.C LIMIT 2 OFFSET 3";
        validateTableOfLongs(client, query, new long[][]{
            {2, 3, 2, 3},
            {3, 4, 3, 4}
        });

        query = "SELECT R1.A, R1.C, R3.A, R3.C FROM R1 FULL JOIN R3 " +
                "ON R1.A " + joinOp + " R3.A " +
                "ORDER BY R1.A, R3.C LIMIT 2 OFFSET 6";
        validateTableOfLongs(client, query, new long[][]{
            {3, 4, 3, 4},
            {4, 5, NULL_VALUE, NULL_VALUE}
        });

        query = "SELECT R1.A, R1.C, R3.A, R3.C FROM R1 FULL JOIN R3 " +
                "ON R1.A " + joinOp + " R3.A " +
                "ORDER BY COALESCE(R1.A, 10), R3.C LIMIT 3 OFFSET 4";
        validateTableOfLongs(client, query, new long[][]{
            {3, 4, 3, 4L},
            {4,5, NULL_VALUE, NULL_VALUE},
            {NULL_VALUE, NULL_VALUE, 5, 5}
        });

        query = "SELECT MAX(R1.C), R1.A, R3.A FROM R1 FULL JOIN R3 " +
                "ON R1.A " + joinOp + " R3.A " +
                "GROUP BY R1.A, R3.A LIMIT 2 OFFSET 2";
        validateRowCount(client, query, 2);
    }

    private void subtestDistributedTableFullJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("P1.INSERT", 1, 1);
        client.callProcedure("P1.INSERT", 1, 2);
        client.callProcedure("P1.INSERT", 2, 1);
        client.callProcedure("P1.INSERT", 3, 3);
        client.callProcedure("P1.INSERT", 4, 4);

        client.callProcedure("P3.INSERT", 1, 1);
        client.callProcedure("P3.INSERT", 2, 1);
        client.callProcedure("P3.INSERT", 3, 3);
        client.callProcedure("P3.INSERT", 4, 4);

        client.callProcedure("R2.INSERT", 1, 1);
        client.callProcedure("R2.INSERT", 2, 1);
        client.callProcedure("R2.INSERT", 2, 2);
        client.callProcedure("R2.INSERT", 3, 3);
        client.callProcedure("R2.INSERT", 5, 5);
        client.callProcedure("R2.INSERT", 5, null);

        String query;

        // case 1: equality join of (P1, R2) on a partition column P1.A
        query = "SELECT P1.A, P1.C, R2.A, R2.C FROM P1 FULL JOIN R2 " +
                "ON P1.A " + joinOp + " R2.A " +
                "ORDER BY 1, 2, 3, 4";
        validateTableOfLongs(client, query, new long[][]{
            {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
            {NULL_VALUE, NULL_VALUE, 5, 5},
            {1, 1, 1, 1},
            {1, 2, 1, 1},
            {2, 1, 2, 1},
            {2, 1, 2, 2},
            {3, 3, 3, 3},
            {4, 4, NULL_VALUE, NULL_VALUE}
        });

        // case 2: equality join of (P1, R2) on a non-partition column
        query = "SELECT P1.A, P1.C, R2.A, R2.C FROM P1 FULL JOIN R2 " +
                "ON P1.C " + joinOp + " R2.A " +
                "WHERE (P1.A > 1 OR P1.A IS NULL) AND (R2.A = 3 OR R2.A IS NULL) " +
                "ORDER BY 1, 2, 3, 4";
        validateTableOfLongs(client, query, new long[][]{
            {3, 3, 3, 3},
            {4, 4, NULL_VALUE, NULL_VALUE}
        });

        // case 3: NLJ FULL join (R2, P1) on partition column  P1.E " + joinOp + " R2.A AND P1.A > 2 are join predicate
        query = "SELECT P1.A, P1.C, R2.A, R2.C FROM P1 FULL JOIN R2 " +
                "ON P1.C " + joinOp + " R2.A AND P1.A > 2 " +
                "ORDER BY 1, 2, 3, 4";
        validateTableOfLongs(client, query, new long[][]{
            {NULL_VALUE, NULL_VALUE, 1, 1},
            {NULL_VALUE, NULL_VALUE, 2, 1},
            {NULL_VALUE, NULL_VALUE, 2, 2},
            {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
            {NULL_VALUE, NULL_VALUE, 5, 5},
            {1, 1, NULL_VALUE, NULL_VALUE},
            {1, 2, NULL_VALUE, NULL_VALUE},
            {2, 1, NULL_VALUE, NULL_VALUE},
            {3, 3, 3, 3},
            {4, 4, NULL_VALUE, NULL_VALUE}
        });

        // case 4: NLJ FULL join (R2, P1) on partition column  P1.E " + joinOp + " R2.A AND R2.A > 1 are join predicate
        query = "SELECT P1.A, P1.C, R2.A, R2.C FROM P1 FULL JOIN R2 " +
                "ON P1.C " + joinOp + " R2.A AND R2.A > 1 " +
                "ORDER BY 1, 2, 3, 4";
        validateTableOfLongs(client, query, new long[][]{
            {NULL_VALUE, NULL_VALUE, 1, 1},
            {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
            {NULL_VALUE, NULL_VALUE, 5, 5},
            {1, 1, NULL_VALUE, NULL_VALUE},
            {1, 2, 2, 1},
            {1, 2, 2, 2},
            {2, 1, NULL_VALUE, NULL_VALUE},
            {3, 3, 3, 3},
            {4, 4, NULL_VALUE, NULL_VALUE}
        });

        // case 5: equality join of (P3, R2) on a partition/index column P1.A, Still NLJ
        query = "SELECT P3.A, P3.F, R2.A, R2.C FROM P3 FULL JOIN R2 " +
                "ON P3.A " + joinOp + " R2.A " +
                "ORDER BY 1, 2, 3, 4";
        validateTableOfLongs(client, query, new long[][]{
            {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
            {NULL_VALUE, NULL_VALUE, 5, 5},
            {1, 1, 1, 1},
            {2, 1, 2, 1},
            {2, 1, 2, 2},
            {3, 3, 3, 3},
            {4, 4, NULL_VALUE, NULL_VALUE}
        });

        // case 6: NLJ join of (P1, P1) on a partition column P1.A
        query = "SELECT LHS.A, LHS.C, RHS.A, RHS.C " +
                "FROM P1 LHS FULL JOIN P1 RHS " +
                "ON LHS.A " + joinOp + " RHS.A AND " +
                "LHS.A < 2 AND RHS.C = 1 " +
                "ORDER BY 1, 2, 3, 4";
        //* enable to debug */ System.out.println(client.callProcedure("@Explain", query).getResults()[0]);
        validateTableOfLongs(client, query, new long[][]{
            {NULL_VALUE, NULL_VALUE, 1, 2},
            {NULL_VALUE, NULL_VALUE, 2, 1},
            {NULL_VALUE, NULL_VALUE, 3, 3},
            {NULL_VALUE, NULL_VALUE, 4, 4},
            {1, 1, 1, 1},
            {1, 2, 1, 1},
            {2, 1, NULL_VALUE, NULL_VALUE},
            {3, 3, NULL_VALUE, NULL_VALUE},
            {4, 4, NULL_VALUE, NULL_VALUE}
        });

        // case 7: NLJ join of (P1, P1) on a partition column P1.A
        // and a constant partition key pseudo-filter
        query = "SELECT LHS.A, LHS.C, RHS.A, RHS.C " +
                "FROM P1 LHS FULL JOIN P1 RHS " +
                "ON LHS.A " + joinOp + " RHS.A AND " +
                "LHS.A = 1 AND RHS.C = 1 " +
                "ORDER BY 1, 2, 3, 4";
        validateTableOfLongs(client, query, new long[][]{
            {NULL_VALUE, NULL_VALUE, 1, 2},
            {NULL_VALUE, NULL_VALUE, 2, 1},
            {NULL_VALUE, NULL_VALUE, 3, 3},
            {NULL_VALUE, NULL_VALUE, 4, 4},
            {1, 1, 1, 1},
            {1, 2, 1, 1},
            {2, 1, NULL_VALUE, NULL_VALUE},
            {3, 3, NULL_VALUE, NULL_VALUE},
            {4, 4, NULL_VALUE, NULL_VALUE}
        });

        // case 8: NLIJ join of (P1, P3) on partition columns
        query = "SELECT P1.A, P1.C, P3.A, P3.F FROM P1 FULL JOIN P3 " +
                "ON P1.A " + joinOp + " P3.A AND P1.A < 2 AND P3.F = 1 " +
                "ORDER BY 1, 2, 3, 4";
        validateTableOfLongs(client, query, new long[][]{
            {NULL_VALUE, NULL_VALUE, 2, 1},
            {NULL_VALUE, NULL_VALUE, 3, 3},
            {NULL_VALUE, NULL_VALUE, 4, 4},
            {1, 1, 1, 1},
            {1, 2, 1, 1},
            {2, 1, NULL_VALUE, NULL_VALUE},
            {3, 3, NULL_VALUE, NULL_VALUE},
            {4, 4, NULL_VALUE, NULL_VALUE}
        });

        // case 8: NLIJ join of (P1, P3) on partition columns
        query = "SELECT P1.A, P1.C, P3.A, P3.F FROM P1 FULL JOIN P3 " +
                "ON P1.A " + joinOp + " P3.A AND P1.A = 1 AND P3.F = 1 " +
                "ORDER BY 1, 2, 3, 4";
        validateTableOfLongs(client, query, new long[][]{
            {NULL_VALUE, NULL_VALUE, 2, 1},
            {NULL_VALUE, NULL_VALUE, 3, 3},
            {NULL_VALUE, NULL_VALUE, 4, 4},
            {1, 1, 1, 1},
            {1, 2, 1, 1},
            {2, 1, NULL_VALUE, NULL_VALUE},
            {3, 3, NULL_VALUE, NULL_VALUE},
            {4, 4, NULL_VALUE, NULL_VALUE}
        });
    }

    private void subtestTwoReplicatedTableFullNLIJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R1.INSERT", 1, 1, null);
        client.callProcedure("R1.INSERT", 1, 2, 2);
        client.callProcedure("R1.INSERT", 2, 1, 1);
        client.callProcedure("R1.INSERT", 3, 3, 3);
        client.callProcedure("R1.INSERT", 4, 4, 4);

        String query;

        // case 0: Empty FULL NLIJ, inner join R3.A > 0 is added as a post-predicate to the inline Index scan
        query = "SELECT R1.A, R1.C, R3.A, R3.C FROM R1 FULL JOIN R3 " +
                "ON R3.A " + joinOp + " R1.A AND R3.A > 2 " +
                "ORDER BY 1, 2, 3, 4";
        validateTableOfLongs(client, query, new long[][]{
            {1, 1, NULL_VALUE, NULL_VALUE},
            {1, 2, NULL_VALUE, NULL_VALUE},
            {2, 1, NULL_VALUE, NULL_VALUE},
            {3, 3, NULL_VALUE, NULL_VALUE},
            {4, 4, NULL_VALUE, NULL_VALUE},
        });

        client.callProcedure("R3.INSERT", 1, 1);
        client.callProcedure("R3.INSERT", 2, 1);
        client.callProcedure("R3.INSERT", 3, 2);
        client.callProcedure("R3.INSERT", 4, 3);
        client.callProcedure("R3.INSERT", 5, 5);

        // case 1: FULL NLIJ, inner join R3.A > 0 is added as a post-predicate to the inline Index scan
        query = "SELECT R1.A, R1.C, R3.A, R3.C FROM R1 FULL JOIN R3 " +
                "ON R3.A " + joinOp + " R1.A AND R3.A > 2 " +
                "ORDER BY 1, 2, 3, 4";
        validateTableOfLongs(client, query, new long[][]{
            {NULL_VALUE, NULL_VALUE, 1, 1},
            {NULL_VALUE, NULL_VALUE, 2, 1},
            {NULL_VALUE, NULL_VALUE, 5, 5},
            {1, 1, NULL_VALUE, NULL_VALUE},
            {1, 2, NULL_VALUE, NULL_VALUE},
            {2, 1, NULL_VALUE, NULL_VALUE},
            {3, 3, 3, 2},
            {4, 4, 4, 3}
        });

        // case 2: FULL NLIJ, inner join L.A > 0 is added as a pre-predicate to the NLIJ
        query = "SELECT LHS.A, LHS.C, RHS.A, RHS.C FROM R3 LHS FULL JOIN R3 RHS " +
                "ON LHS.A " + joinOp + " RHS.A AND LHS.A > 3 " +
                "ORDER BY 1, 2, 3, 4";
        validateTableOfLongs(client, query, new long[][]{
            {NULL_VALUE, NULL_VALUE, 1, 1},
            {NULL_VALUE, NULL_VALUE, 2, 1},
            {NULL_VALUE, NULL_VALUE, 3, 2},
            {1, 1, NULL_VALUE, NULL_VALUE},
            {2, 1, NULL_VALUE, NULL_VALUE},
            {3, 2, NULL_VALUE, NULL_VALUE},
            {4, 3, 4, 3},
            {5, 5, 5, 5}
        });
    }

    private void subtestNonEqualityFullJoin(Client client) throws Exception {
        client.callProcedure("R1.INSERT", 1, 1, 1);
        client.callProcedure("R1.INSERT", 10, 10, 2);

        client.callProcedure("R2.INSERT", 5, 5);
        client.callProcedure("R2.INSERT", 8, 8);

        client.callProcedure("P2.INSERT", 5, 5);
        client.callProcedure("P2.INSERT", 8, 8);

        String query;

        // case 1: two replicated tables joined on non-equality condition
        query = "SELECT R1.A, R2.A FROM R1 FULL JOIN R2 " +
                "ON R1.A > 15 " +
                "ORDER BY R1.A, R2.A";
        validateTableOfLongs(client, query, new long[][]{
            {NULL_VALUE, 5},
            {NULL_VALUE, 8},
            {1, NULL_VALUE},
            {10, NULL_VALUE}
        });

        // case 2: two replicated tables joined on non-equality inner and outer conditions
        query = "SELECT R1.A, R2.A FROM R1 FULL JOIN R2 " +
                "ON R1.A > 5 AND R2.A < 7 " +
                "ORDER BY R1.A, R2.A";
        validateTableOfLongs(client, query, new long[][]{
            {NULL_VALUE, 8},
            {1, NULL_VALUE},
            {10, 5}
        });

        // case 3: distributed table joined on non-equality inner and outer conditions
        query = "SELECT R1.A, P2.A FROM R1 FULL JOIN P2 " +
                "ON R1.A > 5 AND P2.A < 7 " +
                "ORDER BY R1.A, P2.A";
        validateTableOfLongs(client, query, new long[][]{
            {NULL_VALUE, 8},
            {1, NULL_VALUE},
            {10, 5}
        });
    }

    private void subtestMultipleFullJoins(Client client, String joinOp) throws Exception {
        client.callProcedure("R1.INSERT", 1, 1, 1);
        client.callProcedure("R1.INSERT", 10, 10, 2);

        client.callProcedure("R2.INSERT", 1, 2);
        client.callProcedure("R2.INSERT", 3, 8);

        client.callProcedure("P2.INSERT", 1, 3);
        client.callProcedure("P2.INSERT", 8, 8);

        String query;

        // The R1-R2 FULL join is an inner node in the RIGHT join with P2
        // The P2.A = R2.A join condition is NULL-rejecting for the R2 table
        // simplifying the FULL to be R1 RIGHT JOIN R2 which gets converted to R2 LEFT JOIN R1
        query = "SELECT * FROM R1 FULL JOIN R2 " +
                "ON R1.A " + joinOp + " R2.A " +
                "RIGHT JOIN P2 " +
                "ON P2.A " + joinOp + " R1.A " +
                "ORDER BY P2.A";
        validateTableOfLongs(client, query, new long[][]{
            {1, 1, 1, 1, 2, 1, 3},
            {NULL_VALUE, NULL_VALUE, NULL_VALUE, NULL_VALUE, NULL_VALUE, 8, 8}
        });

        // The R1-R2 FULL join is an outer node in the top LEFT join and is not simplified
        // by the P2.A " + joinOp + " R2.A expression
        query = "SELECT * FROM R1 FULL JOIN R2 " +
                "ON R1.A " + joinOp + " R2.A " +
                "LEFT JOIN P2 " +
                "ON P2.A " + joinOp + " R2.A " +
                "ORDER BY P2.A";
        validateTableOfLongs(client, query, new long[][]{
            {10, 10, 2, NULL_VALUE, NULL_VALUE, NULL_VALUE, NULL_VALUE},
            {NULL_VALUE, NULL_VALUE, NULL_VALUE, 3, 8, NULL_VALUE, NULL_VALUE},
            {1, 1, 1, 1, 2, 1, 3}
        });

        // The R1-R2 RIGHT join is an outer node in the top FULL join and is not simplified
        // by the P2.A = R1.A expression
        query = "SELECT * FROM R1 RIGHT JOIN R2 " +
                "ON R1.A " + joinOp + " R2.A " +
                "FULL JOIN P2 " + "ON R1.A = P2.A " +
                "ORDER BY P2.A";
        validateTableOfLongs(client, query, new long[][]{
            {NULL_VALUE, NULL_VALUE, NULL_VALUE, 3, 8, NULL_VALUE, NULL_VALUE},
            {1, 1, 1, 1, 2, 1, 3},
            {NULL_VALUE, NULL_VALUE, NULL_VALUE, NULL_VALUE, NULL_VALUE, 8, 8}
        });

        // The R1-R2 FULL join is an outer node in the top FULL join and is not simplified
        // by the P2.A " + joinOp + " R1.A expression
        query = "SELECT * FROM R1 FULL JOIN R2 " +
                "ON R1.A " + joinOp + " R2.A " +
                "FULL JOIN P2 " +
                "ON R1.A = P2.A " +
                "ORDER BY P2.A";
        validateTableOfLongs(client, query, new long[][]{
            {10, 10, 2, NULL_VALUE, NULL_VALUE, NULL_VALUE, NULL_VALUE},
            {NULL_VALUE, NULL_VALUE, NULL_VALUE, 3, 8, NULL_VALUE, NULL_VALUE},
            {1, 1, 1, 1, 2, 1, 3},
            {NULL_VALUE, NULL_VALUE, NULL_VALUE, NULL_VALUE, NULL_VALUE, 8, 8}
        });
    }

    private void subtestUsingFullJoin(Client client) throws Exception {
        client.callProcedure("R1.INSERT", 1, 1, null);
        client.callProcedure("R1.INSERT", 1, 2, 2);
        client.callProcedure("R1.INSERT", 2, 1, 1);
        client.callProcedure("R1.INSERT", 3, 3, 3);
        client.callProcedure("R1.INSERT", 4, 4, 4);

        client.callProcedure("R2.INSERT", 1, 3);
        client.callProcedure("R2.INSERT", 3, 8);
        client.callProcedure("R2.INSERT", 5, 8);

        client.callProcedure("R3.INSERT", 1, 3);
        client.callProcedure("R3.INSERT", 6, 8);

        String query;

        query = "SELECT MAX(R1.C), A FROM R1 FULL JOIN R2 USING (A) " +
                "WHERE A > 0 " +
                "GROUP BY A " +
                "ORDER BY A";
        validateTableOfLongs(client, query, new long[][]{
            {2, 1},
            {1, 2},
            {3, 3},
            {4, 4},
            {NULL_VALUE, 5}
        });

        query = "SELECT A FROM R1 FULL JOIN R2 USING (A) FULL JOIN R3 USING(A) " +
                "WHERE A > 0 " +
                "ORDER BY A";
        validateTableOfLongs(client, query, new long[][]{
            {1},
            {1},
            {2},
            {3},
            {4},
            {5},
            {6}
        });
    }

    private void subtestFullJoinOrderBy(Client client, String joinOp) throws Exception {
        client.callProcedure("R3.INSERT", 1, null);
        client.callProcedure("R3.INSERT", 1, 1);
        client.callProcedure("R3.INSERT", 2, 2);
        client.callProcedure("R3.INSERT", 2, 3);
        client.callProcedure("R3.INSERT", 3, 1);

        String query;

        query = "SELECT L.A FROM R3 L FULL JOIN R3 R " +
                "ON L.C " + joinOp + " R.C " +
                "ORDER BY A";
        long[][] toExpect;
        if (joinOp.equals("=")) {
            toExpect = new long[][]{
                {NULL_VALUE},
                {1},
                {1},
                {1},
                {2},
                {2},
                {3},
                {3}
            };
        }
        else {
            toExpect = new long[][]{
                // Accepting NULL values in L.C IS NOT DISTINCT FROM R.C
                // eliminates one left-padded row with a null L.A and
                // substitutes a match row with value L.A = 1 indistinguishable
                // here from the right-padded row with L.A = 1 it replaces.
                // {NULL_VALUE},
                {1},
                {1},
                {1},
                {2},
                {2},
                {3},
                {3}
            };
        }
        validateTableOfLongs(client, query, toExpect);

        query = "SELECT L.A, SUM(L.C) FROM R3 L FULL JOIN R3 R " +
                "ON L.C " + joinOp + " R.C " +
                "GROUP BY L.A " +
                "ORDER BY 1";
        if (joinOp.equals("=")) {
            toExpect = new long[][]{
                {NULL_VALUE, NULL_VALUE},
                {1, 2},
                {2, 5},
                {3, 2}
            };
        }
        else {
            toExpect = new long[][]{
                // Accepting NULL values in L.C IS NOT DISTINCT FROM R.C
                // eliminates null pad rows and adds a match row with L.C = null
                // that has no effect on the sums.
                // {NULL_VALUE, NULL_VALUE},
                {1, 2},
                {2, 5},
                {3, 2}
            };
        }
        validateTableOfLongs(client, query, toExpect);
    }

    public void testEng13603() throws Exception {
        Client client = getClient();

        assertSuccessfulDML(client,"insert into T1_ENG_13603 values (null, 1.0, 1);");
        assertSuccessfulDML(client,"insert into T1_ENG_13603 values (0, null, null);");
        assertSuccessfulDML(client,"insert into T2_ENG_13603 values (null);");
        assertSuccessfulDML(client,"insert into T2_ENG_13603 values (0);");

        String query = "select ratio, count(*) "
                + "from T1_ENG_13603 T1 full outer join T2_ENG_13603 T2 on T1.id = T2.id "
                + "group by T1.ratio "
                + "order by T1.ratio;";
        ClientResponse cr = client.callProcedure("@AdHoc", query);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        System.out.println(cr.getResults()[0]);
        assertContentOfTable(new Object[][] {{null, 2}, {1.0, 1}}, cr.getResults()[0]);
    }

    public void testEng15030() throws IOException, ProcCallException {
        Client client = getClient();
        final String bigJoin = "SELECT rowkeys.value as row_key, " +
                " T1.value as T1_value, " +
                "T2.value as T2_value, " +
                "T3.value as T3_value, " +
                "T4.value as T4_value, " +
                "T5.value as T5_value, " +
                "T6.value as T6_value, " +
                "T7.value as T7_value, " +
                "T8.value as T8_value, " +
                "T9.value as T9_value, " +
                "T10.value as T10_value, " +
                "T11.value as T11_value, " +
                "T12.value as T12_value, " +
                "T13.value as T13_value, " +
                "T14.value as T14_value, " +
                "T15.value as T15_value, " +
                "T16.value as T16_value, " +
                "T17.value as T17_value, " +
                "T18.value as T18_value, " +
                "T19.value as T19_value, " +
                "T20.value as T20_value " +
                "FROM rowkeys " +
                "LEFT JOIN float_cells T1 ON rowkeys.part = T1.part AND rowkeys.value = T1.row_key AND T1.attribute_id = 2889597802680156167 " +
                "LEFT JOIN float_cells T2 ON rowkeys.part = T2.part AND rowkeys.value = T2.row_key AND T2.attribute_id = 2889597802688544775 " +
                "LEFT JOIN float_cells T3 ON rowkeys.part = T3.part AND rowkeys.value = T3.row_key AND T3.attribute_id = 2889597802696933383 " +
                "LEFT JOIN float_cells T4 ON rowkeys.part = T4.part AND rowkeys.value = T4.row_key AND T4.attribute_id = 2889597802696949767 " +
                "LEFT JOIN float_cells T5 ON rowkeys.part = T5.part AND rowkeys.value = T5.row_key AND T5.attribute_id = 2889597802705321991 " +
                "LEFT JOIN float_cells T6 ON rowkeys.part = T6.part AND rowkeys.value = T6.row_key AND T6.attribute_id = 2889597802713710599 " +
                "LEFT JOIN float_cells T7 ON rowkeys.part = T7.part AND rowkeys.value = T7.row_key AND T7.attribute_id = 2889597802713726983 " +
                "LEFT JOIN float_cells T8 ON rowkeys.part = T8.part AND rowkeys.value = T8.row_key AND T8.attribute_id = 2889597802722099207 " +
                "LEFT JOIN float_cells T9 ON rowkeys.part = T9.part AND rowkeys.value = T9.row_key AND T9.attribute_id = 2889597802730487815 " +
                "LEFT JOIN float_cells T10 ON rowkeys.part = T10.part AND rowkeys.value = T10.row_key AND T10.attribute_id = 2889597802755653639 " +
                "LEFT JOIN float_cells T11 ON rowkeys.part = T11.part AND rowkeys.value = T11.row_key AND T11.attribute_id = 2889597802680156167 " +
                "LEFT JOIN float_cells T12 ON rowkeys.part = T12.part AND rowkeys.value = T12.row_key AND T12.attribute_id = 2889597802688544785 " +
                "LEFT JOIN float_cells T13 ON rowkeys.part = T13.part AND rowkeys.value = T13.row_key AND T13.attribute_id = 2889597802696933393 " +
                "LEFT JOIN float_cells T14 ON rowkeys.part = T14.part AND rowkeys.value = T14.row_key AND T14.attribute_id = 2889597802696949777 " +
                "LEFT JOIN float_cells T15 ON rowkeys.part = T15.part AND rowkeys.value = T15.row_key AND T15.attribute_id = 2889597802705322001 " +
                "LEFT JOIN float_cells T16 ON rowkeys.part = T16.part AND rowkeys.value = T16.row_key AND T16.attribute_id = 2889597802713710609 " +
                "LEFT JOIN float_cells T17 ON rowkeys.part = T17.part AND rowkeys.value = T17.row_key AND T17.attribute_id = 2889597802713726993 " +
                "LEFT JOIN float_cells T18 ON rowkeys.part = T18.part AND rowkeys.value = T18.row_key AND T18.attribute_id = 2889597802722099217 " +
                "LEFT JOIN float_cells T19 ON rowkeys.part = T19.part AND rowkeys.value = T19.row_key AND T19.attribute_id = 2889597802730487825 " +
                "LEFT JOIN float_cells T20 ON rowkeys.part = T20.part AND rowkeys.value = T20.row_key AND T20.attribute_id = 2889597802755653649 " +
                "WHERE " +
                "rowkeys.dataset_id = 2889597788000092160 AND rowkeys.part = 3297";
        final ClientResponse cr = client.callProcedure("@Explain", bigJoin);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        assertTrue("Expect that the plan contains at least 5 index scans, got fewer",
                Arrays.stream(cr.getResults()[0].toString().split("\n"))
                        .filter(line -> line.contains("INDEX SCAN of")).count() >= 5);
    }

    static public junit.framework.Test suite() {
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestJoinsSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestJoinsSuite.class.getResource("testjoins-ddl.sql"));
//        try {
//          project.addLiteralSchema("CREATE PROCEDURE R4_INSERT AS INSERT INTO R4 VALUES(?, ?);");
//      }
//        catch (IOException e) {
//          e.printStackTrace();
//          fail();
//      }

        LocalCluster config;

        config = new LocalCluster("testjoin-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        assertTrue(config.compile(project));
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("testjoin-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        assertTrue(config.compile(project));
        builder.addServerConfig(config);

        // HSQLDB
        config = new LocalCluster("testjoin-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        assertTrue(config.compile(project));
        builder.addServerConfig(config);

        return builder;
    }
}

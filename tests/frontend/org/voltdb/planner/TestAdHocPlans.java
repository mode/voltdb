/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.io.File;
import java.io.IOException;

import org.voltdb.AdHocQueryTester;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Catalog;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.AdHocPlannedStatement;
import org.voltdb.compiler.PlannerTool;
import org.voltdb.settings.ClusterSettings;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.base.Supplier;

public class TestAdHocPlans extends AdHocQueryTester {

    private PlannerTool m_pt;
    private boolean m_debugging_set_this_to_retry_failures = false;

    @Override
    protected void setUp() throws Exception {
        // For planner-only testing, we shouldn't care about IV2
        VoltDB.Configuration config = setUpSPDB();
        byte[] bytes = MiscUtils.fileToBytes(new File(config.m_pathToCatalog));
        String serializedCatalog = CatalogUtil.getSerializedCatalogStringFromJar(CatalogUtil.loadAndUpgradeCatalogFromJar(bytes).getFirst());
        Catalog catalog = new Catalog();
        catalog.execute(serializedCatalog);
        Supplier<ClusterSettings> settings = config.asClusterSettings().asSupplier();
        CatalogContext context = new CatalogContext(0, 0, catalog, settings, bytes, null, new byte[] {}, 0);
        m_pt = new PlannerTool(context.database, context.getCatalogHash());
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testSP() throws Exception {
        //DB is empty so the hashable numbers don't really seem to matter
        runAllAdHocSPtests(0, 1, 2, 3);
    }

    public void testAdHocQueryForStackOverFlowCondition() throws NoConnectionsException, IOException, ProcCallException {
        // query with max predicates in where clause
        String sql = getQueryForLongQueryTable(300);
        runQueryTest(sql, 1, 1, 1, VALIDATING_SP_RESULT);

        // generate query with lots of predicate to simulate stack overflow when parsing the expression
        try {
            for (int numberPredicates = 2000; numberPredicates < 100000; numberPredicates += 1000) {
                sql = getQueryForLongQueryTable(numberPredicates);
                runQueryTest(sql, 1, 1, 1, VALIDATING_SP_RESULT);
            }
            fail("Query was expected to generate stack over flow error");
        }
        catch (StackOverflowError error) {
            // The test-only interface to the PlannerTool tests at a level below
            // any StackOverflowError handling, so expect the raw StackOverflowError.
        }
    }

    /**
     * For planner-only testing, most of the args are ignored.
     */
    @Override
    public int runQueryTest(String query, int hash, int spPartialSoFar,
            int expected, int validatingSPresult) throws IOException,
            NoConnectionsException, ProcCallException {
        AdHocPlannedStatement result = m_pt.planSqlForTest(query);
        boolean spPlan = result.toString().contains("ALL: null");
        if ((validatingSPresult == VALIDATING_SP_RESULT) != spPlan) {
            System.out.println("Missed: "+ query);
            System.out.println(result);
            // This is a good place for a breakpoint,
            // to set this debugging flag and step into the planner,
            // for a do-over after getting an unexpected result.
            if (m_debugging_set_this_to_retry_failures) {
                result = m_pt.planSqlForTest(query);
            }
        }
        assertEquals((validatingSPresult == VALIDATING_SP_RESULT), spPlan);
        return 0;
    }

}

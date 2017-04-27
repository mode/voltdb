/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.sysprocs;

import java.util.List;
import java.util.Map;

import org.json_voltpatches.JSONStringer;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.ReplicationRole;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltZK;

@ProcInfo(singlePartition = false)
public class Promote extends VoltSystemProcedure {

    @Override
    public void init() {}

    @Override
    public DependencyPair executePlanFragment(Map<Integer, List<VoltTable>> dependencies,
                                              long fragmentId,
                                              ParameterSet params,
                                              SystemProcedureExecutionContext context) {
        throw new RuntimeException("@Promote was given an " +
                                   "invalid fragment id: " + String.valueOf(fragmentId));
    }

    /**
     * Switch to master mode
     * @param ctx       Internal parameter. Not user-accessible.
     * @return          Standard STATUS table.
     */
    public VoltTable[] run(SystemProcedureExecutionContext ctx) throws Exception
    {
        // Choose the lowest site ID on this host to actually flip the bit
        if (ctx.isLowestSiteId()) {
            JSONStringer js = new JSONStringer();
            js.object();
            js.key("role").value(ReplicationRole.NONE.ordinal());
            // Replication active state should the be same across the cluster
            js.key("active").value(VoltDB.instance().getReplicationActive());
            js.endObject();

            VoltDB.instance().getHostMessenger().getZK().setData(
                    VoltZK.replicationconfig,
                    js.toString().getBytes("UTF-8"),
                    -1);
            VoltDB.instance().setReplicationRole(ReplicationRole.NONE);
        }

        VoltTable t = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
        t.addRow(VoltSystemProcedure.STATUS_OK);
        return (new VoltTable[] {t});
    }
}
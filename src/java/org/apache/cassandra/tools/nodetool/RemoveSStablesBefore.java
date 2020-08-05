/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.tools.nodetool;

import com.google.common.collect.Lists;
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.utils.Pair;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "removesstablesbefore", description = "Remove outdated SSTables before specific timestamp without restart")
public class RemoveSStablesBefore extends NodeTool.NodeToolCmd
{
    @Arguments(usage = "<keyspace> <table> <minTimestamp> <windowOffset>...", description = "The keyspace, table name, sstables from minTimestamp(in millis)  till window offset will be removed")
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() >= 4, "removesstables requires ks, cf, minTimestamp(in millis) and time window offset args");
        List<String> srcPaths = Lists.newArrayList(args.subList(4, args.size()));
        long minTimestamp = Long.parseLong(args.get(2));
        int windowOffset = Integer.parseInt(args.get(3));
        boolean execRemove = false;
        if (args.size() > 4 && args.get(4).equalsIgnoreCase("-e"))
            execRemove = true;

        String ret =
                probe.removeSSTablesBefore(args.get(0), args.get(1), minTimestamp, windowOffset, execRemove);

        System.out.println(ret);
    }
}

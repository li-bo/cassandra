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
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "removesstables", description = "Remove outdated SSTables without restart")
public class RemoveSStables extends NodeTool.NodeToolCmd
{
    @Arguments(usage = "<keyspace> <table> <*Data.db file name> ...", description = "The keyspace, table name and files to be deleted")
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() >= 3, "removesstables requires ks, cf and filenames args");
        List<String> srcPaths = Lists.newArrayList(args.subList(2, args.size()));
        List<String> failedDirs = probe.removeSStables(args.get(0), args.get(1), new HashSet<>(srcPaths));
        if (!failedDirs.isEmpty())
        {
            System.err.println("Some directories failed to import, check server logs for details:");
            for (String directory : failedDirs)
                System.err.println(directory);
            System.exit(1);
        }
    }
}

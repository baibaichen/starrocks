// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.thrift;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ThriftPlanTest {

    private static final Logger LOG = LogManager.getLogger(ThriftPlanTest.class);

    public static void main(String[] args) throws IOException, TException {
        String file = "/home/chang/test/20221208223008.053.json";
        TExecPlanFragmentParams planFragmentParams = new TExecPlanFragmentParams();

        readTo(file, planFragmentParams);

        TScanRangeParams x = planFragmentParams.params.per_node_scan_ranges.get(0).get(0);
        List<TScanRangeParams> y = listFilesUsingJavaIO("/home/chang/test/hdfs/hdfs_container/tpch100full/lineitem", x);
        planFragmentParams.params.per_node_scan_ranges.get(0).clear();
        planFragmentParams.params.per_node_scan_ranges.get(0).addAll(y);
        LOG.info(planFragmentParams.toString());

        // String fileName = new SimpleDateFormat("yyyyMMddHHmmss.SSS").format(new Date());
        TSerializer json = new TSerializer(new TJSONProtocol.Factory());
        String jsonString = json.toString(planFragmentParams, "UTF-8");
        try {
            FileUtils.writeStringToFile(new
                    File("/home/chang/test/20221208223008.053.new.json"), jsonString, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<TScanRangeParams> listFilesUsingJavaIO(String dir, TScanRangeParams copied) {
        return Stream.of(Objects.requireNonNull(new File(dir).listFiles()))
                .filter(file -> !file.isDirectory())
                .limit(10)
                .map(file -> {
                    TScanRangeParams cloned = new TScanRangeParams(copied);
                    cloned.scan_range.hdfs_scan_range.setRelative_path(file.getName());
                    cloned.scan_range.hdfs_scan_range.setFile_length(file.length());
                    cloned.scan_range.hdfs_scan_range.setLength(file.length());
                    return cloned;
                })
                .collect(Collectors.toList());
    }

    private static TProtocol protocol(InputStream from) throws TTransportException {
        return protocol(new TIOStreamTransport(from));
    }
    private static TProtocol protocol(TIOStreamTransport t) {
        return new TJSONProtocol(t);
    }
    private static <T extends TBase<?, ?>> void readTo(final InputStream input, T tbase) throws IOException  {
        try {
            tbase.read(protocol(input));
        } catch (TException e) {
            throw new IOException("can not read " + tbase.getClass() + ": " + e.getMessage(), e);
        }
    }

    private static <T extends TBase<?, ?>> void readTo(final String file, T tBase) throws IOException  {
        try (InputStream targetStream = Files.newInputStream(Paths.get(file))) {
            readTo(targetStream, tBase);
        }
    }
}
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

package org.apache.flink.test.csv;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.PrintWriter;

public class ResultSink<T> extends RichSinkFunction<T> {

    private final String filename;

    private transient PrintWriter writer;

    public ResultSink(String filename) {
        this.filename = filename;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        writer = new PrintWriter("results/" + filename);
    }

    @Override
    public void invoke(Object value, Context context) throws Exception {
        writer.println(value.toString());
    }

    @Override
    public void close() throws Exception {
        super.close();
        writer.flush();
        writer.close();
    }
}

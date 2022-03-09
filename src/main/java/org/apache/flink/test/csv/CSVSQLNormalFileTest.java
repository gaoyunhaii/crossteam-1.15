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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class CSVSQLNormalFileTest {

    public static void main(String[] args) {
        // #{id}@#{first}@#{last}@#{age}@#{salary}@#{likecookies}@#{happyday}@#{array_data}
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tableEnv.executeSql(
                "CREATE TABLE some_source (\n"
                        + "  id BIGINT,\n"
                        + "  first STRING,\n"
                        + "  `second` STRING,\n"
                        + "  age INT,\n"
                        + "  salary DECIMAL(10, 1),\n"
                        + "  likeCookies BOOLEAN,\n"
                        + "  happyDay DATE,\n"
                        + "  array_data ARRAY<INT>\n"
                        + ") WITH (\n"
                        + " 'connector' = 'filesystem',\n"
                        + " 'path' = 'file:////Users/owner-pc/workspace_commu/crossteams/crossteam-1.15/csv/ds_complex_pojo.csv',\n"
                        + " 'format' = 'csv',\n"
                        + " 'csv.field-delimiter' = '@',\n"
                        + " 'csv.array-element-delimiter' = '#'\n"
                        + ")");

        tableEnv.executeSql(
                "CREATE TABLE some_sink (\n"
                        + "  id BIGINT,\n"
                        + "  first STRING,\n"
                        + "  `second` STRING,\n"
                        + "  age INT,\n"
                        + "  salary DECIMAL(10, 1),\n"
                        + "  likeCookies BOOLEAN,\n"
                        + "  happyDay DATE,\n"
                        + "  array_data ARRAY<INT>\n"
                        + ") WITH (\n"
                        + " 'connector' = 'filesystem',\n"
                        + " 'path' = 'file:////Users/owner-pc/workspace_commu/crossteams/crossteam-1.15/results/sql',\n"
                        + " 'format' = 'csv',\n"
                        + " 'csv.field-delimiter' = '@',\n"
                        + " 'csv.array-element-delimiter' = '#',\n"
                        + " 'csv.disable-quote-character' = 'true'\n"
                        + ")");

        tableEnv.executeSql("insert into some_sink select * from some_source").print();
    }
}

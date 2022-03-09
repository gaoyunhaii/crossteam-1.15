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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.io.File;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Arrays;

public class CSVDataStreamPojoTest {

    @JsonPropertyOrder({"id", "first", "last", "age", "salary", "likeCookies", "happyDay"})
    public static class MyPojo {
        private long id;
        private String first;
        private String last;
        private int age;
        private float salary;
        private boolean likeCookies;
        private Date happyDay;

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getFirst() {
            return first;
        }

        public void setFirst(String first) {
            this.first = first;
        }

        public String getLast() {
            return last;
        }

        public void setLast(String last) {
            this.last = last;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public float getSalary() {
            return salary;
        }

        public void setSalary(float salary) {
            this.salary = salary;
        }

        public boolean isLikeCookies() {
            return likeCookies;
        }

        public void setLikeCookies(boolean likeCookies) {
            this.likeCookies = likeCookies;
        }

        public Date getHappyDay() {
            return happyDay;
        }

        public void setHappyDay(Date happyDay) {
            this.happyDay = happyDay;
        }

        @Override
        public String toString() {
            return String.format(
                    "%d,%s,%s,%d,%.1f,%s,%s",
                    id,
                    first,
                    last,
                    age,
                    salary,
                    likeCookies,
                    new SimpleDateFormat("yyyy-MM-dd").format(happyDay));
        }
    }

    public static void main(String[] args) throws Exception {
        CsvReaderFormat<MyPojo> csvFormat = CsvReaderFormat.forPojo(MyPojo.class);
        FileSource<MyPojo> source =
                FileSource.forRecordStreamFormat(
                                csvFormat,
                                Path.fromLocalFile(
                                        new File(
                                                "/Users/owner-pc/workspace_commu/crossteams/crossteam-1.15/csv/ds_pojo.csv")))
                        .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "csv source")
                .addSink(new ResultSink<>("r_ds_pojo.csv"));
        env.execute();
    }
}

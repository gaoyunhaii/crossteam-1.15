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

import net.andreinc.mockneat.MockNeat;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

public class CsvGenerator {

    private static void generateDataStreamPojo() {
        MockNeat m = MockNeat.threadLocal();
        final Path path = Paths.get("./csv/ds_pojo.csv");

        m.fmt("#{id},#{first},#{last},#{age},#{salary},#{likecookies},#{happyday}")
                .param("id", m.longSeq())
                .param("first", m.names().first())
                .param("last", m.names().last())
                .param("age", m.ints().range(20, 60))
                .param(
                        "salary",
                        m.floats().range(1000, 10000000).map(x -> String.format("%.1f", x)))
                .param("likecookies", m.bools().probability(0.5))
                .param(
                        "happyday",
                        m.localDates()
                                .map(d -> d.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))))
                .list(1000)
                .consume(
                        list -> {
                            try {
                                List<String> lines = new ArrayList<>();
                                // lines.add("id,first,last,age,salary,likeCookies,happyDay");
                                lines.addAll(list);
                                Files.write(path, lines, CREATE, TRUNCATE_EXISTING, WRITE);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
    }

    private static void generateDataStreamComplexPojo() {
        MockNeat m = MockNeat.threadLocal();
        final Path path = Paths.get("./csv/ds_complex_pojo.csv");

        m.fmt("#{id}@#{first}@#{last}@#{age}@#{salary}@#{likecookies}@#{happyday}@#{array}")
                .param("id", m.longSeq())
                .param("first", m.names().first())
                .param("last", m.names().last())
                .param("age", m.ints().range(20, 60))
                .param(
                        "salary",
                        m.floats().range(1000, 10000000).map(x -> String.format("%.1f", x)))
                .param("likecookies", m.bools().probability(0.5))
                .param(
                        "happyday",
                        m.localDates()
                                .map(d -> d.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))))
                .param(
                        "array",
                        m.ints()
                                .range(1, 6)
                                .map(
                                        x ->
                                                StringUtils.joinWith(
                                                        "#",
                                                        IntStream.range(1, x).boxed().toArray())))
                .list(1000)
                .consume(
                        list -> {
                            try {
                                List<String> lines = new ArrayList<>();
                                // lines.add("id,first,last,age,salary,likeCookies,happyDay");
                                lines.addAll(list);
                                Files.write(path, lines, CREATE, TRUNCATE_EXISTING, WRITE);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
    }

    public static void main(String[] args) {
        generateDataStreamPojo();
        generateDataStreamComplexPojo();
    }
}

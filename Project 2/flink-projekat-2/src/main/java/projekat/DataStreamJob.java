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

package projekat;

import models.Location;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;
import java.util.Properties;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        String inputTopic = "locations";
        String server = "kafka:9092"; //""localhost:9091"; //kafka:9092

        DataStream<String> dataStream = StreamConsumer(inputTopic, server, environment);

        DataStream<Location> locationStream = ConvertStreamFromJsonToLocationType(dataStream);
        //locationStream.print();

        SingleOutputStreamOperator windowedStream = locationStream
                .keyBy(Location::getUser)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(10)))
                .aggregate(new AverageAggregate());

        SingleOutputStreamOperator windowedStream2 = locationStream
                .keyBy(Location::getUser)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new MyProcessWindowFunction());
                //.aggregate(new AverageAggregate(), new MyProcessWindowFunction() );

        //windowedStream.print();
        //windowedStream2.print();

        CassandraService cassandraService = new CassandraService();
        cassandraService.sinkToCassandraDB(windowedStream);


        environment.execute();

    }

    public static DataStream<Location> ConvertStreamFromJsonToLocationType(DataStream<String> jsonStream) {
        return jsonStream.map(kafkaMessage -> {
            try {
                JsonNode jsonNode = new ObjectMapper().readValue(kafkaMessage, JsonNode.class);
                return Location.builder()
                        .Latitude(jsonNode.get("lat").asDouble())
                        .Longitude(jsonNode.get("lon").asDouble())
                        .Altitude(jsonNode.get("alt").asDouble())
                        .Label(jsonNode.get("label").asText())
                        .User(jsonNode.get("user").asText())
                        .Year(jsonNode.get("year").asInt())
                        .Month(jsonNode.get("month").asInt())
                        .Day(jsonNode.get("day").asInt())
                        .Hour(jsonNode.get("hour").asInt())
                        .Minute(jsonNode.get("minute").asInt())
                        .Second(jsonNode.get("second").asInt())
                        .Timestamp(jsonNode.get("ts").asInt())
                        .build();

            } catch (Exception e) {
                return null;
            }
        }).filter(Objects::nonNull).forward();
    }


    public static DataStream<String> StreamConsumer(String inputTopic, String server, StreamExecutionEnvironment environment) throws Exception {
        FlinkKafkaConsumer<String> flinkKafkaConsumer = createStringConsumerForTopic(inputTopic, server);
        DataStream<String> stringInputStream = environment.addSource(flinkKafkaConsumer);


        return stringInputStream.map(new MapFunction<String, String>() {
            private static final long serialVersionUID = -999736771747691234L;

            @Override
            public String map(String value) throws Exception {
                return value;
            }
        });
    }

    public static FlinkKafkaConsumer<String> createStringConsumerForTopic(
            String topic, String kafkaAddress) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        //props.setProperty("group.id",kafkaGroup);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), props);

        return consumer;
    }


    //		// Sets up the execution environment, which is the main entry point
//		// to building Flink applications.
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//		/*
//		 * Here, you can start creating your execution plan for Flink.
//		 *
//		 * Start with getting some data from the environment, like
//		 * 	env.fromSequence(1, 10);
//		 *
//		 * then, transform the resulting DataStream<Long> using operations
//		 * like
//		 * 	.filter()
//		 * 	.flatMap()
//		 * 	.window()
//		 * 	.process()
//		 *
//		 * and many more.
//		 * Have a look at the programming guide:
//		 *
//		 * https://nightlies.apache.org/flink/flink-docs-stable/
//		 *
//		 */
//
//		// Execute program, beginning computation.
//		env.execute("Flink Java API Skeleton");
}




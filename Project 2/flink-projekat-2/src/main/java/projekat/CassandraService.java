package projekat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.log4j.Logger;

/**
 * CassandraService is a class that sinks DataStream into CassandraDB.
 */
public final class   CassandraService {

    private static final Logger LOGGER = Logger.getLogger(DataStreamJob.class);

    /**
     * Creating environment for Cassandra and sink some Data of car stream into CassandraDB
     *
     * @param sinkCarStream  DataStream of type Car.
     */
    public final void sinkToCassandraDB(final DataStream<Tuple5<String, Double, Double, Double, Double>> sinkCarStream) throws Exception {

        LOGGER.info("Creating car data to sink into cassandraDB.");
        SingleOutputStreamOperator<Tuple5<String, String, String, String, String>> sinkCarDataStream = sinkCarStream.map((MapFunction<Tuple5<String, Double, Double, Double, Double>, Tuple5<String, String, String, String, String>>) car ->
                        new Tuple5<>(car.f0, Double.toString(car.f1), Double.toString(car.f2), Double.toString(car.f3), Double.toString(car.f4)))
                .returns(new TupleTypeInfo<>(TypeInformation.of(String.class), TypeInformation.of(String.class), TypeInformation.of(String.class), TypeInformation.of(String.class), TypeInformation.of(String.class)));

        sinkCarDataStream.print();
        LOGGER.info("Open Cassandra connection and Sinking car data into cassandraDB.");
//        CassandraSink.addSink(sinkCarDataStream)
//                .setQuery("INSERT INTO locations_db.flinkgeolocations(user, minvalue, maxvalue, meanvalue, count) values (?, ?, ?, ?, ?);")
//                .setHost("cassandra:9042")
//                .build();

    }
}
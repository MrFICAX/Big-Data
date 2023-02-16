package projekat;
import com.datastax.driver.core.Cluster;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.log4j.Logger;

/**
 * CassandraService is a class that sinks DataStream into CassandraDB.
 */
public final class   CassandraService {

    private static final Logger LOGGER = Logger.getLogger(DataStreamJob.class);

    /**
     * Creating environment for Cassandra and sink some Data of car stream into CassandraDB
     *
     * @param sinkFilteredLocationStream  DataStream of type Car.
     */
    public final void sinkToCassandraDB(final DataStream<Tuple5<String, Double, Double, Double, Double>> sinkFilteredLocationStream) throws Exception {

        LOGGER.info("Creating car data to sink into cassandraDB.");
        SingleOutputStreamOperator<Tuple5<String, String, String, String, String>> sinkLocationStream = sinkFilteredLocationStream.map((MapFunction<Tuple5<String, Double, Double, Double, Double>, Tuple5<String, String, String, String, String>>) filteredData ->
                        new Tuple5<>(filteredData.f0, Double.toString(filteredData.f1), Double.toString(filteredData.f2), Double.toString(filteredData.f3), Double.toString(filteredData.f4)))
                .returns(new TupleTypeInfo<>(TypeInformation.of(String.class), TypeInformation.of(String.class), TypeInformation.of(String.class), TypeInformation.of(String.class), TypeInformation.of(String.class)));

        sinkLocationStream.print();
        LOGGER.info("Open Cassandra connection and Sinking car data into cassandraDB.");
        CassandraSink
                .addSink(sinkLocationStream)
//                .setClusterBuilder(new ClusterBuilder() {
//                    @Override
//                    protected Cluster buildCluster(Cluster.Builder builder) {
//                        return builder
//                                .addContactPoint("cassandra")
//                                .withPort(9042)
//                                .build();
//                    }
//                })
                .setHost("cassandra")
                .setQuery("INSERT INTO locations_db.flinkgeolocation(user, minvalue, maxvalue, meanvalue, count) values (?, ?, ?, ?, ?);")
                .build();

    }
}
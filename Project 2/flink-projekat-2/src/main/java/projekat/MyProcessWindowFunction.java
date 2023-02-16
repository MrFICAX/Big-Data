package projekat;

import models.Location;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

class MyProcessWindowFunction extends ProcessWindowFunction<Location, Tuple3<String, List<String>, String>, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<Location> input, Collector<Tuple3<String, List<String>, String>> out) {
        long count = 0;
        ArrayList<String> lista = new ArrayList<String>();

        for (Location in: input) {
            count++;
            lista.add(in.getUser());
        }
        //out.collect("Window: " + context.window().getStart() + " count: " + count);
        out.collect(new Tuple3<>(Long.toString(context.window().getStart()), lista, Long.toString(count)));
    }
}
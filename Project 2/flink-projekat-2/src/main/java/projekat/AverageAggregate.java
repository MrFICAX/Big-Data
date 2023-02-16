package projekat;

import models.Location;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

class AverageAggregate implements AggregateFunction<Location, Tuple5<String, Double, Double, Double, Double>, Tuple5<String, Double, Double, Double, Double>> {
    @Override
    public Tuple5<String, Double, Double, Double, Double> createAccumulator() {
        return new Tuple5<>("", 0D, Double.MAX_VALUE, Double.MIN_VALUE, 0D);
    }

    @Override
    public Tuple5<String, Double, Double, Double, Double> add(Location value, Tuple5<String, Double, Double, Double, Double> accumulator) {
        accumulator.f0 = value.getUser();
        accumulator.f1 += value.getAltitude();
        accumulator.f2 = accumulator.f2 > value.getAltitude() ? value.getAltitude() : accumulator.f2; //MINIMUM
        accumulator.f3 = accumulator.f3 < value.getAltitude() ? value.getAltitude() : accumulator.f3; //MAXIMUM

        return new Tuple5<>(accumulator.f0, accumulator.f1, accumulator.f2, accumulator.f3, accumulator.f4 + 1);
    }

    @Override
    public Tuple5<String, Double, Double, Double, Double> getResult(Tuple5<String, Double, Double, Double, Double> acc) {
        return new Tuple5<>(acc.f0, acc.f2, acc.f3, acc.f1 / acc.f4, acc.f4);
    }

    @Override
    public Tuple5<String, Double, Double, Double, Double> merge(Tuple5<String, Double, Double, Double, Double> acc1, Tuple5<String, Double, Double, Double, Double> acc2) {
        return new Tuple5<>(acc1.f0, acc1.f1 + acc2.f1, acc1.f2 < acc2.f2 ? acc1.f2 : acc2.f2, acc1.f2 > acc2.f2 ? acc1.f2 : acc2.f2, acc1.f4 + acc2.f4);
    }

//    @Override
//    public Tuple4<String, Double, Double, Double> getResult(Tuple2<Double, Long> accumulator) {
//        return ((double) accumulator.f0) / accumulator.f1;
//    }
//
//    @Override
//    public Tuple5<String, Double, Double, Double, Long> merge(Tuple2<Double, Long> a, Tuple2<Double, Long> b) {
//        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
//    }
}
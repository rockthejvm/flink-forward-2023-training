package com.rockthejvm.course;

import com.rockthejvm.shopping.AddToShoppingCartEvent;
import com.rockthejvm.shopping.ShoppingCartEvent;
import com.rockthejvm.shopping.ShoppingCartEventsGenerator;
import org.apache.commons.math3.analysis.function.Add;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Checkpoints {

  static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

  static void demoCheckpoints() throws Exception {
    // how often checkpoints should run
    env.getCheckpointConfig().setCheckpointInterval(5000);
    // set storage
    env.getCheckpointConfig().setCheckpointStorage("file:///Users/daniel/dev/rockthejvm/trainings/flink-forward-2023/checkpoints");

    // do the usual
    DataStream<ShoppingCartEvent> shoppingCartEvents = env.addSource(
      new ShoppingCartEventsGenerator(100, 10)
    );

    shoppingCartEvents.keyBy(e -> e.getUserId())
      .flatMap(new HighQuantityFunction(3))
      .print();

    env.execute();
  }

  static class HighQuantityFunction
    implements FlatMapFunction<ShoppingCartEvent, Tuple2<String, Long>>,
    CheckpointedFunction {

    private int threshold = 0;

    public HighQuantityFunction(int threshold) {
      this.threshold = threshold;
    }

    @Override
    public void flatMap(ShoppingCartEvent value, Collector<Tuple2<String, Long>> out) throws Exception {
      if (value instanceof AddToShoppingCartEvent) {
        AddToShoppingCartEvent event = (AddToShoppingCartEvent)value;
        if (event.getQuantity() > 0)
          out.collect(new Tuple2(
            event.getSku(),
            event.getQuantity()
          ));
      }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
      System.out.println("Checkpoint triggered at " + context.getCheckpointTimestamp());
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
      // initialize ValueState, ListState, MapState ...
    }
  }


  public static void main(String[] args) throws Exception {
    demoCheckpoints();
  }
}

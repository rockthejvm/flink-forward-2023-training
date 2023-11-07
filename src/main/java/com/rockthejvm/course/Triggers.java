package com.rockthejvm.course;

import com.rockthejvm.shopping.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Triggers {

  // triggers decide when window functions get called

  static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  static DataStream<ShoppingCartEvent> shoppingCartEvents = env.addSource(
    new ShoppingCartEventsGenerator(500, 10)
  );

  static class CountByWindowAll implements AllWindowFunction<ShoppingCartEvent, String, TimeWindow> {
    @Override
    public void apply(TimeWindow window, Iterable<ShoppingCartEvent> values, Collector<String> out) throws Exception {
      int eventCount = 0;
      for (ShoppingCartEvent e : values) if (e instanceof AddToShoppingCartEvent)
        eventCount += 1;

      out.collect(
        "[" + window.getStart() + " - " + window.getEnd() + "] - " + eventCount + " add to cart events"
      );
    }
  }

  static void demoCountTrigger() throws Exception {
    shoppingCartEvents
      .windowAll(
        TumblingProcessingTimeWindows.of(Time.seconds(5))
      )
      .trigger(CountTrigger.of(3)) // window function is called every 5 elements
      .apply(new CountByWindowAll())
      .print();

    env.execute();
  }

  static void demoPurgingTrigger() throws Exception {
    shoppingCartEvents
      .windowAll(
        TumblingProcessingTimeWindows.of(Time.seconds(5))
      )
      .trigger(PurgingTrigger.of(CountTrigger.of(2))) // window function is called every 5 elements
      .apply(new CountByWindowAll())
      .print();

    env.execute();
  }

  public static void main(String[] args) throws Exception {
    demoCountTrigger();
  }
}

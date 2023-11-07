package com.rockthejvm.course;

import com.rockthejvm.shopping.*;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TimeBasedTransformations {

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

  // processing time - different runs produce different results
  static void demoProcessingTime() throws Exception {
    AllWindowedStream<ShoppingCartEvent, TimeWindow> groupedEventsByWindow =
      shoppingCartEvents.windowAll(
        TumblingProcessingTimeWindows.of(Time.seconds(3))
      );

    groupedEventsByWindow.apply(new CountByWindowAll()).print();
    env.execute();
  }

  // custom watermarks
  static class BoundedOutOfOrdernessGenerator implements WatermarkGenerator<ShoppingCartEvent> {
    private long maxDelay;
    private long currentMaxTimestamp = 0L;

    public BoundedOutOfOrdernessGenerator(long maxDelay) {
      this.maxDelay = maxDelay;
    }

    @Override
    public void onEvent(ShoppingCartEvent event, long eventTimestamp, WatermarkOutput output) {
      if (event.getTime().toEpochMilli() > currentMaxTimestamp) {
        currentMaxTimestamp = event.getTime().toEpochMilli();
        output.emitWatermark(new Watermark(event.getTime().toEpochMilli() - maxDelay));
      }
      // we may or may not emit watermarks
    }

    // 200ms
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
      // can emit watermarks as well
    }
  }

  static void demoEventTimeCustom() {
    DataStream<ShoppingCartEvent> shoppingCartEventsET = shoppingCartEvents
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.<ShoppingCartEvent>forGenerator(ctx ->
          new BoundedOutOfOrdernessGenerator(500)
        ).withTimestampAssigner(
          new SerializableTimestampAssigner<ShoppingCartEvent>() {
            @Override
            public long extractTimestamp(ShoppingCartEvent element, long recordTimestamp) {
              return element.getTime().toEpochMilli();
            }
          }
        )
      );
  }

  public static void main(String[] args) {

  }
}

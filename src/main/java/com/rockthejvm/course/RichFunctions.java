package com.rockthejvm.course;

import com.rockthejvm.shopping.AddToShoppingCartEvent;
import com.rockthejvm.shopping.ShoppingCartEvent;
import com.rockthejvm.shopping.ShoppingCartEventsGenerator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class RichFunctions {

  static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

  static void demoRichFunctions() throws Exception {
    env.setParallelism(2);

    DataStream<Integer> numbers = env.fromElements(1,2,3,4,5,6,7,8,9,10);
    // pure FP
    DataStream<Integer> numbersX2 = numbers.map(v -> v * 2);
    // "explicit" map function
    DataStream<Integer> numbersX2_v2 = numbers.map(new MapFunction<Integer, Integer>() {
      @Override
      public Integer map(Integer value) throws Exception {
        return value * 2;
      }
    });

    // rich version of the map function
    DataStream<Integer> numbersX2_v3 = numbers.map(new RichMapFunction<Integer, Integer>() {
      // store resources (state)

      @Override
      public Integer map(Integer value) throws Exception {
        return value * 2;
      }

      // lifecycle methods

      @Override
      public void open(Configuration parameters) throws Exception {
        System.out.println("Starting my work");
      }

      @Override
      public void close() throws Exception {
        System.out.println("Finishing my work");
      }
    });

    DataStream<Integer> numbersX2_v4 = numbers.process(
      new ProcessFunction<Integer, Integer>() {
        @Override
        public void processElement(Integer value, ProcessFunction<Integer, Integer>.Context ctx, Collector<Integer> out) throws Exception {
          out.collect(value * 2);
        }

        // lifecycle methods

        @Override
        public void close() throws Exception {
          System.out.println("Finishing my work");
        }

        @Override
        public void open(Configuration parameters) throws Exception {
          System.out.println("Starting my work");
        }
      }
    );

    numbersX2_v3.print();
    env.execute();
  }

  /**
   * Exercise
   *
   * DataStream of shopping cart events
   * Explode the events into "itemized events"
   * [ ("iphone", 2), ("cable", 3) ] ->
   * [ "iphone", "iphone", "cable", "cable", "cable" ]
   *
   * - use Java lambdas
   * - use ___Function
   * - use the rich version
   * - use a ProcessFunction
   */

  static void exercise() throws Exception {
    DataStream<ShoppingCartEvent> events =
      env.addSource(
        new ShoppingCartEventsGenerator(500, 10)
      );

    DataStream<ShoppingCartEvent> itemizedEvents =
      events.flatMap((event, collector) -> {
        if (event instanceof AddToShoppingCartEvent) {
          AddToShoppingCartEvent atsce = (AddToShoppingCartEvent) event;
          int quantity = atsce.getQuantity();
          for (int i = 0; i < quantity; i++)
            collector.collect(new AddToShoppingCartEvent(
              atsce.getUserId(),
              atsce.getSku(),
              1,
              atsce.getTime()
            ));
        }
      });

    DataStream<String> itemizedEvents_v2 =
      events.flatMap(
        new FlatMapFunction<ShoppingCartEvent, String>() {
          @Override
          public void flatMap(ShoppingCartEvent event, Collector<String> collector) throws Exception {
            if (event instanceof AddToShoppingCartEvent) {
              AddToShoppingCartEvent atsce = (AddToShoppingCartEvent) event;
              int quantity = atsce.getQuantity();
              for (int i = 0; i < quantity; i++)
                collector.collect(atsce.getSku());
            }
          }
        }
      );

    DataStream<String> itemizedEvents_v3 =
      events.flatMap(
        new RichFlatMapFunction<ShoppingCartEvent, String>() {
          @Override
          public void flatMap(ShoppingCartEvent event, Collector<String> collector) throws Exception {
            if (event instanceof AddToShoppingCartEvent) {
              AddToShoppingCartEvent atsce = (AddToShoppingCartEvent) event;
              int quantity = atsce.getQuantity();
              for (int i = 0; i < quantity; i++)
                collector.collect(atsce.getSku());
            }
          }

          @Override
          public void open(Configuration parameters) throws Exception {
            System.out.println("itemizing");
          }

          @Override
          public void close() throws Exception {
            System.out.println("itemizing done");
          }
        }
      );

    DataStream<String> itemizedEvents_v4 =
      events.process(
        new ProcessFunction<ShoppingCartEvent, String>() {
          @Override
          public void processElement(ShoppingCartEvent event, ProcessFunction<ShoppingCartEvent, String>.Context ctx, Collector<String> collector) throws Exception {
            if (event instanceof AddToShoppingCartEvent) {
              AddToShoppingCartEvent atsce = (AddToShoppingCartEvent) event;
              int quantity = atsce.getQuantity();
              for (int i = 0; i < quantity; i++)
                collector.collect(atsce.getSku());
            }
          }

          @Override
          public void open(Configuration parameters) throws Exception {
            System.out.println("itemizing");
          }

          @Override
          public void close() throws Exception {
            System.out.println("itemizing done");
          }
        }
      );
  }

  public static void main(String[] args) throws Exception {
    demoRichFunctions();
  }
}

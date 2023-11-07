package com.rockthejvm.part1streams;

import com.rockthejvm.shopping.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TimeBasedTransformations {

  static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  static DataStream<ShoppingCartEvent> shoppingCartEvents = env.addSource(
    new ShoppingCartEventsGenerator(500, 10)
  );


  public static void main(String[] args) {

  }
}

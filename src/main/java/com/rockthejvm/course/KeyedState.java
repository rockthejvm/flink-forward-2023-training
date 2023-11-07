package com.rockthejvm.course;

import com.rockthejvm.shopping.ShoppingCartEvent;
import com.rockthejvm.shopping.ShoppingCartEventsGenerator;
import com.rockthejvm.shopping.SingleShoppingCartEventsGenerator;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class KeyedState {

    static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    static DataStream<ShoppingCartEvent> shoppingCartEvents = env.addSource(
            new ShoppingCartEventsGenerator(100, 10)
    );

    static KeyedStream<ShoppingCartEvent, String> eventsPerUser =
            shoppingCartEvents.keyBy(e -> e.getUserId());

    // how many events were generated per user?

    // ValueState = "distributed variable"

    static void demoValueState() throws Exception {
        // this is bad
        // 1 - other nodes can't see state
        // 2 - no recovery in case a node crashes
        DataStream<String> numEventsPerUserNaive =
                eventsPerUser.process(
                        new KeyedProcessFunction<String, ShoppingCartEvent, String>() {
                            //                     ^ key    ^ input            ^ output

                            int nEventsForUser = 0;

                            @Override
                            public void processElement(ShoppingCartEvent value, KeyedProcessFunction<String, ShoppingCartEvent, String>.Context ctx, Collector<String> out) throws Exception {
                                nEventsForUser += 1;
                                out.collect("User " + value.getUserId() + " - " + nEventsForUser);
                            }
                        }
                );

        DataStream<String> numEventsPerUser =
                eventsPerUser.process(
                        new KeyedProcessFunction<String, ShoppingCartEvent, String>() {
                            // value state
                            ValueState<Long> stateCounter;

                            // we initialize the state in lifecycle method
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                stateCounter = getRuntimeContext()
                                        .getState(new ValueStateDescriptor<>("events-counter", Long.class));
                            }

                            @Override
                            public void processElement(ShoppingCartEvent value, KeyedProcessFunction<String, ShoppingCartEvent, String>.Context ctx, Collector<String> out) throws Exception {
                                if (stateCounter.value() == null)
                                    stateCounter.update(0L);
                                Long nEventsForUser = stateCounter.value(); // accesses the Flink Value state
                                stateCounter.update(nEventsForUser + 1);
                                out.collect("User " + value.getUserId() + " - " + (nEventsForUser + 1));
                            }
                        }
                );

        numEventsPerUser.print();
        env.execute();
    }

    // let's store ALL the events for a particular user
    // List State

    static void demoListState() throws Exception {
        DataStream<String> reports = eventsPerUser.process(
                new KeyedProcessFunction<String, ShoppingCartEvent, String>() {

                    // list state
                    ListState<ShoppingCartEvent> stateEventsForUser;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        stateEventsForUser = getRuntimeContext()
                                .getListState(new ListStateDescriptor<>("shopping-cart-events", ShoppingCartEvent.class));
                    }

                    @Override
                    public void close() throws Exception {
                        stateEventsForUser.clear();
                    }

                    @Override
                    public void processElement(ShoppingCartEvent event, KeyedProcessFunction<String, ShoppingCartEvent, String>.Context ctx, Collector<String> out) throws Exception {
                        stateEventsForUser.add(event);
                        StringBuilder listOfEventsReport = new StringBuilder("[");
                        for (ShoppingCartEvent e : stateEventsForUser.get())
                            listOfEventsReport.append(e.toString()).append(",");
                        listOfEventsReport.append("]");

                        out.collect("User " + event.getUserId() + " - " + listOfEventsReport);
                    }
                }
        );

        reports.print();
        env.execute();
    }

    // how many events per user, PER EVENT TYPE?
    static void demoMapState() throws Exception {
        DataStream<String> reports = eventsPerUser.process(
                new ProcessFunction<ShoppingCartEvent, String>() {

                    MapState<String, Long> stateCountEventsPerType;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        stateCountEventsPerType = getRuntimeContext()
                                .getMapState(new MapStateDescriptor<>("per-type-counter", String.class, Long.class));
                    }

                    @Override
                    public void close() throws Exception {
                        stateCountEventsPerType.clear();
                    }

                    @Override
                    public void processElement(ShoppingCartEvent event, ProcessFunction<ShoppingCartEvent, String>.Context ctx, Collector<String> out) throws Exception {
                        String eventType = event.getClass().getSimpleName();
                        if (stateCountEventsPerType.contains(eventType)) {
                            long oldCount = stateCountEventsPerType.get(eventType);
                            stateCountEventsPerType.put(eventType, oldCount + 1);
                        } else {
                            stateCountEventsPerType.put(eventType, 1L);
                        }

                        StringBuilder breakdownPerType = new StringBuilder("{");

                        for (Iterator<Map.Entry<String, Long>> it = stateCountEventsPerType.iterator(); it.hasNext(); ) {
                            Map.Entry<String, Long> pair = it.next();
                            String e = pair.getKey();
                            Long count = pair.getValue();
                            breakdownPerType.append(e + " -> " + count + ", ");
                        }
                        breakdownPerType.append("}");

                        out.collect("User " + event.getUserId() + " - " + breakdownPerType);
                    }
                }
        );

        reports.print();
        env.execute();
    }

    /**
     * For every user, for every event type,
     * store the last 5 events, print them out as reports.
     */

    public static void main(String[] args) throws Exception {
        demoLast5EventsPerUser();
    }

    private static void demoLast5EventsPerUser() throws Exception {
        eventsPerUser.process(new ProcessFunction<ShoppingCartEvent, String>() {

            ListState<String> last5;

            @Override
            public void open(Configuration parameters) throws Exception {
                last5 = getRuntimeContext().getListState(new ListStateDescriptor<>("last-5", String.class));
            }

            @Override
            public void close() throws Exception {
                last5.clear();
            }

            @Override
            public void processElement(ShoppingCartEvent value, ProcessFunction<ShoppingCartEvent, String>.Context ctx, Collector<String> out) throws Exception {
                List<String> last5V = new ArrayList<>((Collection) last5.get());
                if (last5V.size() == 5) {
                    last5V.remove(0);
                }
                last5V.add(value.toString());
                last5.update(last5V);
                out.collect(value.getUserId() + ":" + last5V.toString());
            }
        }).print();

        env.execute();
    }
}

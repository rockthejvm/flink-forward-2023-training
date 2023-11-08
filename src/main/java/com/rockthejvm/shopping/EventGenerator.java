package com.rockthejvm.shopping;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.SerializableOptional;
import org.apache.flink.util.function.SerializableFunction;

import java.time.Instant;
import java.util.function.Function;

public class EventGenerator<T> extends RichParallelSourceFunction<T> {
    private int sleepMillisBetweenEvents;
    private Instant baseInstant;
    private Function<Long, T> generator;
    private SerializableOptional<Long> extraDelayInMillisOnEveryTenEvents;
    private volatile boolean running = true;

    public EventGenerator(int sleepMillisBetweenEvents, SerializableFunction<Long, T> generator, Instant baseInstant, SerializableOptional<Long> extraDelayInMillisOnEveryTenEvents) {
        this.sleepMillisBetweenEvents = sleepMillisBetweenEvents;
        this.generator = generator;
        this.baseInstant = baseInstant;
        this.extraDelayInMillisOnEveryTenEvents = extraDelayInMillisOnEveryTenEvents;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        run(1, ctx);
    }

    @Override
    public void cancel() {
        running = false;
    }

    private void run(long id, SourceContext<T> ctx) throws Exception {
        while (running) {
            ctx.collect(generator.apply(id));
            ctx.emitWatermark(new Watermark(baseInstant.plusSeconds(id).toEpochMilli()));
            Thread.sleep(sleepMillisBetweenEvents);
            if (id % 10 == 0) {
                extraDelayInMillisOnEveryTenEvents.ifPresent(delay -> {
                    try { Thread.sleep(delay); }
                    catch (InterruptedException e) {}
                });
            }
            id++;
        }
    }
}
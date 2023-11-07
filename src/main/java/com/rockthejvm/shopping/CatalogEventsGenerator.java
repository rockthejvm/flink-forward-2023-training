package com.rockthejvm.shopping;

import org.apache.flink.types.SerializableOptional;
import java.time.Instant;
import java.util.UUID;

public class CatalogEventsGenerator extends EventGenerator<CatalogEvent> {
    private int sleepMillisBetweenEvents;
    private Instant baseInstant;
    private SerializableOptional<Long> extraDelayInMillisOnEveryTenEvents;

    public CatalogEventsGenerator(int sleepMillisBetweenEvents, Instant baseInstant, SerializableOptional<Long> extraDelayInMillisOnEveryTenEvents) {
        super(
          sleepMillisBetweenEvents,
          id -> new ProductDetailsViewed(
            SingleShoppingCartEventsGenerator.getRandomUser(),
            baseInstant.plusSeconds(id), UUID.randomUUID().toString()),
            baseInstant,
          extraDelayInMillisOnEveryTenEvents
        );

        this.sleepMillisBetweenEvents = sleepMillisBetweenEvents;
        this.baseInstant = baseInstant;
        this.extraDelayInMillisOnEveryTenEvents = extraDelayInMillisOnEveryTenEvents;
    }
}
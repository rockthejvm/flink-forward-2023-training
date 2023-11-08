package com.rockthejvm.shopping;

import org.apache.flink.types.SerializableOptional;
import org.apache.flink.util.function.SerializableFunction;
import org.apache.flink.util.function.SerializableSupplier;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class SingleShoppingCartEventsGenerator extends EventGenerator<ShoppingCartEvent> {
  private int sleepMillisBetweenEvents;
  private Instant baseInstant;
  private SerializableOptional<Long> extraDelayInMillisOnEveryTenEvents;
  private SerializableOptional<String> sourceId;
  private boolean generateRemoved;

  public SingleShoppingCartEventsGenerator(int sleepMillisBetweenEvents, Instant baseInstant, SerializableOptional<Long> extraDelayInMillisOnEveryTenEvents, SerializableOptional<String> sourceId, boolean generateRemoved) {
    super(
      sleepMillisBetweenEvents,
      eventGenerator(
        generateRemoved,
        () -> sourceId
          .map(id -> id + " " + UUID.randomUUID())
          .toOptional()
          .orElse(UUID.randomUUID().toString()),
        baseInstant
      ),
      baseInstant,
      extraDelayInMillisOnEveryTenEvents
    );
    this.sleepMillisBetweenEvents = sleepMillisBetweenEvents;
    this.baseInstant = baseInstant;
    this.extraDelayInMillisOnEveryTenEvents = extraDelayInMillisOnEveryTenEvents;
    this.sourceId = sourceId;
    this.generateRemoved = generateRemoved;
  }

  private static List<String> users = List.of("Bob", "Alice", "Sam", "Tom", "Diana");
  private static Random random = new Random();

  public static String getRandomUser() {
    return users.get(random.nextInt(users.size()));
  }

  private static int getRandomQuantity() {
    return random.nextInt(10);
  }

  private static SerializableFunction<Long, ShoppingCartEvent> eventGenerator(boolean generateRemoved, SerializableSupplier<String> skuGen, Instant baseInstant) {
    return id -> {
      if (!generateRemoved || random.nextBoolean())
        return new AddToShoppingCartEvent(
          getRandomUser(),
          skuGen.get(),
          getRandomQuantity(),
          baseInstant.plusSeconds(id)
        );
      else
        return new RemovedFromShoppingCartEvent(
          getRandomUser(),
          skuGen.get(),
          getRandomQuantity(),
          baseInstant.plusSeconds(id)
        );
    };
  }
}

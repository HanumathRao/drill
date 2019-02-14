package org.apache.drill.common.util.function;

import java.util.function.Consumer;

@FunctionalInterface
public interface CheckedConsumer<T, E extends Throwable> {
  void accept(T t) throws E;

  static <T> Consumer<T> throwingConsumerWrapper(
    CheckedConsumer<T, Exception> throwingConsumer) {

    return i -> {
      try {
        throwingConsumer.accept(i);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    };
  }
}


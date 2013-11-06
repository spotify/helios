package com.spotify.helios.common;

public interface Main {
  int run();
  void shutdown();
  void join() throws InterruptedException;
}

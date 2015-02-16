/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.helios.agent;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.MoreExecutors;

import com.fasterxml.jackson.core.type.TypeReference;

import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.TaskStatusEvent;
import com.spotify.helios.servicescommon.PersistentAtomicReference;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NodeExistsException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Path;

import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Writes task history to ZK, and attempts to gracefully handle the case where ZK is down, and tries
 * to lose the right things if it has to lose stuff.
 *
 * Just some breadcrumbs so next time, the person that follows me can understand why things are
 * the way they are.
 *
 * Theory of operation:
 * 1. saveHistoryItem should never block for any significant amount of time.  Specifically, it
 *    should not block on ZK being in any particular state, and ideally not while a file write is
 *    occurring, as the file may get large if ZK has been away for a long time.
 * 2. We limit each job to max 30 events in memory (and in ZK for that matter)
 * 3. Maximum of 600 total events, so as not to consume all available memory.
 */
public class QueueingHistoryWriter extends AbstractIdleService implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(QueueingHistoryWriter.class);

  public static final int MAX_NUMBER_STATUS_EVENTS_TO_RETAIN = 30;
  private static final int MAX_QUEUE_SIZE = 30;
  private static final int MAX_TOTAL_SIZE = 600;

  private final ConcurrentMap<JobId, Deque<TaskStatusEvent>> items;
  private final ScheduledExecutorService zkWriterExecutor =
      MoreExecutors.getExitingScheduledExecutorService(
          (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1), 0, SECONDS);
  private final String hostname;
  private final AtomicInteger count;
  private final ZooKeeperClient client;
  private final PersistentAtomicReference<ConcurrentMap<JobId, Deque<TaskStatusEvent>>>
      backingStore;

  public QueueingHistoryWriter(final String hostname, final ZooKeeperClient client,
                               final Path backingFile) throws IOException, InterruptedException {
    this.hostname = hostname;
    this.client = client;
    this.backingStore = PersistentAtomicReference.create(backingFile,
        new TypeReference<ConcurrentMap<JobId, Deque<TaskStatusEvent>>>(){},
        new Supplier<ConcurrentMap<JobId, Deque<TaskStatusEvent>>>() {
          @Override public ConcurrentMap<JobId, Deque<TaskStatusEvent>> get() {
            return Maps.newConcurrentMap();
          }
        });
    this.items = backingStore.get();

    // Clean out any errant null values.  Normally shouldn't have any, but we did have a few
    // where it happened, and this will make sure we can get out of a bad state if we get into it.
    final ImmutableSet<JobId> curKeys = ImmutableSet.copyOf(this.items.keySet());
    for (JobId key : curKeys) {
      if (this.items.get(key) == null) {
        this.items.remove(key);
      }
    }

    int itemCount = 0;
    for (Deque<TaskStatusEvent> deque : items.values()) {
      itemCount += deque.size();
    }
    this.count = new AtomicInteger(itemCount);
  }

  @Override
  protected void startUp() throws Exception {
    zkWriterExecutor.scheduleAtFixedRate(this, 1, 1, TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() throws Exception {
    zkWriterExecutor.shutdownNow();
    zkWriterExecutor.awaitTermination(1, TimeUnit.MINUTES);
  }

  private void add(TaskStatusEvent item) throws InterruptedException {
    // If too many "globally", toss them
    while (count.get() >= MAX_TOTAL_SIZE) {
      getNext();
    }

    final JobId key = item.getStatus().getJob().getId();
    final Deque<TaskStatusEvent> deque = getDeque(key);

    synchronized (deque) {
      // if too many in the particular deque, toss them
      while (deque.size() >= MAX_QUEUE_SIZE) {
        deque.remove();
        count.decrementAndGet();
      }
      deque.add(item);
      count.incrementAndGet();
    }

    try {
      backingStore.set(items);
    } catch (ClosedByInterruptException e) {
      log.debug("Writing task status event to backing store was interrupted");
    } catch (IOException e) { // We are best effort after all...
      log.warn("Failed to write task status event to backing store", e);
    }
  }

  private Deque<TaskStatusEvent> getDeque(final JobId key) {
    synchronized (items) {
      final Deque<TaskStatusEvent> deque = items.get(key);
      if (deque == null) {  // try more assertively to get a deque
        final ConcurrentLinkedDeque<TaskStatusEvent> newDeque =
            new ConcurrentLinkedDeque<TaskStatusEvent>();
        items.put(key, newDeque);
        return newDeque;
      }
      return deque;
    }
  }

  public void saveHistoryItem(final JobId jobId, final TaskStatus status)
      throws InterruptedException {
    saveHistoryItem(jobId, status, System.currentTimeMillis());
  }

  public void saveHistoryItem(final JobId jobId, final TaskStatus status, long timestamp)
      throws InterruptedException {
    add(new TaskStatusEvent(status, timestamp, hostname));
  }

  private TaskStatusEvent getNext() {
    // Some explanation: We first find the eldest event from amongst the queues (ok, they're
    // deques, but we really use it as a put back queue), and only then to we try to get
    // a lock on the relevant queue from whence we got the event.  Assuming that all worked
    // *and* that the event we have wasn't rolled off due to max-size limitations, we then
    // pull the item off the queue and return it.  We're basically doing optimistic concurrency,
    // and skewing things so that adding to this should be cheap.

    while (true) {
      final TaskStatusEvent current = findEldestEvent();

      // Didn't find anything that needed processing?
      if (current == null) {
        return null;
      }

      final JobId id = current.getStatus().getJob().getId();
      final Deque<TaskStatusEvent> deque = items.get(id);
      if (deque == null) {
        // shouldn't happen because we should be the only one pulling items off, but....
        continue;
      }

      synchronized (deque) {
        if (!deque.peek().equals(current)) {
          // item got rolled off, try again
          continue;
        }

        // Pull it off the queue and be paranoid.
        final TaskStatusEvent newCurrent = deque.poll();
        count.decrementAndGet();
        checkState(current.equals(newCurrent), "current should equal newCurrent");
        // Safe because this is the *only* place we hold these two locks at the same time.
        synchronized (items) {
          // Extra paranoia: curDeque should always == deque
          final Deque<TaskStatusEvent> curDeque = items.get(id);
          if (curDeque != null && curDeque.isEmpty()) {
            items.remove(id);
          }
        }
        return current;
      }
    }
  }

  public boolean isEmpty() {
    return count.get() == 0;
  }

  private void putBack(TaskStatusEvent event) {
    final JobId key = event.getStatus().getJob().getId();
    final Deque<TaskStatusEvent> queue = getDeque(key);
    synchronized (queue) {
      if (queue.size() >= MAX_QUEUE_SIZE) {
        // already full, just toss the event
        return;
      }
      queue.push(event);
      count.incrementAndGet();
    }
  }

  private TaskStatusEvent findEldestEvent() {
    // We don't lock anything because in the worst case, we just put things in out of order which
    // while not perfect, won't cause any actual harm.  Out of order meaning between jobids, not
    // within the same job id.  Whether this is the best strategy (as opposed to fullest deque)
    // is arguable.
    TaskStatusEvent current = null;
    for (Deque<TaskStatusEvent> queue : items.values()) {
      if (queue == null) {
        continue;
      }
      final TaskStatusEvent item = queue.peek();
      if (current == null || (item.getTimestamp() < current.getTimestamp())) {
        current = item;
      }
    }
    return current;
  }

  @Override
  public void run() {
    while (true) {
      final TaskStatusEvent item = getNext();
      if (item == null) {
        return;
      }

      final JobId jobId = item.getStatus().getJob().getId();
      final String historyPath = Paths.historyJobHostEventsTimestamp(
          jobId, hostname, item.getTimestamp());

      log.debug("writing queued item to zookeeper {} {}", item.getStatus().getJob().getId(),
          item.getTimestamp());

      try {
        client.ensurePath(historyPath, true);
        client.createAndSetData(historyPath, item.getStatus().toJsonBytes());

        // See if too many
        final List<String> events = client.getChildren(Paths.historyJobHostEvents(jobId, hostname));
        if (events.size() > MAX_NUMBER_STATUS_EVENTS_TO_RETAIN) {
          trimStatusEvents(events, jobId);
        }
      } catch (NodeExistsException e) {
        // Ahh, the two generals problem...  We handle by doing nothing since the thing
        // we wanted in, is in.
        log.debug("item we wanted in is already there");
      } catch (ConnectionLossException e) {
        log.warn("Connection lost while putting item into zookeeper, will retry");
        putBack(item);
        break;
      } catch (KeeperException e) {
        log.error("Error putting item into zookeeper, will retry", e);
        putBack(item);
        break;
      }

      final List<String> listeners = new ArrayList<>();

      try {
        listeners.addAll(client.getChildren(Paths.historyListeners()));
      } catch (KeeperException e) {
        log.error("Unable to fetch listener names", e);
      }

      final Iterable<Optional<URL>> possibleUrls =
          Iterables.transform(listeners, new Function<String, Optional<URL>>()
      {
        @Override
        public Optional<URL> apply(final String listener) {
          Optional<URL> result = Optional.absent();

          try {
            result = Optional.of(new URL(new String(
                client.getData(Paths.historyListener(listener)), "UTF-8")));
          } catch (KeeperException | UnsupportedEncodingException | MalformedURLException e) {
            log.error("Unable to fetch endpoint for listener {}", listener, e);
          }

          return result;
        }
      });

      for (URL url: Optional.presentInstances(possibleUrls)) {
        log.info("Pushing task status event to listener {}", url);

        try {
          final HttpURLConnection connection = (HttpURLConnection) url.openConnection();

          connection.setConnectTimeout(1000);
          connection.setReadTimeout(1000);

          connection.addRequestProperty("Content-Type", "application/json");
          connection.addRequestProperty("Charset", "UTF-8");

          connection.setRequestMethod("POST");

          connection.setDoOutput(true);
          connection.getOutputStream().write(Json.asBytes(item));

          if (connection.getResponseCode() / 100 != 2) {
            log.error("Got non-200 response code while communicating with listener {}", url);
          }
        } catch (IOException e) {
          log.error("Unable to communicate with listener {}", url, e);
        }
      }
    }
  }

  private void trimStatusEvents(List<String> events, JobId jobId) {
    // CleanupExecutor only has one thread so can assume no others are fiddling as we do this.
    // All this to sort numerically instead of lexically....
    final List<Long> eventsAsLongs = Lists.newArrayList(Iterables.transform(events,
      new Function<String, Long>() {
      @Override
      public Long apply(String name) {
        return Long.valueOf(name);
      }
    }));
    Collections.sort(eventsAsLongs);

    for (int i = 0; i < (eventsAsLongs.size() - MAX_NUMBER_STATUS_EVENTS_TO_RETAIN); i++) {
      try {
        client.delete(Paths.historyJobHostEventsTimestamp(jobId, hostname, eventsAsLongs.get(i)));
      } catch (KeeperException e) {
        log.warn("failure deleting overflow of status items - we're hoping a later"
            + " execution will fix", e);
      }
    }
  }
}

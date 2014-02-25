package com.spotify.helios.agent;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.TaskStatusEvent;
import com.spotify.helios.servicescommon.PersistentAtomicReference;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;

public class QueueingHistoryWriter implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(QueueingHistoryWriter.class);

  public static final int MAX_NUMBER_STATUS_EVENTS_TO_RETAIN = 30;
  private static final int MAX_QUEUE_SIZE = 30;
  private static final int MAX_TOTAL_SIZE = 600;

  private final ConcurrentMap<JobId, Deque<TaskStatusEvent>> items;
  private final ScheduledExecutorService zkWriterExecutor;
  private final String hostname;
  private final AtomicInteger count;
  private final ZooKeeperClient client;
  private final PersistentAtomicReference<ConcurrentMap<JobId, Deque<TaskStatusEvent>>> backingStore;

  public QueueingHistoryWriter(final String hostname, final ZooKeeperClient client,
                               final String backingFile) throws IOException {
    this(hostname, client, FileSystems.getDefault().getPath(backingFile));
  }

  public QueueingHistoryWriter(final String hostname, final ZooKeeperClient client,
                               final Path backingFile) throws IOException {

    this.zkWriterExecutor = Executors.newScheduledThreadPool(1);
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

    int itemCount = 0;
    for (Deque<TaskStatusEvent> deque : items.values()) {
      itemCount += deque.size();
    }
    this.count = new AtomicInteger(itemCount);
  }

  public void start() {
    zkWriterExecutor.scheduleAtFixedRate(this, 1, 1, TimeUnit.SECONDS);
 }

  public void shutdown() {
    zkWriterExecutor.shutdownNow();
  }

  private void add(TaskStatusEvent item) {
    // If too many, toss them
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
      try {
        backingStore.set(items);
      } catch (IOException e) { // We are best effort after all...
        log.warn("Failed to write task status event to backing store", e);
      }
    }
  }

  private Deque<TaskStatusEvent> getDeque(final JobId key) {
    final Deque<TaskStatusEvent> deque = items.get(key);
    if (deque == null) {  // try more assertively to get a deque
      final ConcurrentLinkedDeque<TaskStatusEvent> newDeque = new ConcurrentLinkedDeque<TaskStatusEvent>();
      items.putIfAbsent(key, newDeque);
      return newDeque;
    }
    return deque;
  }

  public void saveHistoryItem(final JobId jobId, final TaskStatus status) {
    saveHistoryItem(jobId, status, System.currentTimeMillis());
  }

  public void saveHistoryItem(final JobId jobId, final TaskStatus status, long timestamp) {
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

      final Queue<TaskStatusEvent> mappedQueue = items.get(current.getStatus().getJob().getId());
      if (mappedQueue == null) {
        // shouldn't happen because we should be the only one pulling items off, but....
        continue;
      }

      synchronized (mappedQueue) {
        if (!mappedQueue.peek().equals(current)) {
          // item got rolled off, try again
          continue;
        }

        // Pull it off the queue and be paranoid.
        final TaskStatusEvent newCurrent = mappedQueue.poll();
        count.decrementAndGet();
        checkState(current.equals(newCurrent), "current should equal newcurrent");
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
    }
  }

  private TaskStatusEvent findEldestEvent() {
    TaskStatusEvent current = null;
    for (Deque<TaskStatusEvent> queue : items.values()) {
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

      try {
        final JobId jobId = item.getStatus().getJob().getId();
        final String historyPath = Paths.historyJobHostEventsTimestamp(
            jobId, hostname, item.getTimestamp());
        client.createAndSetData(historyPath, item.getStatus().toJsonBytes());

        // See if too many
        final List<String> events = client.getChildren(Paths.historyJobHostEvents(jobId, hostname));
        if (events.size() > MAX_NUMBER_STATUS_EVENTS_TO_RETAIN) {
          trimStatusEvents(events, jobId);
        }
      } catch (KeeperException e) {
        putBack(item);
        break;
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
        log.warn("failure deleting overflow of status items - we're hoping a later execution will fix", e);
      }
    }
  }
}

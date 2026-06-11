package com.analyzer.queue;

import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.assertThat;

class CustomBlockingQueueTest {

    @Test
    void testBasicPutAndTake() throws InterruptedException {
        CustomBlockingQueue<Integer> queue = new CustomBlockingQueue<>(5);
        queue.put(1);
        queue.put(2);

        assertThat(queue.size()).isEqualTo(2);
        assertThat(queue.take()).isEqualTo(1);
        assertThat(queue.take()).isEqualTo(2);
        assertThat(queue.size()).isEqualTo(0);
    }

    @Test
    void testBackpressureBlocking() throws InterruptedException {
        CustomBlockingQueue<Integer> queue = new CustomBlockingQueue<>(2);
        queue.put(1);
        queue.put(2);

        AtomicBoolean wasBlocked = new AtomicBoolean(true);
        Thread producer = new Thread(() -> {
            try {
                queue.put(3);
                wasBlocked.set(false);
            } catch (InterruptedException ignored) {
            }
        });

        producer.start();
        Thread.sleep(100); // Give the producer thread time to block
        assertThat(queue.size()).isEqualTo(2);
        assertThat(wasBlocked.get()).isTrue();

        // Drain one item, which should unblock the producer
        assertThat(queue.take()).isEqualTo(1);
        producer.join(1000); // Wait for producer to finish

        assertThat(wasBlocked.get()).isFalse();
        assertThat(queue.size()).isEqualTo(2);
        assertThat(queue.take()).isEqualTo(2);
        assertThat(queue.take()).isEqualTo(3);
    }

    @Test
    void testShutdownUnblocksThreads() throws InterruptedException {
        CustomBlockingQueue<Integer> queue = new CustomBlockingQueue<>(2);
        queue.put(1);
        queue.put(2);

        AtomicBoolean threwException = new AtomicBoolean(false);
        Thread producer = new Thread(() -> {
            try {
                queue.put(3);
            } catch (IllegalStateException e) {
                threwException.set(true);
            } catch (InterruptedException ignored) {
            }
        });

        producer.start();
        Thread.sleep(100);

        queue.shutdown();
        producer.join(1000);

        assertThat(threwException.get()).isTrue();
        assertThat(queue.take()).isEqualTo(1);
        assertThat(queue.take()).isEqualTo(2);
        assertThat(queue.take()).isNull(); // Returns null once empty and shutdown
    }

    @Test
    void testConcurrentPutAndTake() throws InterruptedException {
        CustomBlockingQueue<Integer> queue = new CustomBlockingQueue<>(10);
        int threadCount = 4;
        int itemsPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount * 2);

        AtomicInteger sumTaken = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threadCount * 2);

        // Producers
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < itemsPerThread; j++) {
                        queue.put(1);
                    }
                } catch (InterruptedException ignored) {
                }
                latch.countDown();
            });
        }

        // Consumers
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < itemsPerThread; j++) {
                        Integer val = queue.take();
                        if (val != null) {
                            sumTaken.addAndGet(val);
                        }
                    }
                } catch (InterruptedException ignored) {
                }
                latch.countDown();
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.SECONDS);

        assertThat(sumTaken.get()).isEqualTo(threadCount * itemsPerThread);
    }
}

package com.analyzer.queue;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A custom bounded blocking queue built on primitive locks and conditions.
 * Used to simulate a message queue with backpressure and graceful shutdown support.
 */
public class CustomBlockingQueue<T> {
    private final Queue<T> queue = new LinkedList<>();
    private final int capacity;
    private boolean isShutdown = false;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();

    public CustomBlockingQueue(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Queue capacity must be greater than zero");
        }
        this.capacity = capacity;
    }

    /**
     * Inserts the specified element into this queue, waiting if necessary
     * for space to become available.
     *
     * @param item the element to add
     * @throws InterruptedException if interrupted while waiting
     * @throws IllegalStateException if the queue has been shut down
     */
    public void put(T item) throws InterruptedException {
        if (item == null) {
            throw new NullPointerException("Null items are not allowed");
        }
        lock.lockInterruptibly();
        try {
            while (queue.size() == capacity && !isShutdown) {
                notFull.await();
            }
            if (isShutdown) {
                throw new IllegalStateException("Queue is shut down");
            }
            queue.add(item);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retrieves and removes the head of this queue, waiting if necessary
     * until an element becomes available.
     *
     * @return the head of this queue, or null if the queue is shut down and empty
     * @throws InterruptedException if interrupted while waiting
     */
    public T take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (queue.isEmpty() && !isShutdown) {
                notEmpty.await();
            }
            if (queue.isEmpty() && isShutdown) {
                return null;
            }
            T item = queue.poll();
            notFull.signal();
            return item;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Initiates an orderly shutdown. Prevents new items from being put into the queue.
     * Wakes up all waiting threads.
     */
    public void shutdown() {
        lock.lock();
        try {
            isShutdown = true;
            notFull.signalAll();
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public boolean isShutdown() {
        lock.lock();
        try {
            return isShutdown;
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    public int capacity() {
        return this.capacity;
    }
}

package com.squareup.tape;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by nsun on 16-4-22.
 */
public class InfQueue<E> extends LinkedBlockingQueue<E> implements BlockingQueue<E> {

    int memoryQueueSize;
    FileObjectQueue<E> backendQueue;
    ReentrantLock backendQueueLock = new ReentrantLock();

    public InfQueue(int inMemorySize, FileObjectQueue.Converter<E> converter, File queueFile) throws IOException {
        super(inMemorySize);
        this.memoryQueueSize = inMemorySize;
        backendQueue = new FileObjectQueue<E>(queueFile, converter);
    }

    @Override
    public int size() {
        return super.size() + backendQueue.size();
    }

    @Override
    public void put(E e) throws InterruptedException {
        if (this.size() < this.memoryQueueSize) {
            super.put(e);
        } else {
            try{
                backendQueueLock.lock();
                backendQueue.add(e);
            } finally {
                backendQueueLock.unlock();
            }
        }
    }

    @Override
    public boolean offer(E e, long l, TimeUnit timeUnit) throws InterruptedException {
        boolean offered = super.offer(e, l, timeUnit);
        if (! offered) {
            try {
                backendQueueLock.lock();
                backendQueue.add(e);
            } finally {
                backendQueueLock.unlock();
            }
        }
        return true;
    }

    @Override
    public boolean offer(E e) {
        try {
            this.put(e);
        } catch (InterruptedException e1) {
        }
        return true;
    }

    @Override
    public E poll(long l, TimeUnit timeUnit) throws InterruptedException {
        try {
            backendQueueLock.lock();
            if (this.backendQueue.size() > 0) {
                E obj = this.backendQueue.peek();
                this.backendQueue.remove();
                return obj;
            } else {
                return super.poll(l, timeUnit);
            }
        } finally {
            backendQueueLock.unlock();
        }

    }

    @Override
    public E poll() {
        try {
            backendQueueLock.lock();
            if (this.backendQueue.size() > 0) {
                E obj = this.backendQueue.peek();
                this.backendQueue.remove();
                return obj;
            } else {
                return super.poll();
            }
        } finally {
            backendQueueLock.unlock();
        }
    }

    @Override
    public E peek() {
        try {
            backendQueueLock.lock();
            if (this.backendQueue.size() > 0) {
                E obj = this.backendQueue.peek();
                return obj;
            } else {
                return super.peek();
            }
        } finally {
            backendQueueLock.unlock();
        }
    }

    @Override
    public E take() throws InterruptedException {
        try {
            backendQueueLock.lock();
            if (this.backendQueue.size() > 0) {
                E obj = this.backendQueue.peek();
                this.backendQueue.remove();
                return obj;
            } else {
                return super.take();
            }
        } finally {
            backendQueueLock.unlock();
        }
    }

    E peekFromMemoryQueue() {
        return super.peek();
    }

    E takeFromMemoryQueue() throws InterruptedException {
        return super.take();
    }

    E pollFromMemoryQueue() {
        return super.poll();
    }

    E pollFromMemoryQueue(long l, TimeUnit timeUnit) throws InterruptedException {
        return super.poll(l, timeUnit);
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("Not work for now");
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException("Not work for now");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Not work for now.");
    }

    @Override
    public boolean isEmpty() {
        return this.backendQueue.size() == 0 && super.isEmpty();
    }

    public void close() {
        this.backendQueue.close();
    }
}

package com.squareup.tape;

import java.io.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by nsun on 16-4-25.
 */
public class StrictInfQueue<E> implements BlockingQueue<E> {

    private AtomicLong orderCounter = new AtomicLong();
    private InfQueue<StrictInfQueue.OrderedWrapper<E>> innerQueue;

    public StrictInfQueue(int inMemorySize, FileObjectQueue.Converter<E> converter, File queueFile) throws IOException {
        innerQueue = new InfQueue<StrictInfQueue.OrderedWrapper<E>>(inMemorySize, new OrderedWrapperConverter(converter), queueFile);
    }

    OrderedWrapper<E> wrappedValue(E e) {
        OrderedWrapper<E> wrappedValue = new OrderedWrapper<E>();
        wrappedValue.id = orderCounter.getAndIncrement();
        wrappedValue.value = e;
        return wrappedValue;
    }

    @Override
    public void put(E e) throws InterruptedException {
        OrderedWrapper<E> wv = wrappedValue(e);
        innerQueue.put(wv);
    }

    @Override
    public boolean offer(E e, long l, TimeUnit timeUnit) throws InterruptedException {
        OrderedWrapper<E> wv = wrappedValue(e);
        return innerQueue.offer(wv, l, timeUnit);
    }

    @Override
    public boolean offer(E e) {
        OrderedWrapper<E> wv = wrappedValue(e);
        return innerQueue.offer(wv);
    }

    @Override
    public E poll(long l, TimeUnit timeUnit) throws InterruptedException {
        try {
            innerQueue.backendQueueLock.lock();
            if (innerQueue.backendQueue.size() > 0) {
                OrderedWrapper<E> objFromFile = innerQueue.backendQueue.peek();
                OrderedWrapper<E> objFromMem = innerQueue.peekFromMemoryQueue();
                if (objFromMem == null || objFromFile.id < objFromMem.id) {
                    innerQueue.backendQueue.remove();
                    return objFromFile.value;
                } else {
                    innerQueue.takeFromMemoryQueue();
                    return objFromMem.value;
                }
            } else {
                OrderedWrapper<E> vw = innerQueue.pollFromMemoryQueue(l, timeUnit);
                if (vw != null) {
                    return vw.value;
                } else {
                    return null;
                }
            }
        } finally {
            innerQueue.backendQueueLock.unlock();
        }
    }

    @Override
    public E poll() {
        try {
            innerQueue.backendQueueLock.lock();
            if (innerQueue.backendQueue.size() > 0) {
                OrderedWrapper<E> objFromFile = innerQueue.backendQueue.peek();
                OrderedWrapper<E> objFromMem = innerQueue.peekFromMemoryQueue();
                if (objFromMem == null || objFromFile.id < objFromMem.id) {
                    innerQueue.backendQueue.remove();
                    return objFromFile.value;
                } else {
                    innerQueue.takeFromMemoryQueue();
                    return objFromMem.value;
                }
            } else {
                OrderedWrapper<E> vw = innerQueue.pollFromMemoryQueue();
                if (vw != null) {
                    return vw.value;
                } else {
                    return null;
                }
            }
        } catch (InterruptedException e) {
            return null;
        } finally {
            innerQueue.backendQueueLock.unlock();
        }
    }

    @Override
    public E element() {
        throw new UnsupportedOperationException("Not work for now.");
    }

    @Override
    public E peek() {
        try {
            innerQueue.backendQueueLock.lock();
            if (innerQueue.backendQueue.size() > 0) {
                OrderedWrapper<E> objFromFile = innerQueue.backendQueue.peek();
                OrderedWrapper<E> objFromMem = innerQueue.peekFromMemoryQueue();
                if (objFromMem == null || objFromFile.id < objFromMem.id) {
                    return objFromFile.value;
                } else {
                    return objFromMem.value;
                }
            } else {
                OrderedWrapper<E> vw = innerQueue.peekFromMemoryQueue();
                if (vw != null) {
                    return vw.value;
                } else {
                    return null;
                }
            }
        } finally {
            innerQueue.backendQueueLock.unlock();
        }
    }

    @Override
    public E take() throws InterruptedException {

        try {
            innerQueue.backendQueueLock.lock();
            if (innerQueue.backendQueue.size() > 0) {
                OrderedWrapper<E> objFromFile = innerQueue.backendQueue.peek();
                OrderedWrapper<E> objFromMem = innerQueue.peekFromMemoryQueue();
                if (objFromMem == null || objFromFile.id < objFromMem.id) {
                    innerQueue.backendQueue.remove();
                    return objFromFile.value;
                } else {
                    innerQueue.takeFromMemoryQueue();
                    return objFromMem.value;
                }
            } else {
                OrderedWrapper<E> ev = innerQueue.takeFromMemoryQueue();
                if (ev != null) {
                    return ev.value;
                } else {
                    return null;
                }
            }
        } catch (InterruptedException e) {
            return null;
        } finally {
            innerQueue.backendQueueLock.unlock();
        }
    }

    @Override
    public boolean add(E e) {
        throw new UnsupportedOperationException("Not work for now.");
    }

    @Override
    public boolean addAll(Collection<? extends E> collection) {
        throw new UnsupportedOperationException("Not work for now.");
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        throw new UnsupportedOperationException("Not work for now.");
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
        throw new UnsupportedOperationException("Not work for now.");
    }

    @Override
    public void clear() {
        innerQueue.clear();
    }

    @Override
    public E remove() {
        throw new UnsupportedOperationException("Not work for now.");
    }

    @Override
    public int remainingCapacity() {
        throw new UnsupportedOperationException("Not work for now.");
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("Not work for now.");
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        throw new UnsupportedOperationException("Not work for now.");
    }

    @Override
    public int size() {
        return innerQueue.size();
    }

    @Override
    public boolean isEmpty() {
        return innerQueue.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException("Not work for now.");
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException("Not work for now.");
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException("Not work for now.");
    }

    @Override
    public <T> T[] toArray(T[] ts) {
        throw new UnsupportedOperationException("Not work for now.");
    }

    @Override
    public int drainTo(Collection<? super E> collection) {
        throw new UnsupportedOperationException("Not work for now.");
    }

    @Override
    public int drainTo(Collection<? super E> collection, int i) {
        throw new UnsupportedOperationException("Not work for now.");
    }

    static class OrderedWrapper<E> {
        E value;
        long id;
    }

    static class OrderedWrapperConverter<E> implements FileObjectQueue.Converter<OrderedWrapper<E>> {
        private FileObjectQueue.Converter<E> origConverter;

        public OrderedWrapperConverter(FileObjectQueue.Converter<E> converter) {
            this.origConverter = converter;
        }

        @Override
        public OrderedWrapper<E> from(byte[] bytes) throws IOException {
            byte[] idBytes = Arrays.copyOfRange(bytes, 0, 8);
            byte[] dataBytes = Arrays.copyOfRange(bytes, 8, bytes.length);

            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(idBytes));
            long id = dis.readLong();

            OrderedWrapper<E> ow = new OrderedWrapper<E>();
            ow.id = id;
            ow.value = this.origConverter.from(dataBytes);

            return ow;
        }

        @Override
        public void toStream(OrderedWrapper<E> o, OutputStream bytes) throws IOException {
            DataOutputStream dos = new DataOutputStream(bytes);
            dos.writeLong(o.id);

            this.origConverter.toStream(o.value, bytes);
        }
    }
}

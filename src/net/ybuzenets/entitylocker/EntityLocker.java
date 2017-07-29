package net.ybuzenets.entitylocker;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An utility class designed to provide synchronization mechanism for entities.
 * EntityLocker allows to synchronize the execution of "protected code" on entities using their IDs
 *
 * @param <T> type of entity ID
 */
public class EntityLocker<T> {
    private final ConcurrentMap<T, Lock> lockStorage = new ConcurrentHashMap<>();

    /**
     * Executes protected code with synchronization on entity.
     * To avoid deadlocks, any thread that cannot acquire a lock in five seconds is interrupted
     *
     * @param entityId ID of entity which needs to be locked
     * @param callback code that needs to be executed
     * @throws InterruptedException if the current thread is interrupted while acquiring the lock
     * @see #executeWithLock(Object, Runnable, long, TimeUnit)
     */
    public void executeWithLock(T entityId, Runnable callback) throws InterruptedException {
        executeWithLock(entityId, callback, 5L, TimeUnit.SECONDS);
    }

    /**
     * Executes protected code with synchronization on entity.
     *
     * @param entityId ID of entity which needs to be locked
     * @param callback code that needs to be executed
     * @param timeOut  timeout after which thread is interrupted
     * @param timeUnit time unit of the {@code time} parameter
     * @throws InterruptedException if the current thread is interrupted while acquiring the lock
     */
    public void executeWithLock(T entityId, Runnable callback, long timeOut, TimeUnit timeUnit) throws InterruptedException {
        final Lock entityLock = getLock(entityId);
        if (entityLock.tryLock(timeOut, timeUnit)) {
            runSynchronized(callback, entityLock);
        } else {
            Thread.currentThread().interrupt();
        }
    }

    private void runSynchronized(Runnable callback, Lock entityLock) {
        try {
            callback.run();
        } finally {
            entityLock.unlock();
        }
    }

    private Lock getLock(T entityId) {
        return Optional.ofNullable(entityId)
            .map(id -> lockStorage.computeIfAbsent(id, lock -> new ReentrantLock()))
            .orElseThrow(() -> new IllegalArgumentException("Entity ID cannot be null"));
    }
}

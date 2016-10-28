package ru.ifmo.ctddev.paragen;

import info.kgeorgiy.java.advanced.mapper.ParallelMapper;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class ParallelMapperImpl implements ParallelMapper {

    private List<Thread> threadList;
    private List<CustomRunnable> tasks;
    private ReentrantLock lock;
    private Condition condition;

    public ParallelMapperImpl(int threads) {

        threadList = new ArrayList<>(threads);
        tasks = new LinkedList<>();
        Thread tmp;

        lock = new ReentrantLock();
        condition = lock.newCondition();

        for (int i = 0; i < threads; ++i) {
            tmp = new Thread(new HandlerRunnable(tasks, lock, condition));
            threadList.add(tmp);
        }

        threadList.forEach(Thread::start);
    }

    @Override
    public <T, R> List<R> map(Function<? super T, ? extends R> f, List<? extends T> args) throws InterruptedException {
        List<CustomRunnable<T, R>> runnables = new LinkedList<>();
        ReentrantLock localLock = new ReentrantLock();
        Condition localCondition = localLock.newCondition();
        for (T arg : args) {
            runnables.add(new CustomRunnable<>(f, arg, localLock, localCondition));
        }

        lock.lock();

        tasks.addAll(runnables);
        condition.signalAll();

        lock.unlock();


        List<R> res = new ArrayList<>(args.size());

        localLock.lock();
        for (CustomRunnable<T, R> curr :
                runnables) {
            while (curr.result() == null) {
                localCondition.await();
            }

            res.add(curr.result());

        }
        localLock.unlock();
        return res;
    }

    @Override
    public void close() throws InterruptedException {
        threadList.forEach(Thread::interrupt);
    }

    private class CustomRunnable<T, R> implements Runnable {

        Function<? super T, ? extends R> func;
        T data;
        R res;
        final ReentrantLock lock;
        final Condition condition;

        CustomRunnable(Function<? super T, ? extends R> func, T val, ReentrantLock lock, Condition condition) {
            data = val;
            this.func = func;
            this.lock = lock;
            this.condition = condition;
        }

        @Override
        public void run() {
            lock.lock();
            res = func.apply(data);
            condition.signal();
            lock.unlock();
        }

        R result() {
            return res;
        }
    }

    private class HandlerRunnable implements Runnable {

        List<CustomRunnable> list;
        ReentrantLock lock;
        Condition condition;

        HandlerRunnable(List<CustomRunnable> list, ReentrantLock lock, Condition condition) {
            this.list = list;
            this.lock = lock;
            this.condition = condition;
        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                Runnable tmp;
                lock.lock();
                while (list.size() == 0) {
                    try {
                        condition.await();
                    } catch (InterruptedException e) {
                        return;
                    }
                }
                tmp = list.remove(0);
                lock.unlock();

                tmp.run();

            }
        }
    }
}


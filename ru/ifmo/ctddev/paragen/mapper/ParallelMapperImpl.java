package ru.ifmo.ctddev.paragen;

import info.kgeorgiy.java.advanced.mapper.ParallelMapper;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

public class ParallelMapperImpl implements ParallelMapper {

    private List<Thread> threadList;
    private final List<CustomRunnable> tasks;


    public ParallelMapperImpl(int threads) {

        threadList = new ArrayList<>(threads);
        tasks = new LinkedList<>();
        Thread tmp;
        for (int i = 0; i < threads; ++i) {
            tmp = new Thread(new HandlerRunnable(tasks));
            threadList.add(tmp);
        }
        threadList.forEach(Thread::start);
    }

    @Override
    public <T, R> List<R> map(Function<? super T, ? extends R> f, List<? extends T> args) throws InterruptedException {
       
       	List<CustomRunnable<T, R>> runnables = new LinkedList<>();

        synchronized (tasks) {

        	for (T arg : args) {
            	runnables.add(new CustomRunnable<>(f, arg));
        	}

            tasks.addAll(runnables);
            tasks.notifyAll();

        }

        List<R> res = new ArrayList<R>(args.size());
        for (CustomRunnable<T, R> curr :
                runnables) {
            synchronized (curr) {
                while (curr.result() == null) {
                    curr.wait();
                }
            }

            res.add(curr.result());

        }

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

        CustomRunnable(Function<? super T, ? extends R> func, T val) {
            data = val;
            this.func = func;
        }

        @Override
        public void run() {
            res = func.apply(data);
        }

        R result() {
            return res;
        }
    }

    private class HandlerRunnable implements Runnable {

        List<CustomRunnable> list;

        HandlerRunnable(List<CustomRunnable> list) {
            this.list = list;
        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                Runnable tmp;
                synchronized (list) {

                    while (list.size() == 0) {
                        try {
                            list.wait();
                        } catch (InterruptedException e) {
                            return;
                        }
                    }
                    tmp = list.remove(0);
                }

                synchronized (tmp) {
                    tmp.run();
                    tmp.notify();
                }

            }
        }
    }
}


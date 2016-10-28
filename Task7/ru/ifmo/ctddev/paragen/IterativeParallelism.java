package ru.ifmo.ctddev.paragen;

import info.kgeorgiy.java.advanced.concurrent.ListIP;
import info.kgeorgiy.java.advanced.mapper.ParallelMapper;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class IterativeParallelism implements ListIP {

    private ParallelMapper mapper;

    public IterativeParallelism() {
    }

    public IterativeParallelism(ParallelMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public <T> T maximum(int threads, List<? extends T> values, Comparator<? super T> comparator) throws InterruptedException {
        return minimum(threads, values, comparator.reversed());
    }

    @Override
    public <T> T minimum(int threads, List<? extends T> values, Comparator<? super T> comparator) throws InterruptedException {
        List<T> list = new ArrayList<>(values);
        while (list.size() > 1) {
            list = runThreads(threads, list, objects -> {
                StringBuilder builder = new StringBuilder();
                return (T) objects.stream().min(comparator).get();
            });
            int fix = list.size() / 2;
            if (threads > fix) {
                threads = fix;
            }
        }
        return list.get(0);
    }

    @Override
    public <T> boolean all(int threads, List<? extends T> values, Predicate<? super T> predicate) throws InterruptedException {
        List<Boolean> ans = runThreads(threads, values, (Function<List<? extends T>, Boolean>) ts -> ts.stream().allMatch(predicate));

        return !ans.contains(Boolean.FALSE);
    }

    @Override
    public <T> boolean any(int threads, List<? extends T> values, Predicate<? super T> predicate) throws InterruptedException {
        List<Boolean> ans = runThreads(threads, values, (Function<List<? extends T>, Boolean>) ts -> ts.stream().anyMatch(predicate));

        return ans.contains(Boolean.TRUE);
    }

    @Override
    public String join(int threads, List<?> values) throws InterruptedException {
        List<?> list = new ArrayList<>(values);
        while (list.size() > 1) {
            list = runThreads(threads, list, (Function<List<?>, String>) objects -> {
                StringBuilder builder = new StringBuilder();
                objects.stream().map(Object::toString).forEach(builder::append);
                return builder.toString();
            });
            int fix = list.size() / 2;
            if (threads > fix) {
                threads = fix;
            }
        }
        return list.get(0).toString();
    }

    @Override
    public <T> List<T> filter(int threads, List<? extends T> values, Predicate<? super T> predicate) throws InterruptedException {
        List<List<T>> list = runThreads(threads, values, new Function<List<? extends T>, List<T>>() {
            @Override
            public List<T> apply(List<? extends T> ts) {
                return ts.stream().filter(predicate).collect(Collectors.toList());
            }
        });
        List<T> ans = new ArrayList<>();
        list.forEach(ans::addAll);
        return ans;
    }

    @Override
    public <T, U> List<U> map(int threads, List<? extends T> values, Function<? super T, ? extends U> f) throws InterruptedException {
        List<List<U>> ans = runThreads(threads, values, new Function<List<? extends T>, List<U>>() {
            @Override
            public List<U> apply(List<? extends T> ts) {
                return ts.stream().map(f).collect(Collectors.toList());

            }
        });


        return ans.stream().collect(ArrayList::new, ArrayList::addAll, ArrayList::addAll);

    }

    private <T, U> List<U> runThreads(int threads, List<? extends T> list, Function<List<? extends T>, U> func) throws InterruptedException {
        if (mapper != null) {
            withMapper(threads, list, func);
        }

        if (list.size() < threads) {
            threads = list.size();
        }
        Thread pool[] = new Thread[threads];
        List<MyRunnable<T, U>> runnables = new ArrayList<>(threads);
        List<U> res = new ArrayList<>();
        MyRunnable<T, U> tmp;
        int count = list.size() / threads;

        for (int i = 0, fi, li; i < threads; ++i) {
            fi = i * count;
            li = fi + count;
            if (i + 1 == threads) {
                li = list.size();
            }
            List<? extends T> subList = list.subList(fi, li);
            runnables.add(tmp = new MyRunnable<>(subList, func));
            pool[i] = new Thread(tmp);
            pool[i].start();
        }


        for (int i = 0; i < threads; ++i) {

            if (pool[i].isAlive()) {
                pool[i].join();
            }
            res.add(runnables.get(i).getResult());

        }
        return res;
    }

    private <T, U> List<U> withMapper(int threads, List<? extends T> list, Function<List<? extends T>, U> func) throws InterruptedException {
        if (threads > list.size()) {
            threads = list.size();
        }
        int count = list.size() / threads, fi, si;

        List<List<? extends T>> buf = new ArrayList<>(threads);

        for (int i = 0; i < threads; ++i) {
            fi = i * count;
            si = (i + 1 == threads) ? list.size() : fi + count;
            buf.add(list.subList(fi, si));
        }
        return mapper.map(func, buf);
    }


    private class MyRunnable<T, U> implements Runnable {

        List<? extends T> list;
        Function<List<? extends T>, U> func;
        U result;

        MyRunnable(List<? extends T> list, Function<List<? extends T>, U> func) {
            this.func = func;
            this.list = list;
        }

        @Override
        public void run() {
            result = func.apply(list);
        }

        U getResult() {
            return result;
        }
    }
}

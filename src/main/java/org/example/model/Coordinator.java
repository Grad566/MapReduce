package org.example.model;


import org.example.model.task.MapTask;
import org.example.model.task.ReduceTask;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class Coordinator {
    private final BlockingQueue<MapTask> mapTasks;
    private final BlockingQueue<ReduceTask> reduceTasks;
    /**
     * Служит для хранения промежуточных файлов,
     * в каждом бакете лежит список файлов для 1-ой задачи reduce,
     * ключом является id reduce задачи,
     * кол-во бакетов = кол-ву reduce задач.
     */
    private final ConcurrentHashMap<Integer, List<String>> temporaryFiles;
    /**
     * Нужен для синхронизации между переходом от выполнения map задач к reduce задачам.
     */
    private final CountDownLatch latch;
    /**
     * Нужен для синхронизации и равномерного распределения файлов в бакеты.
     */
    private final AtomicInteger completedMapTasks;

    public Coordinator(List<String> paths, int reduceTaskCount) {
        long count = 0;
        mapTasks = new ArrayBlockingQueue<>(paths.size());
        for (String path : paths) {
            mapTasks.add(new MapTask(count++, path));
        }

        temporaryFiles = new ConcurrentHashMap<>();
        for (int i = 0; i < reduceTaskCount; i++) {
            temporaryFiles.put(i, new ArrayList<>());
        }

        reduceTasks = new ArrayBlockingQueue<>(reduceTaskCount);
        for (long i = 0; i < reduceTaskCount; i++) {
            ReduceTask reduceTask = new ReduceTask();
            reduceTask.setId(i);
            reduceTasks.add(reduceTask);
        }

        latch = new CountDownLatch(paths.size());
        completedMapTasks = new AtomicInteger(0);
    }

    public MapTask getMaptask() {
        return mapTasks.poll();
    }

    /**
     * В момент получения задачи, она связывается с одним из списков файлов из temporaryFiles.
     * @return reduceTask
     */
    public ReduceTask getReducetask() {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        ReduceTask reduceTask = reduceTasks.poll();
        if (reduceTask != null) {
            int index = (int) (reduceTask.getId() % temporaryFiles.size());
            reduceTask.setFilePaths(temporaryFiles.get(index));
        }
        return reduceTask;

    }

    /**
     * Помещает временный файл в один из бакетов.
     * @param filePath - временный файл, в котором содержатся промежуточные вычисления.
     */
    public synchronized void mapTaskCompleteReport(String filePath) {
        int index = completedMapTasks.get() % temporaryFiles.size();
        temporaryFiles.get(index).add(filePath);

        completedMapTasks.incrementAndGet();
        latch.countDown();
    }
}


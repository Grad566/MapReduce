package org.example;


import org.example.model.Coordinator;
import org.example.model.Worker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) {
        var path1 = "input1.txt";
        var path2 = "input2.txt";
        var path3 = "input3.txt";
        var path4 = "input4.txt";

        var list = new ArrayList<>(List.of(path1, path2, path3, path4));
        var coord = new Coordinator(list, 2);

        int numberOfWorkers = 2;
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfWorkers);

        for (int i = 0; i < numberOfWorkers; i++) {
            executorService.submit(new Worker(coord));
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {}
    }
}

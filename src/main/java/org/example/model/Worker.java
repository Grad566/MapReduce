package org.example.model;

import lombok.AllArgsConstructor;
import org.example.exception.MapTaskException;
import org.example.exception.ReduceTaskException;
import org.example.model.task.MapTask;
import org.example.model.task.ReduceTask;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@AllArgsConstructor
public class Worker implements Runnable {
    private final Coordinator coordinator;

    @Override
    public void run() {
        while (true) {
            MapTask mapTask = coordinator.getMaptask();
            if (mapTask == null) {
                break;
            }

            String resultFilePath = produceMapTask(mapTask);
            coordinator.mapTaskCompleteReport(resultFilePath);
        }

        while (true) {
            ReduceTask reduceTask = coordinator.getReducetask();
            if (reduceTask == null) {
                break;
            }

            List<String> filePaths = reduceTask.getFilePaths();
            produceReduceTask(filePaths, reduceTask.getId());
        }
    }

    /**
     * Производит вычисления, записывает их в промежуточный файл и возвращает его название.
     * @param task - mapTask
     * @return имя промежуточного файла.
     */
    private String produceMapTask(MapTask task) {
        try {
            String[] content = getContent(task.getFileName());
            List<KeyValue> keyValues = getKeyValue(content);
            return writeToFile(keyValues, task.getId());
        } catch (IOException e) {
            throw new MapTaskException("Error processing map task: " + e.getMessage(), e);
        }
    }

    /**
     * Извлекает значения из промежуточных файлов, высчитывает их и помещает в treeMap.
     * @param paths список промежуточных файлов
     * @param taskId id reduceTask
     */
    private void produceReduceTask(List<String> paths, long taskId) {
        Map<String, Integer> result = new TreeMap<>();

        try {
            for (String path : paths) {
                List<KeyValue> keyValues = getKeyValueFromFile(path);
                for (KeyValue keyValue : keyValues) {
                    result.merge(keyValue.getKey(), 1, Integer::sum);
                }
            }

            writeToFinalFile(result, taskId);
        } catch (IOException e) {
            throw new ReduceTaskException("Error processing reduce task: " + e.getMessage(), e);
        }
    }

    private String[] getContent(String path) throws IOException {
        StringBuilder sb = new StringBuilder();

        try (BufferedReader in = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = in.readLine()) != null) {
                sb.append(line).append(" ");
            }
        }

        String content = sb.toString().trim();
        return content.split(" ");
    }

    private List<KeyValue> getKeyValueFromFile(String path) throws IOException {
        List<KeyValue> result = new ArrayList<>();

        try (BufferedReader in = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = in.readLine()) != null) {
                String[] arr = line.split(" ");
                result.add(new KeyValue(arr[0], arr[1]));
            }
        }

        return result;
    }

    private List<KeyValue> getKeyValue(String[] arr) {
        List<KeyValue> keyValues = new ArrayList<>();
        for (String word : arr) {
            keyValues.add(new KeyValue(word, "1"));
        }
        return keyValues;
    }

    private String writeToFile(List<KeyValue> keyValues, long taskId) throws IOException {
        String path = "mr-" + taskId + "-" + Thread.currentThread().getName() + ".txt";
        try (BufferedWriter out = new BufferedWriter(new FileWriter(path))) {
            for (KeyValue keyValue : keyValues) {
                out.write(keyValue.toString());
                out.newLine();
            }
        }

        return path;
    }

    private void writeToFinalFile(Map<String, Integer> map, long taskId) throws IOException {
        String path = "final-" + taskId + "-" + Thread.currentThread().getName() + ".txt";
        try (BufferedWriter out = new BufferedWriter(new FileWriter(path))) {
            for (var entry : map.entrySet()) {
                out.write(entry.toString());
                out.newLine();
            }
        }
    }
}



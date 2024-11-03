
import org.example.service.Coordinator;
import org.example.service.Worker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class MapReduceTest {
    private Coordinator coordinator;
    private final List<String> testFiles = Arrays.asList("src/test/resources/testfile1.txt",
            "src/test/resources/testfile2.txt");

    @BeforeEach
    public void setUp() {
        int reduceTaskCount = 2;
        coordinator = new Coordinator(testFiles, reduceTaskCount);
    }

    @AfterEach
    public void tearDown() {
        File resourcesDir = new File("src/test/resources");
        if (resourcesDir.exists() && resourcesDir.isDirectory()) {
            for (File file : Objects.requireNonNull(resourcesDir.listFiles())) {
                if (!file.isDirectory()) {
                    file.delete();
                }
            }
        }

        deleteFileIfExists("final-1-pool-1-thread-1.txt");
        deleteFileIfExists("final-1-pool-1-thread-2.txt");
        deleteFileIfExists("final-1-pool-2-thread-1.txt");
        deleteFileIfExists("final-1-pool-2-thread-2.txt");

        deleteFileIfExists("final-0-pool-1-thread-1.txt");
        deleteFileIfExists("final-0-pool-1-thread-2.txt");
        deleteFileIfExists("final-0-pool-2-thread-1.txt");
        deleteFileIfExists("final-0-pool-2-thread-2.txt");
    }

    @Test
    public void testMapReduce() {
        createTestFile("src/test/resources/testfile1.txt", "hello world");
        createTestFile("src/test/resources/testfile2.txt", "hello junit");

        int numberOfWorkers = 2;
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfWorkers);

        for (int i = 0; i < numberOfWorkers; i++) {
            executorService.submit(new Worker(coordinator));
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {}

        assertTrue(new File("final-1-pool-1-thread-1.txt").exists()
                || new File("final-1-pool-1-thread-2.txt").exists()
                || new File("final-1-pool-2-thread-1.txt").exists()
                || new File("final-1-pool-2-thread-2.txt").exists());
        assertTrue(new File("final-0-pool-1-thread-1.txt").exists()
                || new File("final-0-pool-1-thread-2.txt").exists()
                || new File("final-0-pool-2-thread-2.txt").exists()
                || new File("final-0-pool-2-thread-1.txt").exists());
    }

    @Test
    public void testGeneratedFilesContainPattern() {
        createTestFile("src/test/resources/testfile1.txt", "hello world");
        createTestFile("src/test/resources/testfile2.txt", "hello junit");

        int numberOfWorkers = 2;
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfWorkers);

        for (int i = 0; i < numberOfWorkers; i++) {
            executorService.submit(new Worker(coordinator));
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {}

        assertTrue(fileExistsAndContainsPattern("final-1-pool-1-thread-1.txt"));
        assertTrue(fileExistsAndContainsPattern("final-1-pool-1-thread-2.txt"));
        assertTrue(fileExistsAndContainsPattern("final-1-pool-2-thread-1.txt"));
        assertTrue(fileExistsAndContainsPattern("final-1-pool-2-thread-2.txt"));

        assertTrue(fileExistsAndContainsPattern("final-0-pool-1-thread-1.txt"));
        assertTrue(fileExistsAndContainsPattern("final-0-pool-1-thread-2.txt"));
        assertTrue(fileExistsAndContainsPattern("final-0-pool-2-thread-1.txt"));
        assertTrue(fileExistsAndContainsPattern("final-0-pool-2-thread-2.txt"));
    }

    private void createTestFile(String fileName, String content) {
        try {
            Files.write(Paths.get(fileName), content.getBytes());
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    private boolean fileExistsAndContainsPattern(String fileName) {
        File file = new File(fileName);
        if (!file.exists()) {
            return true; // значит, что эти данные были обработаны другим потоком
        }

        Pattern pattern = Pattern.compile("\\w+=\\d+");
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (pattern.matcher(line).matches()) {
                    return true;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException();
        }
        return false;
    }

    private void deleteFileIfExists(String fileName) {
        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }
    }

}

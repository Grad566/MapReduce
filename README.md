## MapReduce
Принимает на вход несколько текстовых файлов, на выходе несколько файлов, в кажом из которых список ключ-значение, где ключ - слово, а значение - кол-во вхождений в обработанных файлах.

### Требования для локального запуска
jdk - 21

gradle - 8.7

### Worker и Coordinator.
Coordinator принимает на вхож список файлов и кол-во reduceTask.
```
var path1 = "input1.txt";
var path2 = "input2.txt";
var path3 = "input3.txt";
var path4 = "input4.txt";

var list = new ArrayList<>(List.of(path1, path2, path3, path4));
var coord = new Coordinator(list, 2);
```
Worker принимает на вход Coordinator.
```
int numberOfWorkers = 2;
ExecutorService executorService = Executors.newFixedThreadPool(numberOfWorkers);

for (int i = 0; i < numberOfWorkers; i++) {
  executorService.submit(new Worker(coord));
}
```
### Запуск
Можно проверить работу программы в методе main.


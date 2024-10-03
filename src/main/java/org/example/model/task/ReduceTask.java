package org.example.model.task;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class ReduceTask {
    private long id;
    private List<String> filePaths;

    @Override
    public String toString() {
        return id + " " + filePaths.toString();
    }
}

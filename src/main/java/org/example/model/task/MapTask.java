package org.example.model.task;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class MapTask {
    private final Long id;
    private final String fileName;

    @Override
    public String toString() {
        return id + " " + fileName;
    }
}

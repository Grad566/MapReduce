package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class KeyValue {
    private String key;
    private String value;

    @Override
    public String toString() {
        return key + " " + value;
    }
}

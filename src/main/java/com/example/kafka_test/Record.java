package com.example.kafka_test;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Record {
    private long offset;
    private String value;



}

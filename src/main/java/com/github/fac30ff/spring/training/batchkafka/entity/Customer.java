package com.github.fac30ff.spring.training.batchkafka.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Customer {
  private Long id;
  private String name;
}

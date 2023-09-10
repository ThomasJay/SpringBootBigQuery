package com.thomasjayconsulting.javabigquery.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Flight {
    private String origin;
    private String destination;
    private String airline;
    private Integer stops;
    private Double price;
}

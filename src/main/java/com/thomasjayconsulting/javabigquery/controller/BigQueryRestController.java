package com.thomasjayconsulting.javabigquery.controller;

import com.thomasjayconsulting.javabigquery.model.Flight;
import com.thomasjayconsulting.javabigquery.model.FlightHolder;
import com.thomasjayconsulting.javabigquery.service.BigQueryService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1")
@Slf4j
public class BigQueryRestController {

    @Autowired
    BigQueryService bigQueryService;

    @GetMapping("/querydb")
    public FlightHolder querydb() {
        log.info("Starting querydb");
        return bigQueryService.basicQuery();
    }

    @GetMapping("/insertdb")
    public String insertdb() {
        log.info("Starting insertdb");
        return bigQueryService.insertdb();
    }


}

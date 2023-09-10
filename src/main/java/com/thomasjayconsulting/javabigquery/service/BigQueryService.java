package com.thomasjayconsulting.javabigquery.service;

import com.google.cloud.bigquery.*;
import com.thomasjayconsulting.javabigquery.model.Flight;
import com.thomasjayconsulting.javabigquery.model.FlightHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@Slf4j
public class BigQueryService {

    private static final String projectId = "chatsearchme";
    private static final String datasetId = "Flights";

    public FlightHolder basicQuery() {
        log.info("basicQuery Started");

        FlightHolder flightHolder = new FlightHolder();

        List<Flight> flights = new ArrayList<Flight>();

        flightHolder.setFlights(flights);
        flightHolder.setCount(0);
        flightHolder.setStatus("Fail");


        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(
                                "SELECT * "
                                        + "FROM `chatsearchme.Flights.sfo_jfk_flights` "
                                        + "LIMIT 1000")
                        // Use standard SQL syntax for queries.
                        // See: https://cloud.google.com/bigquery/sql-reference/
                        .setUseLegacySql(false)
                        .build();

        String jobIdStr = UUID.randomUUID().toString();

        log.info("service gctj jobIdStr: " + jobIdStr);

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(jobIdStr);

        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());


        try {
            // Wait for the query to complete.
            queryJob = queryJob.waitFor();

            if (queryJob == null) {
                log.error("Job no longer exists!");
                return flightHolder;
            }

            if (queryJob.getStatus().getError() != null) {
                log.error("Job Failed!");
                return flightHolder;
            }

            // Get the results.
            TableResult result = queryJob.getQueryResults();

            double total = 0.0;

            for (FieldValueList row : result.iterateAll()) {
                String origin = row.get("Origin").getStringValue();
                String destination = row.get("Destination").getStringValue();
                String airline = row.get("Airline").getStringValue();
                int stops = row.get("Stops").getNumericValue().intValue();
                double price = row.get("Price").getDoubleValue();

                Flight flight = new Flight(origin, destination, airline, stops, price);
                flights.add(flight);

                log.info("Data origin: " + origin + " destination: " + destination + " airline: " + airline + " stops: " + stops + " price: " + price);
                total = total + price;

            }

            log.info("Total: " + total);

            flightHolder.setStatus("Success");
            flightHolder.setCount(flights.size());


        }
        catch (Exception e) {
            log.error("Exception e: " + e.getMessage());
            return flightHolder;
        }

        return flightHolder;
    }

    public String insertdb() {

        log.info("insertdb started");

        // Step 1: Initialize BigQuery service
        BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId)
                .build().getService();

        // Step 2: Create insertAll (streaming) request
        InsertAllRequest insertAllRequest = getInsertRequest();

        // Step 3: Insert data into table
        InsertAllResponse response = bigquery.insertAll(insertAllRequest);



        return "Insert Completed";
    }

    private InsertAllRequest getInsertRequest() {
        String tableId = "sfo_jfk_flights";

        InsertAllRequest.Builder builder =  InsertAllRequest.newBuilder(datasetId, tableId);

        builder.addRow(createFlightMap("SFO", "JFK", "AA", 5, 125.0));
        builder.addRow(createFlightMap("SFO", "JFK", "DL", 6, 155.0));

        return builder.build();

    }

    private Map<String, Object> createFlightMap(String origin, String destination, String airline, Integer stops, Double price) {

        Map<String, Object> rowMap = new HashMap<String, Object>();
        rowMap.put("Origin", origin);
        rowMap.put("Destination", destination);
        rowMap.put("Airline", airline);
        rowMap.put("Stops", stops);
        rowMap.put("Price", price);

        return rowMap;
    }


    }

package com.controller.collectors;

import com.controller.util.ValidationUtils;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InvalidObjectException;

/**
 * Test for DBCollectorTest.
 * @author Shuli
 */
public class DBCollectorTest {
    @Test
    public void mysqlOutputTest() throws IOException, ProcessingException {
        File schemaFile = new File("/vagrant/ottertune/controller/src/main/java/com/controller/schema.json");
        File knobsJson = new File("/vagrant/ottertune/controller/output/mysql/knobs.json");
        File metricsAfterJson = new File("/vagrant/ottertune/controller/output/mysql/metrics_after.json");
        File metricsBeforeJson = new File("/vagrant/ottertune/controller/output/mysql/metrics_before.json");

        if(!ValidationUtils.isJsonValid(schemaFile, knobsJson)) {
            throw new InvalidObjectException("invalid knobs json output file");
        }
        if(!ValidationUtils.isJsonValid(schemaFile, metricsAfterJson)) {
            throw new InvalidObjectException("invalid metrics_after json output file");
        }
        if(!ValidationUtils.isJsonValid(schemaFile, metricsBeforeJson)) {
            throw new InvalidObjectException("invalid metrics_before json output file");
        }
    }

    @Test
    public void postgresOutputTest() throws IOException, ProcessingException {
        File schemaFile = new File("/vagrant/ottertune/controller/src/main/java/com/controller/schema.json");
        File knobsJson = new File("/vagrant/ottertune/controller/output/postgres/knobs.json");
        File metricsAfterJson = new File("/vagrant/ottertune/controller/output/postgres/metrics_after.json");
        File metricsBeforeJson = new File("/vagrant/ottertune/controller/output/postgres/metrics_before.json");

        if(!ValidationUtils.isJsonValid(schemaFile, knobsJson)) {
            throw new InvalidObjectException("invalid knobs json output file");
        }
        if(!ValidationUtils.isJsonValid(schemaFile, metricsAfterJson)) {
            throw new InvalidObjectException("invalid metrics_after json output file");
        }
        if(!ValidationUtils.isJsonValid(schemaFile, metricsBeforeJson)) {
            throw new InvalidObjectException("invalid metrics_before json output file");
        }
    }

    @Test
    public void mockJsonOutput1Test() throws IOException, ProcessingException {
        File schemaFile = new File("/vagrant/ottertune/controller/src/main/java/com/controller/schema.json");
        File mockJsonFile = new File("/vagrant/ottertune/controller/src/test/java/com/controller/collectors/mockJsonOutput2.json");

        if(ValidationUtils.isJsonValid(schemaFile, mockJsonFile)) {
            throw new InvalidObjectException("the mock json output file should be invalid!");
        }
    }


}
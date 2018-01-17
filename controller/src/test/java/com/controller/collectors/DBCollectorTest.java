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
        File jsonFile = new File("/vagrant/ottertune/controller/output/mysql/knobs.json");

        if (!ValidationUtils.isJsonValid(schemaFile, jsonFile)) {
            throw new InvalidObjectException("invalid json output file");
        }
    }
        

}
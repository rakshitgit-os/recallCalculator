package com.intel.pgres;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;

@RestController
public class RecallCalculatorController {

    @Autowired
    private RecallCalculator recallCalculator;

    @GetMapping("/recall/{indexType}")
    public BigDecimal getRecall(@PathVariable("indexType") String indexType) throws SQLException, ClassNotFoundException {
        BigDecimal bb=  recallCalculator.calculateRecall(indexType);

        try {
            // Now calling Files.writeString() method
            // with path , content & standard charsets
            Files.writeString(Path.of("/home/intel/recallValue.txt"), bb.toBigInteger().toString(),
                    StandardCharsets.UTF_8);
        }

        // Catch block to handle the exception
        catch (IOException ex) {
            // Print messqage exception occurred as
            // invalid. directory local path is passed
            System.out.print("Invalid Path");
        }

        return  bb;

    }
}

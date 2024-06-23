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

    @GetMapping("/recall/{indexType}/{dim}")
    public BigDecimal getRecall(@PathVariable("indexType") String indexType, @PathVariable("dim") int dim) throws SQLException, ClassNotFoundException {
        BigDecimal bb=  recallCalculator.calculateRecall(indexType, dim);

        return  bb;

    }
}

package com.intel.pgres;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.sql.SQLException;

@RestController
public class RecallCalculatorController {

    @Autowired
    private RecallCalculator recallCalculator;

    @GetMapping("/recall/{indexType}")
    public BigDecimal getRecall(@PathVariable("indexType") String indexType) throws SQLException, ClassNotFoundException {
        return recallCalculator.calculateRecall(indexType);
    }
}

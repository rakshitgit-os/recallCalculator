package com.intel.pgres;

import com.pgvector.PGvector;
import jakarta.annotation.PostConstruct;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class RecallCalculator {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${queryEmbedding}")
    private String queryStr;

    public BigDecimal calculateRecall(final String indexType, final int dim) throws ClassNotFoundException, SQLException {
        //pgvtest

        List<String> queries = getQueryEmbedding();

        return new BigDecimal(getRecallAverages(getRecall(queries, indexType, dim)));
    }

    private double getRecallAverages(List<BigDecimal> recallValues){

        return recallValues.stream().filter((recall) -> recall != null).mapToDouble((recall) -> recall.doubleValue()).average().getAsDouble();
    }


    private List<List<List<Float>>> getNearestNeighbours(List<String> queries, String indexType, int dim) {
        List<List<List<Float>>> nearestNeighbors = new LinkedList<>();
        for(String query : queries) {
            Float[] queryEmbedding = convertEmbeddingStrToFloatArr(query, dim);

            // get the query embedding
            float[] queryEmbeddingVector = new float[dim];
            for (int i = 0; i < dim; i++) {
                queryEmbeddingVector[i] = queryEmbedding[i].floatValue();
            }

            List<List<Float>> resultEmbeddings = getNearestNeighbours(queryEmbeddingVector, indexType, dim);
            nearestNeighbors.add(resultEmbeddings);
        }
        return nearestNeighbors;
    }

    private List<BigDecimal> getRecall(List<String> queries, String indexType, int dim) {

        List<List<List<Float>>> embeddingsListWithIndex = getNearestNeighbours(queries, indexType, dim);

        // query without index
        if (indexType.equalsIgnoreCase("fp32")) {
            jdbcTemplate.execute("DROP INDEX IF EXISTS vecs_embedding_idx;");
        }

        List<List<List<Float>>> embeddingsListWithoutIndex = getNearestNeighbours(queries, "fp32", dim);
        List<BigDecimal> recallValues = new LinkedList<>();

        for (int i = 0; i < embeddingsListWithIndex.size(); i++) {
            List<List<Float>> queryResultWithIndex = embeddingsListWithIndex.get(i);
            List<List<Float>> queryResultWithoutIndex = embeddingsListWithoutIndex.get(i);

            long count = queryResultWithIndex.stream().filter(result -> queryResultWithoutIndex.contains(result)).collect(Collectors.toList()).stream().count();

            BigDecimal actualNoOfNearestNeighbours = new BigDecimal(queryResultWithoutIndex.size());
            BigDecimal noOfAppxNearestNeighboursInActualSet = new BigDecimal(count);
            BigDecimal recall = noOfAppxNearestNeighboursInActualSet.divide(actualNoOfNearestNeighbours, 4, RoundingMode.CEILING);
            recallValues.add(recall);

            System.out.println("without index is " + queryResultWithoutIndex.size());
            System.out.println("with index is is " + queryResultWithIndex.size());
            System.out.println("count is " + count);
            System.out.println("recall is " + recall.doubleValue());
        }

        return recallValues;
    }

    private Float[] convertEmbeddingStrToFloatArr(String embedding, int dim) {
        //Float[] actualEmbedding = new Float[128];
        Float[] actualEmbedding = new Float[dim];
        String[] embeddingStrArr = embedding.split(",");
        int length = embeddingStrArr.length;
        embeddingStrArr[0] = embeddingStrArr[0].substring(1, embeddingStrArr[0].length());
        embeddingStrArr[length - 1] = embeddingStrArr[length - 1 ].substring( 0, embeddingStrArr[length - 1 ].length() - 1);

        for(int i=0; i <length; i++) {
            actualEmbedding[i] = Float.parseFloat(embeddingStrArr[i]);
        }

        return actualEmbedding;
    }

    private List<List<Float>> getNearestNeighbours(float[] queryEmbedding, String indexType, int dim){
        Object[] queryEmbeddingVector = new Object[] { new PGvector(queryEmbedding) };
        List<Map<String, Object>> rows = null;
        if (indexType.equalsIgnoreCase("fp32")) {
            rows = jdbcTemplate.queryForList("SELECT id, embedding FROM vecs ORDER BY embedding <-> ? LIMIT 20", queryEmbeddingVector);
        } else if (indexType.equalsIgnoreCase("fp16")){
            rows = jdbcTemplate.queryForList("SELECT id, embedding FROM vecs ORDER BY embedding::halfvec("+dim+") <-> (?)::halfvec("+dim+") LIMIT 20", queryEmbeddingVector);
            //rows= jdbcTemplate.queryForList("SELECT id, embedding FROM vecs ORDER BY embedding::halfvec(128) <-> (select CAST(? AS halfvec)) LIMIT 40", neighborParams);
        } else {
            throw new IllegalArgumentException("invalid index type. It must be fp16 or fp32");
        }

        List<List<Float>> nearestNeighbours= new LinkedList<>();
        for (Map row : rows) {

            PGobject pGobject = (PGobject) row.get("embedding");
            String str = pGobject.getValue();
            //System.out.println("embeddings are " + str);
            Float[] nearestNeighbour = convertEmbeddingStrToFloatArr(str, dim);
            nearestNeighbours.add(Arrays.asList(nearestNeighbour));
        }
        return  nearestNeighbours;
    }


    private List<String> getQueryEmbedding(){

        List<Map<String, Object>> rows = jdbcTemplate.queryForList("SELECT id, embedding FROM vecs LIMIT 20");
        List<String> queryEmbeddings = new LinkedList<>();

        for (Map row : rows) {

            PGobject pGobject = (PGobject) row.get("embedding");
            String str = pGobject.getValue();
            //System.out.println("embeddings are " + str);
            queryEmbeddings.add(str);
        }
        return queryEmbeddings;
    }
}
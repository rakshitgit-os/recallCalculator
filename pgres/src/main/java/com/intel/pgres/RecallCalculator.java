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

@Component
public class RecallCalculator {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${queryEmbedding}")
    private String queryStr;

    //@PostConstruct
    public BigDecimal calculateRecall(final String indexType) throws ClassNotFoundException, SQLException {
        //pgvtest

        String query = getQueryEmbedding();
        Float [] queryEmbedding = convertEmbeddingStrToFloatArr(query);

        // get the query embedding
        float [] queryEmbeddingVector = new float[1536];
        for (int i = 0; i < 1536; i ++) {
            queryEmbeddingVector[i] =queryEmbedding[i].floatValue();
        }


        List<List<Float>> embeddingsListWithIndex = getNearestNeighbours(queryEmbeddingVector, indexType);

        // query without index
        //jdbcTemplate.execute("DROP INDEX IF EXISTS vecs_embedding_idx;");
        List<List<Float>> embeddingsListWithoutIndex = getNearestNeighbours(queryEmbeddingVector, "fp32");


        int count = 0;
        // calculate recall
        for (int i=0; i < embeddingsListWithIndex.size(); i ++){
            if(embeddingsListWithoutIndex.contains(embeddingsListWithIndex.get(i))){
                count ++;
            }
        }

        BigDecimal actualNoOfNearestNeighbours = new BigDecimal(embeddingsListWithoutIndex.size());
        BigDecimal noOfAppxNearestNeighboursInActualSet = new BigDecimal(count);
        BigDecimal recall = noOfAppxNearestNeighboursInActualSet.divide(actualNoOfNearestNeighbours, 4, RoundingMode.CEILING);

        System.out.println("without index is " + embeddingsListWithoutIndex.size());
        System.out.println("with index is is " + embeddingsListWithIndex.size());
        System.out.println("count is " + count);
        System.out.println("recall is " + recall);
        return  recall;

    }

    private Float[] convertEmbeddingStrToFloatArr(String embedding) {
        //Float[] actualEmbedding = new Float[128];
        Float[] actualEmbedding = new Float[1536];
        String[] embeddingStrArr = embedding.split(",");
        int length = embeddingStrArr.length;
        embeddingStrArr[0] = embeddingStrArr[0].substring(1, embeddingStrArr[0].length());
        embeddingStrArr[length - 1] = embeddingStrArr[length - 1 ].substring( 0, embeddingStrArr[length - 1 ].length() - 1);

        for(int i=0; i <length; i++){
            actualEmbedding[i] = Float.parseFloat(embeddingStrArr[i]);
        }

        return actualEmbedding;
    }

    private List<List<Float>> getNearestNeighbours(float[] queryEmbedding, String indexType){
        Object[] queryEmbeddingVector = new Object[] { new PGvector(queryEmbedding) };
        List<Map<String, Object>> rows = null;
        if (indexType.equalsIgnoreCase("fp32")) {
            rows = jdbcTemplate.queryForList("SELECT id, embedding FROM vecs ORDER BY embedding <-> ? LIMIT 20", queryEmbeddingVector);
        } else if (indexType.equalsIgnoreCase("fp16")){
            rows = jdbcTemplate.queryForList("SELECT id, embedding FROM vecs ORDER BY embedding::halfvec(1536) <-> (?)::halfvec(1536) LIMIT 20", queryEmbeddingVector);
            //rows= jdbcTemplate.queryForList("SELECT id, embedding FROM vecs ORDER BY embedding::halfvec(128) <-> (select CAST(? AS halfvec)) LIMIT 40", neighborParams);
        } else {
            throw new IllegalArgumentException("invalid index type. It must be fp16 or fp32");
        }

        List<List<Float>> nearestNeighbours= new LinkedList<>();
        for (Map row : rows) {

            PGobject pGobject = (PGobject) row.get("embedding");
            String str = pGobject.getValue();
            System.out.println("embeddings are " + str);
            Float[] nearestNeighbour = convertEmbeddingStrToFloatArr(str);
            nearestNeighbours.add(Arrays.asList(nearestNeighbour));
        }
        return  nearestNeighbours;
    }

    private String getQueryEmbedding(){

        List<Map<String, Object>> rows = jdbcTemplate.queryForList("SELECT id, embedding FROM vecs LIMIT 10");
        List<String> queryEmbeddings = new LinkedList<>();

        for (Map row : rows) {

            PGobject pGobject = (PGobject) row.get("embedding");
            String str = pGobject.getValue();
            //System.out.println("embeddings are " + str);
            queryEmbeddings.add(str);
        }
        return queryEmbeddings.get(5);
    }
}
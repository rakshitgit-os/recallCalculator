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

    @PostConstruct
    public void calculateRecall() throws ClassNotFoundException, SQLException {
        //pgvtest

        float [] queryEmbedding = new float[1536];
        /*for (int i = 0; i < 128; i ++) {
            queryEmbedding[i] =0.01f; //recall was 0.77
        }*/

        // recall was 0.95, 0.90 for 22 nearest neighbours for 128 for full vector
        //recall was 0.85, 0.8858 , 0.91(31 out of 35 was found), 0.9143(32 out of 35 found) for 35 nearest neiughbours for 128 full vector
        String query = queryStr;
        Float [] queryEm = convertEmbeddingStrToFloatArr(query);

        for (int i = 0; i < 1536; i ++) {
            queryEmbedding[i] =queryEm[i].floatValue();
        }

        // query without index
        jdbcTemplate.execute("DROP INDEX IF EXISTS vecs_embedding_idx;");
        List<List<Float>> embeddingsListWithoutIndex = getNearestNeighbours(queryEmbedding);

        //query with index
        jdbcTemplate.execute("CREATE INDEX ON vecs USING hnsw(embedding vector_l2_ops) WITH (m = 16, ef_construction = 64)");
        jdbcTemplate.execute("set hnsw.efSearch = 64");
        List<List<Float>> embeddingsListWithIndex = getNearestNeighbours(queryEmbedding);

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

        System.out.println("actual is " + embeddingsListWithoutIndex.size());
        System.out.println("count is " + count);
        System.out.println("recall is " + recall);

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

    private List<List<Float>> getNearestNeighbours(float[] queryEmbedding){
        Object[] neighborParams = new Object[] { new PGvector(queryEmbedding) };
        List<Map<String, Object>> rows = jdbcTemplate.queryForList("SELECT id, embedding FROM vecs ORDER BY embedding <-> ? LIMIT 50", neighborParams);

        //List<Map<String, Object>> rows = jdbcTemplate.queryForList("SELECT id, embedding FROM vecs ORDER BY embedding::halfvec(1356) <-> (select CAST(? AS halfvec)) LIMIT 5", neighborParams);
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
}


    /*List<Float> f1 = embeddingsListWithoutIndex.get(0);
    List<Float> f2 = embeddingsListWithIndex.get(0);

    boolean b = embeddingsListWithoutIndex.contains(embeddingsListWithIndex.get(0));
        b = f1.equals(f2);

                for(int i=0; i< 128; i++){

        if(f1.get(i).floatValue() != f2.get(i).floatValue()){
        System.out.println("not expected");
        }
        }
        if (b) {

        }*/

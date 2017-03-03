package com.phynum.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONArray;


import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

/**
 * Created by thomasdutta on 3/1/17.
 */


public class ElasticLoader {

//    public static Logger logger = LoggerFactory.getLogger("ElasticLoader");

    public void doThings(JSONArray jsonArray) throws UnknownHostException {

        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .put("client.transport.sniff", true).build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

        IndicesAdminClient indicesAdminClient = client.admin().indices();

        boolean exists = indicesAdminClient
                .prepareExists("chicagodata")
                .execute().actionGet().isExists();

        if(exists) {
            System.out.println("Index already exists");

        }else {
            indicesAdminClient.prepareCreate("chicagodata").get();
            System.out.println("Index created");
        }


        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();


        ObjectMapper objectMapper = new ObjectMapper();
        for(int i=0; i<jsonArray.length(); i++) {
//            try {
//                String stringifiedJson = objectMapper.writeValueAsString(jsonArray.get(i));
                bulkRequestBuilder.add(client.prepareIndex("chicagodata", "record").setSource(jsonArray.get(i).toString()));

//            } catch (JsonProcessingException e) {
//                e.printStackTrace();
//            }
            if(i%1000 == 0) {
                System.out.println(i+" records processed");
            }

        }
        BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
        System.out.println(bulkResponse.getIngestTookInMillis());
        // on shutdown
        client.close();

    }
}

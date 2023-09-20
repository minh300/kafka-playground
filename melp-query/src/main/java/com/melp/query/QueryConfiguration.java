package com.melp.query;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.melp.query.es.ESClient;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;



@Configuration
public class QueryConfiguration {
	  @Bean
	  ApplicationRunner run() {
		    return args -> {

		    };
	  }
	  
	  @Bean
	  ESClient esClient() {
		  System.out.println("came here");
			String serverUrl = "http://localhost:9200";
			RestClient restClient = RestClient
			    .builder(HttpHost.create(serverUrl))
			    .build();

			// Create the transport with a Jackson mapper
			ElasticsearchTransport transport = new RestClientTransport(
			    restClient, new JacksonJsonpMapper());

			// And create the API client
			ElasticsearchClient esClient = new ElasticsearchClient(transport);
			ESClient client = new ESClient(esClient);
			return client;
	  }
}

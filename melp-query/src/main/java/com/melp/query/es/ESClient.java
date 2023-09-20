package com.melp.query.es;


import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.melp.avro.Business;
import com.melp.avro.Business_ES;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchAllQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.*;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.util.ObjectBuilder;
import com.fasterxml.jackson.databind.JsonNode;

public class ESClient implements IESClient {
	  private static final Logger log = LoggerFactory.getLogger(ESClient.class);

	private final ElasticsearchClient  client;
	
	public ESClient(final ElasticsearchClient client) {
		this.client = client;		
	}
	
	public List<JsonNode> findBusinessesWithin(final double top, final double left, final double bottem, final double right) throws IOException {
		SearchResponse<JsonNode> search = client.search(s -> s
			    .index("business_es")
			    .query(q -> q
				.bool(b -> b.must(m -> m.matchAll(ma -> ma))
				.filter(f -> f
						.geoBoundingBox(box -> box
								.field("location").boundingBox(bb -> bb
										.coords(c -> c.top(top).left(left).bottom(bottem).right(right))))))),
			    JsonNode.class);
		//TODO sort?
		List<Hit<JsonNode>> hits = search.hits().hits();
//		for (Hit<JsonNode> hit: hits) {
//			System.out.println(hit);
////			JsonNode business = hit.source();
////			System.out.println(business);
//		}
		return search.hits().hits().stream().map(a -> a.source()).toList();
	}
	

	@Override
	public List<JsonNode> findBusinesses(final String query) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
	
	public static void main(String[] args) throws IOException {
		// URL and API key
		String serverUrl = "http://localhost:9200";
		String apiKey = "VnVhQ2ZHY0JDZGJrU...";

		// Create the low-level client
		RestClient restClient = RestClient
		    .builder(HttpHost.create(serverUrl))
//		    .setDefaultHeaders(new Header[]{
//		        new BasicHeader("Authorization", "ApiKey " + apiKey)
//		    })
		    .build();

		// Create the transport with a Jackson mapper
		ElasticsearchTransport transport = new RestClientTransport(
		    restClient, new JacksonJsonpMapper());

		// And create the API client
		ElasticsearchClient esClient = new ElasticsearchClient(transport);
		ESClient client = new ESClient(esClient);
		client.findBusinessesWithin(50.73, -120.1, 40.01, -71.12);
	}

}

package com.melp.query.controller;

import java.io.IOException;
import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.JsonNode;
import com.melp.query.es.IESClient;

@RestController
@RequestMapping("/query/business")
public class BusinessQueryController {
	
	private final IESClient client;
	
	public BusinessQueryController(final IESClient client) {
		this.client = client;
	}
	
	@GetMapping(value="/closest")
	@ResponseBody
	public List<JsonNode> queryClosestBusinesses(
			@RequestParam("lat") final long latitude,
			@RequestParam("long") final long longitude, @RequestParam("radius") final long radius) throws IOException {
		
		return client.findBusinessesWithin(50.73, -120.1, 40.01, -71.12);
		
	}
	
	@GetMapping
	@ResponseBody
	public List<JsonNode> queryBusinesses(@RequestParam("query") final String query) throws IOException {
		
		return client.findBusinesses(query);
		
	}
}

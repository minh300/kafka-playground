package com.melp.query.es;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

public interface IESClient {
	public List<JsonNode> findBusinessesWithin(final double top, final double left, final double bottem, final double right) throws IOException;
	public List<JsonNode> findBusinesses(final String query) throws IOException;

}

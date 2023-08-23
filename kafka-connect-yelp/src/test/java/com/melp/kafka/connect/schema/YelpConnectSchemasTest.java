package com.melp.kafka.connect.schema;


import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class YelpConnectSchemasTest {
	ObjectMapper mapper = new ObjectMapper();

	@Test
	public void buildBusinessStructNullHoursTest()
			throws JsonMappingException, JsonProcessingException {
		String event = "{\"business_id\":\"QjPqXSqb9aiBPeRJ91mgnQ\",\"name\":\"Pure Bliss Wellness Center & Spa\","
				+ "\"address\":\"1704 Walnut St\",\"city\":\"Philadelphia\",\"state\":\"PA\",\"postal_code\":\"19103\","
				+ "\"latitude\":39.9498377,\"longitude\":-75.1694777,\"stars\":3.5,\"review_count\":8,\"is_open\":0,"
				+ "\"attributes\":{\"BusinessAcceptsCreditCards\":\"True\",\"ByAppointmentOnly\":\"True\","
				+ "\"BusinessParking\":\"{'garage': False, 'street': True, 'validated': False, 'lot': False, 'valet': False}\""
				+ ",\"RestaurantsPriceRange2\":\"2\"},\"categories\":\"Day Spas, Beauty & Spas\",\"hours\":null}";
		JsonNode eventAsJsonNode = mapper.readTree(event);

		assertNotNull(YelpConnectSchemas.buildBusinessStruct(eventAsJsonNode));
	}
	
	@Test
	public void buildBusinessStructHoursAttrTest() throws JsonMappingException, JsonProcessingException {
		String event = "{\"business_id\":\"ZXtiw_Ldhl-1HgsbJSLBHg\",\"name\":\"Walgreens\",\"address\":\"13 N Black Horse Pike\","
				+ "\"city\":\"Williamstown\",\"state\":\"NJ\",\"postal_code\":\"08094\",\"latitude\":39.6891882,\"longitude\":-74.9937592,"
				+ "\"stars\":2.0,\"review_count\":12,\"is_open\":1,\"attributes\":{\"BikeParking\":\"False\",\"Caters\":\"False\","
				+ "\"RestaurantsPriceRange2\":\"2\",\"ByAppointmentOnly\":\"False\",\"BusinessAcceptsCreditCards\":\"True\","
				+ "\"RestaurantsTakeOut\":\"True\",\"BusinessParking\":\"{u'valet': False, u'garage': None, u'street': None, u'lot': True, u'validated': None}\","
				+ "\"DriveThru\":\"True\",\"RestaurantsDelivery\":\"False\"},\"categories\":\"Food, Convenience Stores, Drugstores, "
				+ "Beauty & Spas, Cosmetics & Beauty Supply, Shopping\",\"hours\":{\"Monday\":\"6:0-0:0\",\"Tuesday\":\"6:0-0:0\",\"Wednesday\":\"6:0-0:0\",\"Thursday\":\"6:0-0:0\","
				+ "\"Friday\":\"6:0-0:0\",\"Saturday\":\"6:0-0:0\",\"Sunday\":\"6:0-0:0\"}}";
		JsonNode eventAsJsonNode = mapper.readTree(event);

		assertNotNull(YelpConnectSchemas.buildBusinessStruct(eventAsJsonNode));
	}
	
	@Test
	public void buildBusinessStructHoursNullAttrTest() throws JsonMappingException, JsonProcessingException {
		String event = "{\"business_id\":\"ZXtiw_Ldhl-1HgsbJSLBHg\",\"name\":\"Walgreens\",\"address\":\"13 N Black Horse Pike\","
				+ "\"city\":\"Williamstown\",\"state\":\"NJ\",\"postal_code\":\"08094\",\"latitude\":39.6891882,\"longitude\":-74.9937592,"
				+ "\"stars\":2.0,\"review_count\":12,\"is_open\":1,\"attributes\":{\"BikeParking\":\"False\",\"Caters\":\"False\","
				+ "\"RestaurantsPriceRange2\":\"2\",\"ByAppointmentOnly\":\"False\",\"BusinessAcceptsCreditCards\":\"True\","
				+ "\"RestaurantsTakeOut\":\"True\",\"BusinessParking\":\"{u'valet': False, u'garage': None, u'street': None, u'lot': True, u'validated': None}\","
				+ "\"DriveThru\":\"True\",\"RestaurantsDelivery\":\"False\"},\"categories\":\"Food, Convenience Stores, Drugstores, "
				+ "Beauty & Spas, Cosmetics & Beauty Supply, Shopping\",\"hours\":{\"Monday\":null, \"Wednesday\":\"6:0-0:0\",\"Thursday\":\"6:0-0:0\","
				+ "\"Friday\":\"6:0-0:0\",\"Saturday\":\"6:0-0:0\",\"Sunday\":\"6:0-0:0\"}}";
		JsonNode eventAsJsonNode = mapper.readTree(event);

		assertNotNull(YelpConnectSchemas.buildBusinessStruct(eventAsJsonNode));
	}
	
	@Test
	public void nullCategoriesTest() throws JsonMappingException, JsonProcessingException {
		String event = "{\"business_id\":\"NoU_2sLsdgUxAf9S7vuCQg\",\"name\":\"Food Bazaar\",\"address\":\"1500 Locust St, Ste 5\","
				+ "\"city\":\"Philadelphia\",\"state\":\"PA\",\"postal_code\":\"19102\",\"latitude\":39.9482528,\"longitude\":-75.1666834,"
				+ "\"stars\":2.0,\"review_count\":8,\"is_open\":0,\"attributes\":null,\"categories\":null,\"hours\":null}";
		JsonNode eventAsJsonNode = mapper.readTree(event);
		assertNotNull(YelpConnectSchemas.buildBusinessStruct(eventAsJsonNode));
	}
	

	@Test
	public void buildHourStructNullTest()
			throws JsonMappingException, JsonProcessingException {
		String event = "{\"business_id\":\"QjPqXSqb9aiBPeRJ91mgnQ\",\"name\":\"Pure Bliss Wellness Center & Spa\","
				+ "\"address\":\"1704 Walnut St\",\"city\":\"Philadelphia\",\"state\":\"PA\",\"postal_code\":\"19103\","
				+ "\"latitude\":39.9498377,\"longitude\":-75.1694777,\"stars\":3.5,\"review_count\":8,\"is_open\":0,"
				+ "\"attributes\":{\"BusinessAcceptsCreditCards\":\"True\",\"ByAppointmentOnly\":\"True\","
				+ "\"BusinessParking\":\"{'garage': False, 'street': True, 'validated': False, 'lot': False, 'valet': False}\""
				+ ",\"RestaurantsPriceRange2\":\"2\"},\"categories\":\"Day Spas, Beauty & Spas\",\"hours\":null}";
		JsonNode eventAsJsonNode = mapper.readTree(event);

		assertNull(YelpConnectSchemas.buildHoursStruct(eventAsJsonNode));
	}
	
	@Test
	public void buildHoursStructNullAttrTest() throws JsonMappingException, JsonProcessingException {
		String event = "{\"business_id\":\"ZXtiw_Ldhl-1HgsbJSLBHg\",\"name\":\"Walgreens\",\"address\":\"13 N Black Horse Pike\","
				+ "\"city\":\"Williamstown\",\"state\":\"NJ\",\"postal_code\":\"08094\",\"latitude\":39.6891882,\"longitude\":-74.9937592,"
				+ "\"stars\":2.0,\"review_count\":12,\"is_open\":1,\"attributes\":{\"BikeParking\":\"False\",\"Caters\":\"False\","
				+ "\"RestaurantsPriceRange2\":\"2\",\"ByAppointmentOnly\":\"False\",\"BusinessAcceptsCreditCards\":\"True\","
				+ "\"RestaurantsTakeOut\":\"True\",\"BusinessParking\":\"{u'valet': False, u'garage': None, u'street': None, u'lot': True, u'validated': None}\","
				+ "\"DriveThru\":\"True\",\"RestaurantsDelivery\":\"False\"},\"categories\":\"Food, Convenience Stores, Drugstores, "
				+ "Beauty & Spas, Cosmetics & Beauty Supply, Shopping\",\"hours\":{\"Monday\":null, \"Wednesday\":\"6:0-0:0\",\"Thursday\":\"6:0-0:0\","
				+ "\"Friday\":\"6:0-0:0\",\"Saturday\":\"6:0-0:0\",\"Sunday\":\"6:0-0:0\"}}";
		JsonNode eventAsJsonNode = mapper.readTree(event);

		assertNotNull(YelpConnectSchemas.buildHoursStruct(eventAsJsonNode));
	}
}

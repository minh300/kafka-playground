package com.minh.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

public class MelpAvroUtilsTest {
	@Test
	public void jsonToBusinessTest() throws IOException {
		Business business  = MelpAvroUtils.jsonToBusinessAvro(
				"{\"business_id\":\"Pns2l4eNsfO8kk83dixA6A\","
				+ "\"name\":\"Abby Rappoport, LAC, CMQ\","
				+ "\"address\":\"1616 Chapala St, Ste 2\","
				+ "\"city\":\"Santa Barbara\","
				+ "\"state\":\"CA\","
				+ "\"postal_code\":\"93101\","
				+ "\"latitude\":34.4266787,"
				+ "\"longitude\":-119.7111968,"
				+ "\"stars\":5.0,"
				+ "\"review_count\":7,"
				+ "\"is_open\":0,"
				+ "\"attributes\":{\"RestaurantsTakeOut\":{\"boolean\":true}},"
				+ "\"categories\":{\"string\":\"Doctors, Traditional Chinese Medicine, Naturopathic\\/Holistic, Acupuncture, Health & Medical, Nutritionists\"},"
				+ "\"hours\":null, \"test\":true}");
		assertEquals("Pns2l4eNsfO8kk83dixA6A", business.getBusinessId());
		assertEquals("Abby Rappoport, LAC, CMQ", business.getName());
		assertEquals("1616 Chapala St, Ste 2", business.getAddress());
		assertEquals("Santa Barbara", business.getCity());
		assertEquals("CA", business.getState());
		assertEquals("93101", business.getPostalCode());
		assertEquals(34.4266787f, business.getLatitude());
		assertEquals(-119.7111968f, business.getLongitude());
		assertEquals(5.0, business.getStars());
		assertEquals(7, business.getReviewCount());
		assertEquals(0, business.getIsOpen());
		//assertTrue((Boolean)business.getAttributes().get(new Utf8("RestaurantsTakeOut")));
		String[] categories = business.getCategories().split(", ");
		assertEquals("Doctors", categories[0]);
		assertEquals("Traditional Chinese Medicine", categories[1]);
		assertEquals("Naturopathic/Holistic", categories[2]);
		assertEquals("Acupuncture", categories[3]);
		assertEquals("Health & Medical", categories[4]);
		assertEquals("Nutritionists", categories[5]);
	}
	
    

}

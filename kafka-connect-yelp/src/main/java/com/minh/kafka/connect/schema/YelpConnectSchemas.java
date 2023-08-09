package com.minh.kafka.connect.schema;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import com.fasterxml.jackson.databind.JsonNode;
import com.minh.avro.Business;

import io.confluent.connect.avro.AvroData;

public class YelpConnectSchemas {
	public static AvroData data = new AvroData(100);

	public static final String BUSINESS = "business";
	public static final String ATTRIBUTES = "attributes";
	public static final String BUSINESS_ID_FIELD = "business_id";
	public static final String NAME_FIELD = "name";
	public static final String ADDRESS_FIELD = "address";
	public static final String CITY_FIELD = "city";
	public static final String STATE_FIELD = "state";
	public static final String POSTAL_CODE_FIELD = "postal_code";
	public static final String LATITUDE_FIELD = "latitude";
	public static final String LONGITUDE_FIELD = "longitude";
	public static final String STARS_FIELD = "stars";
	public static final String REVIEW_COUNT_FIELD = "review_count";
	public static final String IS_OPEN_FIELD = "is_open";
	public static final String ATTRIBUTES_FIELD = "attributes";
	public static final String GARAGE_FIELD = "garage";
	public static final String STREET_FIELD = "street";
	public static final String VALIDATED_FIELD = "validated";
	public static final String LOT_FIELD = "lot";
	public static final String VALET_FIELD = "valet";
	public static final String CATEGORIES_FIELD = "categories";
	public static final String HOURS_FIELD = "hours";
	public static final String MONDAY_FIELD = "Monday";
	public static final String TUESDAY_FIELD = "Tuesday";
	public static final String WEDNESDAY_FIELD = "Wednesday";
	public static final String THURSDAY_FIELD = "Thursday";
	public static final String FRIDAY_FIELD = "Friday";
	public static final String SATURDAY_FIELD = "Saturday";
	public static final String SUNDAY_FIELD = "Sunday";
	
	public static final Schema BUSINESS_SCHEMA = YelpConnectSchemas.data.toConnectSchema(Business.SCHEMA$);
	public static final Schema HOURS_SCHEMA = BUSINESS_SCHEMA.field(HOURS_FIELD).schema();
//	public static Schema ATTRIBUTES_SCHEMA = SchemaBuilder.struct().name(ATTRIBUTES)
//			.version(1)
//			.map(Schema.STRING_SCHEMA, null)
//			.build();
//
//	public static Schema BUSINESS_SCHEMA = SchemaBuilder.struct().name(BUSINESS)
//			.version(1)
//			.field(BUSINESS_ID_FIELD, Schema.STRING_SCHEMA)
//			.field(NAME_FIELD, Schema.STRING_SCHEMA)
//			.field(ADDRESS_FIELD, Schema.STRING_SCHEMA)
//			.field(CITY_FIELD, Schema.STRING_SCHEMA)
//			.field(STATE_FIELD, Schema.STRING_SCHEMA)
//			.field(POSTAL_CODE_FIELD, Schema.STRING_SCHEMA)
//			.field(LATITUDE_FIELD, Schema.FLOAT32_SCHEMA)
//			.field(LONGITUDE_FIELD, Schema.FLOAT32_SCHEMA)
//			.field(STARS_FIELD, Schema.FLOAT32_SCHEMA)
//			.field(REVIEW_COUNT_FIELD, Schema.INT32_SCHEMA)
//			.field(IS_OPEN_FIELD, Schema.INT16_SCHEMA)
//			.field(ATTRIBUTES_FIELD, ATTRIBUTES_SCHEMA)
//			.field(IS_OPEN_FIELD, Schema.INT16_SCHEMA)
//			.field(IS_OPEN_FIELD, Schema.INT16_SCHEMA)
//			.field(IS_OPEN_FIELD, Schema.INT16_SCHEMA)
//			.field(IS_OPEN_FIELD, Schema.INT16_SCHEMA)
//			.field(IS_OPEN_FIELD, Schema.INT16_SCHEMA)
//			.field(IS_OPEN_FIELD, Schema.INT16_SCHEMA)
//			.field(IS_OPEN_FIELD, Schema.INT16_SCHEMA);
			
	public static Struct buildBusinessStruct(final JsonNode recordValue){

        // Issue top level fields
		Struct hours = new Struct(HOURS_SCHEMA);

		boolean hasHours = recordValue.get(HOURS_FIELD)!=null && !recordValue.get(HOURS_FIELD).isNull();
		if(hasHours) {
			putIfExist(hours, MONDAY_FIELD, recordValue.get(HOURS_FIELD).get(MONDAY_FIELD));
			putIfExist(hours, TUESDAY_FIELD, recordValue.get(HOURS_FIELD).get(TUESDAY_FIELD));
			putIfExist(hours, WEDNESDAY_FIELD, recordValue.get(HOURS_FIELD).get(WEDNESDAY_FIELD));
			putIfExist(hours, THURSDAY_FIELD, recordValue.get(HOURS_FIELD).get(THURSDAY_FIELD));
			putIfExist(hours, FRIDAY_FIELD, recordValue.get(HOURS_FIELD).get(FRIDAY_FIELD));
			putIfExist(hours, SATURDAY_FIELD, recordValue.get(HOURS_FIELD).get(SATURDAY_FIELD));
			putIfExist(hours, SUNDAY_FIELD, recordValue.get(HOURS_FIELD).get(SUNDAY_FIELD));
		}
        Struct businessStruct = new Struct(BUSINESS_SCHEMA)
                .put(BUSINESS_ID_FIELD, recordValue.get(BUSINESS_ID_FIELD).textValue())
                .put(NAME_FIELD, recordValue.get(NAME_FIELD).textValue())
                .put(ADDRESS_FIELD, recordValue.get(ADDRESS_FIELD).textValue())
                .put(CITY_FIELD, recordValue.get(CITY_FIELD).textValue())
                .put(STATE_FIELD, recordValue.get(STATE_FIELD).textValue())
                .put(POSTAL_CODE_FIELD, recordValue.get(POSTAL_CODE_FIELD).textValue())
                .put(LATITUDE_FIELD, recordValue.get(LATITUDE_FIELD).floatValue())
                .put(LONGITUDE_FIELD, recordValue.get(LONGITUDE_FIELD).floatValue())
                .put(STARS_FIELD, recordValue.get(STARS_FIELD).floatValue())
                .put(REVIEW_COUNT_FIELD, recordValue.get(REVIEW_COUNT_FIELD).intValue())
                .put(IS_OPEN_FIELD, recordValue.get(IS_OPEN_FIELD).intValue())
              //  .put(ATTRIBUTES_FIELD, null) TODO
                .put(CATEGORIES_FIELD, recordValue.get(CATEGORIES_FIELD).textValue());
        if(hasHours) {
        	businessStruct.put(HOURS_FIELD, hours);
        }

        return businessStruct;
    }
	

	private static void putIfExist(final Struct struct, final String fieldName, JsonNode node) {
		if (node != null) {
			struct.put(fieldName, node.textValue());
		}
	}
}

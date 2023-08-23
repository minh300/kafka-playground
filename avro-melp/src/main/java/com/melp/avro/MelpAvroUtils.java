package com.melp.avro;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.compress.utils.IOUtils;

public class MelpAvroUtils {
	private static final String BUSINESS_STRING = "business";
	private static final String CHECKIN_STRING = "checkin";
	private static final String REVIEW_STRING = "review";
	private static final String TIP_STRING = "tip";
	private static final String USER_STRING = "user";
	private static final String BUSINESS_SCHEMA = loadBusinessSchema();
	private static final String CHECKIN_SCHEMA = loadCheckinSchema();
	private static final String REVIEW_SCHEMA = loadReviewSchema();
	private static final String TIP_SCHEMA = loadTipSchema();
	private static final String USER_SCHEMA = loadUserSchema();
	
	private static String loadBusinessSchema() {
		return loadSchema(BUSINESS_STRING);
	}
	
	private static String loadCheckinSchema() {
		return loadSchema(CHECKIN_STRING);
	}
	
	private static String loadReviewSchema() {
		return loadSchema(REVIEW_STRING);
	}
	
	private static String loadTipSchema() {
		return loadSchema(TIP_STRING);
	}
	
	private static String loadUserSchema() {
		return loadSchema(USER_STRING);
	}

	private static String loadSchema(final String name) {
		try (InputStream input = MelpAvroUtils.class.getResourceAsStream("/avro/" + name + ".avsc")) {
			return new String(IOUtils.toByteArray(input), StandardCharsets.UTF_8);
		} catch (IOException e) {
			throw new RuntimeException("Unable to load schema " + name, e);
		}
	}
	
	public static Business jsonToBusinessAvro(final String genericRecordStr) throws IOException {
		return (Business) jsonToAvro(genericRecordStr, BUSINESS_SCHEMA);
	}
	
	public static Checkin jsonToCheckInAvro(final String genericRecordStr) throws IOException {
		return (Checkin) jsonToAvro(genericRecordStr, CHECKIN_SCHEMA);
	}

	public static Review jsonToReviewAvro(final String genericRecordStr) throws IOException {
		return (Review) jsonToAvro(genericRecordStr, REVIEW_SCHEMA);
	}
	
	public static Tip jsonToTipAvro(final String genericRecordStr) throws IOException {
		return (Tip) jsonToAvro(genericRecordStr, TIP_SCHEMA);
	}
	
	public static User jsonToUserAvro(final String genericRecordStr) throws IOException {
		return (User) jsonToAvro(genericRecordStr, USER_SCHEMA);
	}
	
	public static GenericRecord jsonToAvro(final String genericRecordStr, final String schemaStr) throws IOException {
		Schema.Parser schemaParser = new Schema.Parser();
		Schema schema = schemaParser.parse(schemaStr);
		DecoderFactory decoderFactory = new DecoderFactory();
		JsonDecoder decoder = decoderFactory.jsonDecoder(schema, genericRecordStr);
		DatumReader<GenericData.Record> reader =
		            new GenericDatumReader<>(schema);
		 
		return SpecificData.get().deepCopy(schema, reader.read(null, decoder));
	}
}

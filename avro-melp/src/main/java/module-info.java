
module avro_melp {
	requires transitive org.apache.avro;
	requires transitive org.apache.commons.compress;
	requires transitive com.fasterxml.jackson.databind;
    exports com.melp.avro;
}
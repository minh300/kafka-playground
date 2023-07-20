package minh.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppProperties implements IAppProperties {

	private static final String KAFKA_BOOTSTRAP_SERVER_STR = "bootstrap.servers";

	private final Properties props;
	
	public static final AppProperties instance = init();

	public AppProperties(final Properties props) throws IOException {
		this.props = props;
	}

	@Override
	public String getProp(final String key) {
		return props.getProperty(key);
	}

	public String getBootStrapServer() {
		return getProp(KAFKA_BOOTSTRAP_SERVER_STR);
	}
	
	private static AppProperties init()  {
		Properties prop = new Properties();

		try (InputStream input = AppProperties.class.getResourceAsStream("/app.properties")) {

			prop.load(input);
			return new AppProperties(prop);
		} catch (IOException e) {
			throw new RuntimeException("Unable to load properties", e);
		}
	}
}

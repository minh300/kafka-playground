package minh.common;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;


public class AppPropertiesTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppPropertiesTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppPropertiesTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertEquals("127.0.0.1:9092", AppProperties.instance.getBootStrapServer() );
        assertEquals("test", AppProperties.instance.getProp("random.prop") );
    }
}

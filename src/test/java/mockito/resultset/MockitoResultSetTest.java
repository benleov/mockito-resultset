package mockito.resultset;

import org.junit.Test;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 */
public class MockitoResultSetTest {

    @Test
    public void testMockResultSetBuilder() throws IOException, SQLException {

        MockitoResultSetBuilder builder = new MockitoResultSetBuilder();

        ResultSet resultSet = builder.withPath("src/test/resources/test.csv").build();

        // TODO: assert results are correct

    }

}

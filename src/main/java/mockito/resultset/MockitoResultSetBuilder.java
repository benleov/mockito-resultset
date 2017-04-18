package mockito.resultset;

import de.siegmar.fastcsv.reader.CsvParser;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRow;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class MockitoResultSetBuilder {

    private File file;
    private String path;
    private Reader reader;
    private Character delimiter;

    public MockitoResultSetBuilder withFile(File file) {
        this.file = file;
        return this;
    }

    public MockitoResultSetBuilder withReader(Reader reader) {
        this.reader = reader;
        return this;
    }

    public MockitoResultSetBuilder withPath(String path) {
        this.path = path;
        return this;
    }

    public MockitoResultSetBuilder withDelimiter(Character delimiter) {
        this.delimiter = delimiter;
        return this;
    }


    public ResultSet build() throws SQLException, IOException {

        CsvReader csvReader = new CsvReader();

        csvReader.setContainsHeader(true);

        if (delimiter != null) {
            csvReader.setTextDelimiter(delimiter);
        }

        Reader fileReader;

        if (reader != null) {
            fileReader = reader;
        } else if (file != null) {
            fileReader = new FileReader(file);
        } else if (path != null) {
            fileReader = new FileReader(path);
        } else {
            throw new IOException("No file, path or reader was specified.");
        }

        try (CsvParser csvParser = csvReader.parse(fileReader)) {

            csvParser.nextRow();    // read in headers

            List<String> headers = csvParser.getHeader();

            ResultSet resultSet = mock(ResultSet.class);

            ResultSetMetaData metaData = mock(ResultSetMetaData.class);
            when(resultSet.getMetaData()).thenReturn(metaData);
            when(metaData.getColumnCount()).thenReturn(headers.size());

            // parse headers

            int headerIndex = 1;
            for (String header : headers) {

                String[] split = header.split(" ");
                String columnName = split[0];
                String columnType = split[1];

                when(metaData.getColumnName(headerIndex)).thenReturn(columnName);
                when(metaData.getColumnType(1)).thenReturn(JDBCType.valueOf(columnType).getVendorTypeNumber());
                headerIndex++;
            }

            int rowIndex = 0;

            CsvRow row;

            while ((row = csvParser.nextRow()) != null) {

                for (int columnIndex = 0; columnIndex < headers.size(); columnIndex++) {

                    String columnName = headers.get(columnIndex);
                    String columnValue = row.getField(columnIndex);

                    when(resultSet.getArray(headers.get(columnIndex))).thenReturn(null);
                    when(resultSet.getArray(columnName)).thenReturn(null);

                    when(resultSet.getAsciiStream(headers.get(columnIndex))).thenReturn(null);
                    when(resultSet.getAsciiStream(columnName)).thenReturn(null);

                    when(resultSet.getBigDecimal(headers.get(columnIndex))).thenReturn(null);
                    when(resultSet.getBigDecimal(columnName)).thenReturn(null);

                    when(resultSet.getBinaryStream(headers.get(columnIndex))).thenReturn(null);
                    when(resultSet.getBinaryStream(columnName)).thenReturn(null);

                    when(resultSet.getBlob(headers.get(columnIndex))).thenReturn(null);
                    when(resultSet.getBlob(columnName)).thenReturn(null);

                    when(resultSet.getBoolean(headers.get(columnIndex))).thenReturn(Boolean.valueOf(columnValue));
                    when(resultSet.getBoolean(columnName)).thenReturn(Boolean.valueOf(columnValue));

                    when(resultSet.getByte(headers.get(columnIndex))).thenReturn(columnValue.getBytes()[0]);
                    when(resultSet.getByte(columnName)).thenReturn(columnValue.getBytes()[0]);

                    when(resultSet.getBytes(headers.get(columnIndex))).thenReturn(columnValue.getBytes());
                    when(resultSet.getBytes(columnName)).thenReturn(null);

                    when(resultSet.getCharacterStream(headers.get(columnIndex))).thenReturn(null);
                    when(resultSet.getCharacterStream(columnName)).thenReturn(null);

                    when(resultSet.getClob(headers.get(columnIndex))).thenReturn(null);
                    when(resultSet.getClob(columnName)).thenReturn(null);

                    when(resultSet.getDate(headers.get(columnIndex))).thenReturn(null);
                    when(resultSet.getDate(columnName)).thenReturn(null);

                    try {
                        double doubleValue = Double.valueOf(columnValue);
                        when(resultSet.getDouble(headers.get(columnIndex))).thenReturn(doubleValue);
                        when(resultSet.getDouble(columnName)).thenReturn(doubleValue);
                    } catch (NumberFormatException e) {
                    }

                    try {
                        float floatValue = Float.valueOf(columnValue);
                        when(resultSet.getFloat(headers.get(columnIndex))).thenReturn(floatValue);
                        when(resultSet.getFloat(columnName)).thenReturn(floatValue);
                    } catch (NumberFormatException e) {
                    }

                    try {
                        int integerValue = Integer.valueOf(columnValue);
                        when(resultSet.getInt(headers.get(columnIndex))).thenReturn(integerValue);
                        when(resultSet.getInt(columnName)).thenReturn(integerValue);
                    } catch (NumberFormatException e) {
                    }

                    try {
                        long longValue = Long.valueOf(columnValue);
                        when(resultSet.getLong(headers.get(columnIndex))).thenReturn(longValue);
                        when(resultSet.getLong(columnName)).thenReturn(longValue);
                    } catch (NumberFormatException e) {
                    }


                    when(resultSet.getNCharacterStream(headers.get(columnIndex))).thenReturn(null);
                    when(resultSet.getNCharacterStream(columnName)).thenReturn(null);

                    when(resultSet.getNClob(headers.get(columnIndex))).thenReturn(null);
                    when(resultSet.getNClob(columnName)).thenReturn(null);

                    when(resultSet.getNString(headers.get(columnIndex))).thenReturn(columnValue);
                    when(resultSet.getNString(columnName)).thenReturn(columnValue);

                    when(resultSet.getObject(headers.get(columnIndex))).thenReturn(columnValue);
                    when(resultSet.getObject(columnName)).thenReturn(columnValue);

                    when(resultSet.getRef(headers.get(columnIndex))).thenReturn(null);
                    when(resultSet.getRef(columnName)).thenReturn(null);

                    when(resultSet.getRowId(headers.get(columnIndex))).thenReturn(null);
                    when(resultSet.getRowId(columnName)).thenReturn(null);

                    try {
                        short shortValue = Short.valueOf(columnValue);
                        when(resultSet.getShort(headers.get(columnIndex))).thenReturn(shortValue);
                        when(resultSet.getShort(columnName)).thenReturn(shortValue);

                    } catch (NumberFormatException e) {
                    }


                    when(resultSet.getSQLXML(headers.get(columnIndex))).thenReturn(null);
                    when(resultSet.getSQLXML(columnName)).thenReturn(null);

                    when(resultSet.getString(headers.get(columnIndex))).thenReturn(columnValue);
                    when(resultSet.getString(columnName)).thenReturn(columnValue);

                    when(resultSet.getTime(headers.get(columnIndex))).thenReturn(null);
                    when(resultSet.getTime(columnName)).thenReturn(null);

                    when(resultSet.getTimestamp(headers.get(columnIndex))).thenReturn(null);
                    when(resultSet.getTimestamp(columnName)).thenReturn(null);

                    when(resultSet.getURL(headers.get(columnIndex))).thenReturn(null);
                    when(resultSet.getURL(columnName)).thenReturn(null);
                }


                rowIndex++;
            }

            // set resultset.next(), with last value false
            Boolean[] nextReturns = new Boolean[rowIndex + 1];
            Arrays.fill(nextReturns, Boolean.TRUE);
            nextReturns[nextReturns.length - 1] = false;

            when(resultSet.next()).thenReturn(nextReturns[0], Arrays.copyOfRange(nextReturns, 1, nextReturns.length));

            return resultSet;

        }


    }

}



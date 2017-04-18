package mockito.resultset;

import de.siegmar.fastcsv.reader.CsvParser;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRow;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
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

        class ResultSetState {

            public ResultSetState() {
            }

            int currentRow = -1;
            int totalRows;
            List<List<String>> data = new ArrayList<>();

            public String getCurrentValue(int column) {
                return data.get(currentRow).get(column);
            }

        }


        try (CsvParser csvParser = csvReader.parse(fileReader)) {

            final ResultSetState state = new ResultSetState();

            CsvRow row;

            while ((row = csvParser.nextRow()) != null) {

                List<String> rowData = new ArrayList<>();

                for (int columnIndex = 0; columnIndex < row.getFieldCount(); columnIndex++) {
                    String columnValue = row.getField(columnIndex);
                    rowData.add(columnValue);

                }

                state.data.add(rowData);
            }


            List<String> headers = csvParser.getHeader();

            ResultSet resultSet = mock(ResultSet.class);

            ResultSetMetaData metaData = mock(ResultSetMetaData.class);
            when(resultSet.getMetaData()).thenReturn(metaData);
            when(metaData.getColumnCount()).thenReturn(headers.size());

            // parse headers

            int headerIndex = 1;

            List<JDBCType> columnTypes = new ArrayList<>(headers.size());

            for (String header : headers) {

                String[] split = header.split(" ");
                String columnName = split[0];
                String columnTypeString = split[1];
                JDBCType columnType = JDBCType.valueOf(columnTypeString);

                when(metaData.getColumnName(headerIndex)).thenReturn(columnName);
                when(metaData.getColumnType(1)).thenReturn(columnType.getVendorTypeNumber());
                headerIndex++;
                columnTypes.add(columnType);
            }

            int rowIndex = 0;

            csvReader = new CsvReader();
            csvReader.setContainsHeader(true);

            try (CsvParser csvParser2 = csvReader.parse(new FileReader(path))){
                csvParser2.nextRow();    // read off headers
                csvParser.getHeader();

                while ((row = csvParser2.nextRow()) != null) {

                    //List<String> rowData = new ArrayList<>();

                    for (int columnIndex = 0; columnIndex < headers.size(); columnIndex++) {

                        String columnName = headers.get(columnIndex);
                        String columnValue = row.getField(columnIndex);
                        JDBCType columnType = columnTypes.get(columnIndex);

                        // rowData.add(columnValue);
                        // https://db.apache.org/ojb/docu/guides/jdbc-types.html#Mapping+of+JDBC+Types+to+Java+Types

                        switch (columnType) {
                            case TINYINT:
                                when(resultSet.getByte(headers.get(columnIndex))).thenReturn(columnValue.getBytes()[0]);
                                when(resultSet.getByte(columnName)).thenReturn(columnValue.getBytes()[0]);
                                break;
                            case SMALLINT:
                                try {
                                    short shortValue = Short.valueOf(columnValue);
                                    when(resultSet.getShort(headers.get(columnIndex))).thenReturn(shortValue);
                                    when(resultSet.getShort(columnName)).thenReturn(shortValue);

                                } catch (NumberFormatException e) {
                                }
                                break;
                            case INTEGER:
                                try {
                                    int integerValue = Integer.valueOf(columnValue);
                                    when(resultSet.getInt(headers.get(columnIndex))).thenReturn(integerValue);
                                    when(resultSet.getInt(columnName)).thenReturn(integerValue);
                                } catch (NumberFormatException e) {
                                }
                                break;
                            case BIGINT:
                                try {
                                    long longValue = Long.valueOf(columnValue);
                                    when(resultSet.getLong(headers.get(columnIndex))).thenReturn(longValue);
                                    when(resultSet.getLong(columnName)).thenReturn(longValue);
                                } catch (NumberFormatException e) {
                                }
                                break;

                            case REAL:
                                try {
                                    float floatValue = Float.valueOf(columnValue);
                                    when(resultSet.getFloat(headers.get(columnIndex))).thenReturn(floatValue);
                                    when(resultSet.getFloat(columnName)).thenReturn(floatValue);
                                } catch (NumberFormatException e) {
                                }
                                break;
                            case FLOAT:
                            case DOUBLE:
                                try {
                                    double doubleValue = Double.valueOf(columnValue);
                                    when(resultSet.getDouble(headers.get(columnIndex))).thenReturn(doubleValue);
                                    when(resultSet.getDouble(columnName)).thenReturn(doubleValue);
                                } catch (NumberFormatException e) {
                                }
                                break;
                            case NUMERIC:
                            case DECIMAL:
                                when(resultSet.getBigDecimal(headers.get(columnIndex))).thenReturn(null);
                                when(resultSet.getBigDecimal(columnName)).thenReturn(null);
                                break;
                            case CHAR:
                            case NCHAR:
                                when(resultSet.getString(headers.get(columnIndex))).thenReturn(columnValue);
                                when(resultSet.getString(columnName)).thenReturn(columnValue);
                                break;
                            case NVARCHAR:
                            case LONGNVARCHAR:
                            case VARCHAR:
                            case LONGVARCHAR:
                                //when(resultSet.getString(headers.get(columnIndex))).thenReturn(columnValue);
                               // when(resultSet.getString(columnName)).thenReturn(columnValue);
                                when(resultSet.getString(any(Integer.class))).thenAnswer(new Answer<String>() {

                                    @Override
                                    public String answer(InvocationOnMock invocation) throws Throwable {

                                        int columnNumber = invocation.getArgument(0);

                                        if(columnNumber > 0) {
                                            return state.getCurrentValue(columnNumber - 1);
                                        } else {
                                            return "";
                                        }

                                    }
                                });
                                break;
                            case DATE:
                                when(resultSet.getDate(headers.get(columnIndex))).thenReturn(null);
                                when(resultSet.getDate(columnName)).thenReturn(null);
                                break;
                            case TIME:
                                when(resultSet.getTime(headers.get(columnIndex))).thenReturn(null);
                                when(resultSet.getTime(columnName)).thenReturn(null);
                                break;
                            case TIMESTAMP:
                                when(resultSet.getTimestamp(headers.get(columnIndex))).thenReturn(null);
                                when(resultSet.getTimestamp(columnName)).thenReturn(null);
                                break;
                            case BINARY:
                            case VARBINARY:
                            case LONGVARBINARY:
                                when(resultSet.getBinaryStream(headers.get(columnIndex))).thenReturn(null);
                                when(resultSet.getBinaryStream(columnName)).thenReturn(null);

                                when(resultSet.getBytes(headers.get(columnIndex))).thenReturn(columnValue.getBytes());
                                when(resultSet.getBytes(columnName)).thenReturn(null);
                                break;
                            case NULL:
                                break;
                            case OTHER:
                                break;
                            case JAVA_OBJECT:
                                break;
                            case DISTINCT:
                                break;
                            case STRUCT:
                                break;
                            case ARRAY:
                                when(resultSet.getArray(headers.get(columnIndex))).thenReturn(null);
                                when(resultSet.getArray(columnName)).thenReturn(null);
                                break;
                            case BLOB:
                                when(resultSet.getBlob(headers.get(columnIndex))).thenReturn(null);
                                when(resultSet.getBlob(columnName)).thenReturn(null);
                                break;
                            case CLOB:
                                when(resultSet.getClob(headers.get(columnIndex))).thenReturn(null);
                                when(resultSet.getClob(columnName)).thenReturn(null);
                                break;
                            case REF:
                                when(resultSet.getRef(headers.get(columnIndex))).thenReturn(null);
                                when(resultSet.getRef(columnName)).thenReturn(null);
                                break;
                            case DATALINK:
                                break;
                            case BIT:
                            case BOOLEAN:
                                when(resultSet.getBoolean(headers.get(columnIndex))).thenReturn(Boolean.valueOf(columnValue));
                                when(resultSet.getBoolean(columnName)).thenReturn(Boolean.valueOf(columnValue));
                                break;
                            case ROWID:
                                when(resultSet.getRowId(headers.get(columnIndex))).thenReturn(null);
                                when(resultSet.getRowId(columnName)).thenReturn(() -> columnValue.getBytes());
                                break;
                            case NCLOB:
                                when(resultSet.getNClob(headers.get(columnIndex))).thenReturn(null);
                                when(resultSet.getNClob(columnName)).thenReturn(null);
                                break;
                            case SQLXML:
                                when(resultSet.getSQLXML(headers.get(columnIndex))).thenReturn(null);
                                when(resultSet.getSQLXML(columnName)).thenReturn(null);
                                break;
                            case REF_CURSOR:
                                break;
                            case TIME_WITH_TIMEZONE:
                                when(resultSet.getTimestamp(headers.get(columnIndex))).thenReturn(null);
                                when(resultSet.getTimestamp(columnName)).thenReturn(null);
                                break;
                            case TIMESTAMP_WITH_TIMEZONE:
                                when(resultSet.getTimestamp(headers.get(columnIndex))).thenReturn(null);
                                when(resultSet.getTimestamp(columnName)).thenReturn(null);
                                break;
                        }

                        // TODO: move these to switch. The only mocked methods below this should be
                        // 'gets' that apply to all data types

//                    when(resultSet.getAsciiStream(headers.get(columnIndex))).thenReturn(null);
//                    when(resultSet.getAsciiStream(columnName)).thenReturn(null);
//
//                    when(resultSet.getCharacterStream(headers.get(columnIndex))).thenReturn(null);
//                    when(resultSet.getCharacterStream(columnName)).thenReturn(null);
//
//                    when(resultSet.getNCharacterStream(headers.get(columnIndex))).thenReturn(null);
//                    when(resultSet.getNCharacterStream(columnName)).thenReturn(null);
//
//                    when(resultSet.getNString(headers.get(columnIndex))).thenReturn(columnValue);
//                    when(resultSet.getNString(columnName)).thenReturn(columnValue);
//
//                    when(resultSet.getObject(headers.get(columnIndex))).thenReturn(columnValue);
//                    when(resultSet.getObject(columnName)).thenReturn(columnValue);
//
//                    when(resultSet.getURL(headers.get(columnIndex))).thenReturn(null);
//                    when(resultSet.getURL(columnName)).thenReturn(null);
                    }

                    // state.data.add(rowData);
                    rowIndex++;
                }
            }
            state.totalRows = rowIndex;

            when(resultSet.next()).thenAnswer(new Answer<Object>() {


                @Override
                public Object answer(InvocationOnMock invocation) throws Throwable {
                    boolean hasNext = state.currentRow < state.totalRows;
                    state.currentRow++;
                    return hasNext;
                }

            });

            return resultSet;

        }


    }

}



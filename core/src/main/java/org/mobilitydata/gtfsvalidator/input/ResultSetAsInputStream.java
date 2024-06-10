package org.mobilitydata.gtfsvalidator.input;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;

public class ResultSetAsInputStream extends InputStream {

    private final PreparedStatement statement;
    private final ResultSet resultSet;

    private final int ColumnCount;
    private byte[] buffer;
    private int position;

    public ResultSetAsInputStream(final Connection connection, final String sql, final int ColumnCount, final Object... parameters) throws SQLException {
        this.ColumnCount = ColumnCount;
        statement = createStatement(connection, sql, parameters);
        resultSet = statement.executeQuery();
    }

    private static PreparedStatement createStatement(final Connection connection, final String sql, final Object[] parameters) throws SQLException {
        // PreparedStatement should be created here from passed connection, sql and parameters
        PreparedStatement prepStatement=connection.prepareStatement(sql);
        if (parameters.length > 0) {
            prepStatement.setInt(1, (Integer) parameters[0]);
            // Set the offset
            prepStatement.setInt(2, (Integer) parameters[1]);
        }
        return prepStatement;
    }

    @Override
    public int read() throws IOException {
        try {
            if(buffer == null) {
                // first call of read method
                if(!resultSet.next()) {
                    return -1; // no rows - empty input stream
                } else {
                    buffer = getRowByteArray();
                    position = 0;
                    return buffer[position++] & (0xff);
                }
            } else {
                // not first call of read method
                if(position < buffer.length) {
                    // buffer already has some data in, which hasn't been read yet - returning it
                    return buffer[position++] & (0xff);
                } else {
                    // all data from buffer was read - checking whether there is next row and re-filling buffer
                    if(!resultSet.next()) {
                        return -1; // the buffer was read to the end and there is no rows - end of input stream
                    } else {
                        // there is next row - converting it to byte array and re-filling buffer
                        buffer = getRowByteArray();;
                        position = 0;
                        return buffer[position++] & (0xff);
                    }
                }
            }
        } catch(final SQLException ex) {
            throw new IOException(ex);
        }
    }

    public byte[] getRowByteArray() throws SQLException {


        StringBuilder stringRow = new StringBuilder();

            for (int i = 1; i <= ColumnCount; i++) {

                stringRow.append(resultSet.getString(i));

                if (i==ColumnCount){
                    stringRow.append("\n");
                }
                else
                    stringRow.append(",");

            }

        return stringRow.toString().getBytes();
    }

    @Override
    public void close() throws IOException {
        try {
            statement.close();
        } catch(final SQLException ex) {
            throw new IOException(ex);
        }
    }
}

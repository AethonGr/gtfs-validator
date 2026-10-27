package org.mobilitydata.gtfsvalidator.input;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.stream.Stream;

public class GtfsDatabaseInput extends GtfsInput {

    private Connection conn;

    private final String company_id;

    private final String db;


    public GtfsDatabaseInput(String db, String company_id) throws  SQLException {
        this.db = db;
        this.company_id = company_id;
        this.conn = ConnectDB();

    }

    /**
     * Connects to the PostgreSQL database DATABASE_NAME (default "transitool"); the -db argument is the schema that
     * the table names resolve to (working_data, operational_data). The rows are read through cursors, which
     * PostgreSQL only keeps inside a transaction, hence no autocommit.
     */
    public Connection ConnectDB() throws SQLException {
        String host = null;
        String user = null;
        String password = null;
        String port = null;
        String database = null;


        if (System.getenv("DATABASE_HOST") != null || System.getenv("DATABASE_USER") != null || System.getenv("DATABASE_PASSWORD") != null) {
            host = System.getenv("DATABASE_HOST");
            user = System.getenv("DATABASE_USER");
            password = System.getenv("DATABASE_PASSWORD");
            port = System.getenv("DATABASE_PORT");
            database = System.getenv("DATABASE_NAME");
        } else {
            host = "localhost";
            user = "root";
            password = "0112358";
        }
        if (port == null) {
            port = "5432";
        }
        if (database == null) {
            database = "transitool";
        }

        String connection_url = "jdbc:postgresql://" + host + ":" + port + "/" + database + "?currentSchema=" + this.db;

        try{
            Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection(connection_url, user, password);
            connection.setAutoCommit(false);
            return connection;

        } catch (ClassNotFoundException | SQLException e){

            throw new SQLException("Exception while connecting to database! Exception : " + e);
        }
    }
    public Connection getConn() {
        return conn;
    }
    
    public ResultSet ExecuteQuery(String Query) throws SQLException {
        Statement st = conn.createStatement();
        return st.executeQuery(Query);
    }

    private String getColumns(String filename) throws SQLException {
        String tableName = filename.substring(0, filename.lastIndexOf('.'));
        ResultSet column_rs = this.ExecuteQuery("SELECT * FROM " + tableName + " LIMIT 1");
        ResultSetMetaData rsmd = column_rs.getMetaData();
        int columnCount = rsmd.getColumnCount();

        String columns = "";
        for (int i = 1; i <= columnCount - 1; i++) {
            columns = columns.concat(rsmd.getColumnName(i));
            if (i != columnCount - 1) {
                columns = columns.concat(",");
            }
        }

        return columns;
    }

    /** ORDER BY the primary key (company_id aside): the order the rows had in MySQL, and a stable one. */
    private String getOrderBy(String filename) throws SQLException {
        String tableName = filename.substring(0, filename.lastIndexOf('.'));
        ResultSet key_rs = conn.getMetaData().getPrimaryKeys(null, this.db, tableName);
        SortedMap<Integer, String> keyColumns = new TreeMap<>();
        while (key_rs.next()) {
            if (!key_rs.getString("COLUMN_NAME").equals("company_id")) {
                // Qualified: the select list has text columns with the same names
                keyColumns.put(key_rs.getInt("KEY_SEQ"), tableName + "." + key_rs.getString("COLUMN_NAME"));
            }
        }
        key_rs.close();
        return keyColumns.isEmpty() ? "" : " ORDER BY " + String.join(", ", keyColumns.values());
    }


    private String getQueryString(String filename) {
        // Get the table name instead of text file
        String tableName = filename.substring(0, filename.lastIndexOf('.'));
        Map<String, String> queries = new HashMap<String, String>();

        queries.put("agency", "SELECT " +
                              "'agency_id' AS agency_id," +
                              "'agency_name' AS agency_name," +
                              "'agency_url' AS agency_url," +
                              "'agency_timezone' AS agency_timezone," +
                              "'agency_lang' AS agency_lang," +
                              "'agency_phone' AS agency_phone," +
                              "'agency_fare_url' AS agency_fare_url," +
                              "'agency_email' AS agency_email" +
                              " UNION " +
                              " SELECT " +
                              "CONCAT('\"',COALESCE(agency_id::text, ''), '\"')  AS agency_id," +
                              "CONCAT('\"',COALESCE(agency_name::text, ''), '\"')  AS agency_name," +
                              "CONCAT('\"',COALESCE(agency_url::text, ''), '\"')  AS agency_url," +
                              "CONCAT('\"',COALESCE(agency_timezone::text, ''), '\"')  AS agency_timezone," +
                              "CONCAT('\"',COALESCE(agency_lang::text, ''), '\"')  AS agency_lang," +
                              "CONCAT('\"',COALESCE(agency_phone::text, ''), '\"')  AS agency_phone," +
                              "CONCAT('\"',COALESCE(agency_fare_url::text, ''), '\"')  AS agency_fare_url," +
                              "CONCAT('\"',COALESCE(agency_email::text, ''), '\"')  AS agency_email" +
                              " FROM agency WHERE company_id = " + company_id + " ;");

        queries.put("routes", "SELECT " +
                              "'route_id' AS route_id," +
                              "'agency_id' AS agency_id," +
                              "'route_short_name' AS route_short_name," +
                              "'route_long_name' AS route_long_name," +
                              "'route_desc' AS route_desc," +
                              "'route_type' AS route_type," +
                              "'route_url' AS route_url," +
                              "'route_color' AS route_color," +
                              "'route_text_color' AS route_text_color," +
                              "'route_sort_order' AS route_sort_order," +
                              "'continuous_pickup' AS continuous_pickup," +
                              "'continuous_drop_off' AS continuous_drop_off" +
                              " UNION " +
                              " SELECT " +
                              "CONCAT('\"',COALESCE(route_id::text, ''), '\"') AS route_id," +
                              "CONCAT('\"',COALESCE(agency_id::text, ''), '\"') AS agency_id," +
                              "CONCAT('\"',COALESCE(route_short_name::text, ''), '\"') AS route_short_name," +
                              "CONCAT('\"',COALESCE(route_long_name::text, ''), '\"') AS route_long_name," +
                              "CONCAT('\"',COALESCE(route_desc::text, ''), '\"') AS route_desc," +
                              "CONCAT('\"',COALESCE(route_type::text, ''), '\"') AS route_type," +
                              "CONCAT('\"',COALESCE(route_url::text, ''), '\"') AS route_url," +
                              "CONCAT('\"',COALESCE(route_color::text, ''), '\"') AS route_color," +
                              "CONCAT('\"',COALESCE(route_text_color::text, ''), '\"') AS route_text_color," +
                              "CONCAT('\"',COALESCE(route_sort_order::text, ''), '\"') AS route_sort_order," +
                              "CONCAT('\"',COALESCE(continuous_pickup::text, ''), '\"') AS continuous_pickup," +
                              "CONCAT('\"',COALESCE(continuous_drop_off::text, ''), '\"') AS continuous_drop_off" +
                              " FROM routes WHERE company_id = " + company_id + " ;");

        queries.put("stops", "SELECT " +
                             "'stop_id' AS stop_id," +
                             "'stop_code' AS stop_code," +
                             "'stop_name' AS stop_name," +
                             "'stop_desc' AS stop_desc," +
                             "'stop_lat' AS stop_lat," +
                             "'stop_lon' AS stop_lon," +
                             "'zone_id' AS zone_id," +
                             "'stop_url' AS stop_url," +
                             "'location_type' AS location_type," +
                             "'parent_station' AS parent_station," +
                             "'stop_timezone' AS stop_timezone," +
                             "'wheelchair_boarding' AS wheelchair_boarding," +
                             "'level_id' AS level_id," +
                             "'platform_code' AS platform_code" +
                             " UNION " +
                             " SELECT " +
                             "CONCAT('\"',COALESCE(stop_id::text, ''), '\"') AS stop_id," +
                             "CONCAT('\"',COALESCE(stop_code::text, ''), '\"') AS stop_code," +
                             "CONCAT('\"',COALESCE(stop_name::text, ''), '\"') AS stop_name," +
                             "CONCAT('\"',COALESCE(stop_desc::text, ''), '\"') AS stop_desc," +
                             "CONCAT('\"',COALESCE(stop_lat::text, ''), '\"') AS stop_lat," +
                             "CONCAT('\"',COALESCE(stop_lon::text, ''), '\"') AS stop_lon," +
                             "CONCAT('\"',COALESCE(zone_id::text, ''), '\"') AS zone_id," +
                             "CONCAT('\"',COALESCE(stop_url::text, ''), '\"') AS stop_url," +
                             "CONCAT('\"',COALESCE(location_type::text, ''), '\"') AS location_type," +
                             "CONCAT('\"',COALESCE(parent_station::text, ''), '\"') AS parent_station," +
                             "CONCAT('\"',COALESCE(stop_timezone::text, ''), '\"') AS stop_timezone," +
                             "CONCAT('\"',COALESCE(wheelchair_boarding::text, ''), '\"') AS wheelchair_boarding," +
                             "CONCAT('\"',COALESCE(level_id::text, ''), '\"') AS level_id," +
                             "CONCAT('\"',COALESCE(platform_code::text, ''), '\"') AS platform_code" +
                             " FROM stops WHERE company_id = " + company_id + " ;");

        queries.put("trips", "SELECT " +
                             "'route_id' AS route_id," +
                             "'service_id' AS service_id," +
                             "'trip_id' AS trip_id," +
                             "'trip_headsign' AS trip_headsign," +
                             "'trip_short_name' AS trip_short_name," +
                             "'direction_id' AS direction_id," +
                             "'block_id' AS block_id," +
                             "'shape_id' AS shape_id," +
                             "'wheelchair_accessible' AS wheelchair_accessible," +
                             "'bikes_allowed' AS bikes_allowed" +
                             " UNION " +
                             " SELECT " +
                             "CONCAT('\"',COALESCE(route_id::text, ''), '\"') AS route_id," +
                             "CONCAT('\"',COALESCE(service_id::text, ''), '\"') AS service_id," +
                             "CONCAT('\"',COALESCE(trip_id::text, ''), '\"') AS trip_id," +
                             "CONCAT('\"',COALESCE(trip_headsign::text, ''), '\"') AS trip_headsign," +
                             "CONCAT('\"',COALESCE(trip_short_name::text, ''), '\"') AS trip_short_name," +
                             "CONCAT('\"',COALESCE(direction_id::text, ''), '\"') AS direction_id," +
                             "CONCAT('\"',COALESCE(block_id::text, ''), '\"') AS block_id," +
                             "CONCAT('\"',COALESCE(shape_id::text, ''), '\"') AS shape_id," +
                             "CONCAT('\"',COALESCE(wheelchair_accessible::text, ''), '\"') AS wheelchair_accessible," +
                             "CONCAT('\"',COALESCE(bikes_allowed::text, ''), '\"') AS bikes_allowed" +
                             " FROM trips WHERE company_id = " + company_id + " ;");

        queries.put("stop_times", "SELECT " +
                                  "'trip_id' AS trip_id," +
                                  "'arrival_time' AS arrival_time," +
                                  "'departure_time' AS departure_time," +
                                  "'stop_id' AS stop_id," +
                                  "'stop_sequence' AS stop_sequence," +
                                  "'stop_headsign' AS stop_headsign," +
                                  "'pickup_type' AS pickup_type," +
                                  "'drop_off_type' AS drop_off_type," +
                                  "'continuous_pickup' AS continuous_pickup," +
                                  "'continuous_drop_off' AS continuous_drop_off," +
                                  "'shape_dist_traveled' AS shape_dist_traveled," +
                                  "'timepoint' AS timepoint" +
                                  " UNION " +
                                  " SELECT " +
                                  "CONCAT('\"',COALESCE(trip_id::text, ''), '\"') AS trip_id," +
                                  "CONCAT('\"',COALESCE(to_char(make_interval(secs => extract(epoch from arrival_time)), 'HH24:MI:SS'), ''), '\"') AS arrival_time," +
                                  "CONCAT('\"',COALESCE(to_char(make_interval(secs => extract(epoch from departure_time)), 'HH24:MI:SS'), ''), '\"') AS departure_time," +
                                  "CONCAT('\"',COALESCE(stop_id::text, ''), '\"') AS stop_id," +
                                  "CONCAT('\"',COALESCE(stop_sequence::text, ''), '\"') AS stop_sequence," +
                                  "CONCAT('\"',COALESCE(stop_headsign::text, ''), '\"') AS stop_headsign," +
                                  "CONCAT('\"',COALESCE(pickup_type::text, ''), '\"') AS pickup_type," +
                                  "CONCAT('\"',COALESCE(drop_off_type::text, ''), '\"') AS drop_off_type," +
                                  "CONCAT('\"',COALESCE(continuous_pickup::text, ''), '\"') AS continuous_pickup," +
                                  "CONCAT('\"',COALESCE(continuous_drop_off::text, ''), '\"') AS continuous_drop_off," +
                                  "CONCAT('\"',COALESCE(round(shape_dist_traveled::float8::numeric, 2)::float8::text, ''), '\"') AS shape_dist_traveled," +
                                  "CONCAT('\"',COALESCE(timepoint::text, '') , '\"')AS timepoint" +
                                  " FROM stop_times WHERE company_id = " + company_id + " ;");

        queries.put("calendar", "SELECT " +
                                "'service_id' AS service_id," +
                                "'monday' AS monday," +
                                "'tuesday' AS tuesday," +
                                "'wednesday' AS wednesday," +
                                "'thursday' AS thursday," +
                                "'friday' AS friday," +
                                "'saturday' AS saturday," +
                                "'sunday' AS sunday," +
                                "'start_date' AS start_date," +
                                "'end_date' AS end_date" +
                                " UNION " +
                                " SELECT " +
                                "CONCAT('\"',COALESCE(service_id::text, ''), '\"') AS service_id," +
                                "CONCAT('\"',COALESCE(monday::text, ''), '\"') AS monday," +
                                "CONCAT('\"',COALESCE(tuesday::text, ''), '\"') AS tuesday," +
                                "CONCAT('\"',COALESCE(wednesday::text, ''), '\"') AS wednesday," +
                                "CONCAT('\"',COALESCE(thursday::text, ''), '\"') AS thursday," +
                                "CONCAT('\"',COALESCE(friday::text, ''), '\"') AS friday," +
                                "CONCAT('\"',COALESCE(saturday::text, ''), '\"') AS saturday," +
                                "CONCAT('\"',COALESCE(sunday::text, ''), '\"') AS sunday," +
                                "CONCAT('\"',COALESCE(to_char(start_date, 'YYYYMMDD'), ''), '\"') AS start_date," +
                                "CONCAT('\"',COALESCE(to_char(end_date, 'YYYYMMDD'), ''), '\"') AS end_date" +
                                " FROM calendar WHERE company_id = " + company_id + " ;");

        queries.put("calendar_dates", "SELECT " +
                                   "'service_id' AS service_id," +
                                   "'date' AS date," +
                                   "'exception_type' AS exception_type" +
                                   " UNION " +
                                   " SELECT " +
                                   "CONCAT('\"',COALESCE(service_id::text, ''), '\"') AS service_id," +
                                   "CONCAT('\"',COALESCE(to_char(date, 'YYYYMMDD'), ''), '\"') AS date," +
                                   "CONCAT('\"',COALESCE(exception_type::text, ''), '\"') AS exception_type" +
                                   " FROM calendar_dates WHERE company_id = " + company_id + " ;");

        queries.put("fare_attributes", "SELECT " +
                                       "'fare_id' AS fare_id," +
                                       "'price' AS price," +
                                       "'currency_type' AS currency_type," +
                                       "'payment_method' AS payment_method," +
                                       "'transfers' AS transfers," +
                                       "'agency_id' AS agency_id," +
                                       "'transfer_duration' AS transfer_duration" +
                                       " UNION " +
                                       " SELECT " +
                                       "CONCAT('\"', COALESCE(fare_id::text, ''), '\"') AS fare_id," +
                                       "CONCAT('\"', COALESCE(price::text, ''), '\"') AS price," +
                                       "CONCAT('\"', COALESCE(currency_type::text, ''), '\"') AS currency_type," +
                                       "CONCAT('\"', COALESCE(payment_method::text, ''), '\"') AS payment_method," +
                                       "CONCAT('\"', COALESCE(transfers::text, ''), '\"') AS transfers," +
                                       "CONCAT('\"', COALESCE(agency_id::text, ''), '\"') AS agency_id," +
                                       "CONCAT('\"', COALESCE(transfer_duration::text, ''), '\"') AS transfer_duration" +
                                       " FROM fare_attributes WHERE company_id = " + company_id + " ;");

        queries.put("fare_rules", "SELECT " +
                                "'fare_id' AS fare_id," +
                                "'route_id' AS route_id," +
                                "'origin_id' AS origin_id," +
                                "'destination_id' AS destination_id," +
                                "'contains_id' AS contains_id" +
                                " UNION " +
                                "SELECT " +
                                "CONCAT('\"',COALESCE(fare_id::text, ''), '\"') AS fare_id," +
                                "CONCAT('\"',COALESCE(route_id::text, ''), '\"') AS route_id," +
                                "CONCAT('\"',COALESCE(origin_id::text, ''), '\"') AS origin_id," +
                                "CONCAT('\"',COALESCE(destination_id::text, ''), '\"') AS destination_id," +
                                "CONCAT('\"',COALESCE(contains_id::text, ''), '\"') AS contains_id" +
                                " FROM fare_rules WHERE company_id = " + company_id + " ;");

        queries.put("shapes", "SELECT " +
                              "'shape_id' AS shape_id," +
                              "'shape_pt_lat' AS shape_pt_lat," +
                              "'shape_pt_lon' AS shape_pt_lon," +
                              "'shape_pt_sequence' AS shape_pt_sequence," +
                              "'shape_dist_traveled' AS shape_dist_traveled" +
                              " UNION " +
                              " SELECT " +
                              "CONCAT('\"',COALESCE(shape_id::text, ''), '\"') AS shape_id," +
                              "CONCAT('\"',COALESCE(shape_pt_lat::text, ''), '\"') AS shape_pt_lat," +
                              "CONCAT('\"',COALESCE(shape_pt_lon::text, ''), '\"') AS shape_pt_lon," +
                              "CONCAT('\"',COALESCE(shape_pt_sequence::text, ''), '\"') AS shape_pt_sequence," +
                              "CONCAT('\"',COALESCE(round(shape_dist_traveled::float8::numeric, 2)::float8::text, '') , '\"') AS shape_dist_traveled" +
                              " FROM shapes WHERE company_id = " + company_id + " ;");

        queries.put("frequencies", "SELECT " +
                                   "'trip_id' AS trip_id," +
                                   "'start_time' AS start_time," +
                                   "'end_time' AS end_time," +
                                   "'headway_secs' AS headway_secs," +
                                   "'exact_times' AS exact_times" +
                                   " UNION " +
                                   " SELECT " +
                                   "CONCAT('\"', COALESCE(trip_id::text, ''), '\"') AS trip_id," +
                                   "CONCAT('\"', COALESCE(to_char(make_interval(secs => extract(epoch from start_time)), 'HH24:MI:SS'), ''), '\"') AS start_time," +
                                   "CONCAT('\"', COALESCE(to_char(make_interval(secs => extract(epoch from end_time)), 'HH24:MI:SS'), ''), '\"') AS end_time," +
                                   "CONCAT('\"', COALESCE(headway_secs::text, ''), '\"') AS headway_secs," +
                                   "CONCAT('\"', COALESCE(exact_times::text, ''), '\"') AS exact_times" +
                                   " FROM frequencies WHERE company_id = " + company_id + " ;");

        queries.put("feed_info", "SELECT " +
                                 "'feed_publisher_name' AS feed_publisher_name," +
                                 "'feed_publisher_url' AS feed_publisher_url," +
                                 "'feed_lang' AS feed_lang," +
                                 "'default_lang' AS default_lang," +
                                 "'feed_start_date' AS feed_start_date," +
                                 "'feed_end_date' AS feed_end_date," +
                                 "'feed_version' AS feed_version," +
                                 "'feed_contact_email' AS feed_contact_email," +
                                 "'feed_contact_url' AS feed_contact_url" +
                                 " UNION " +
                                 " SELECT " +
                                 "CONCAT('\"', COALESCE(feed_publisher_name::text, ''), '\"') AS feed_publisher_name," +
                                 "CONCAT('\"', COALESCE(feed_publisher_url::text, ''), '\"') AS feed_publisher_url," +
                                 "CONCAT('\"', COALESCE(feed_lang::text, ''), '\"') AS feed_lang," +
                                 "CONCAT('\"', COALESCE(default_lang::text, ''), '\"') AS default_lang," +
                                 "CONCAT('\"', COALESCE(to_char(feed_start_date, 'YYYYMMDD'), ''), '\"') AS feed_start_date," +
                                 "CONCAT('\"', COALESCE(to_char(feed_end_date, 'YYYYMMDD'), ''), '\"') AS feed_end_date," +
                                 "CONCAT('\"', COALESCE(feed_version::text, ''), '\"') AS feed_version," +
                                 "CONCAT('\"', COALESCE(feed_contact_email::text, ''), '\"') AS feed_contact_email," +
                                 "CONCAT('\"', COALESCE(feed_contact_url::text, ''), '\"') AS feed_contact_url" +
                                 " FROM feed_info WHERE company_id = " + company_id + " ;");

        queries.put("attributions", "SELECT " +
                                    "'attribution_id' AS attribution_id," +
                                    "'agency_id' AS agency_id," +
                                    "'route_id' AS route_id," +
                                    "'trip_id' AS trip_id," +
                                    "'organization_name' AS organization_name," +
                                    "'is_producer' AS is_producer," +
                                    "'is_operator' AS is_operator," +
                                    "'is_authority' AS is_authority," +
                                    "'attribution_url' AS attribution_url," +
                                    "'attribution_email' AS attribution_email," +
                                    "'attribution_phone' AS attribution_phone" +
                                    " UNION " +
                                    " SELECT " +
                                    "CONCAT('\"', COALESCE(attribution_id::text, ''), '\"') AS attribution_id," +
                                    "CONCAT('\"', COALESCE(agency_id::text, ''), '\"') AS agency_id," +
                                    "CONCAT('\"', COALESCE(route_id::text, ''), '\"') AS route_id," +
                                    "CONCAT('\"', COALESCE(trip_id::text, ''), '\"') AS trip_id," +
                                    "CONCAT('\"', COALESCE(organization_name::text, ''), '\"') AS organization_name," +
                                    "CONCAT('\"', COALESCE(is_producer::text, ''), '\"') AS is_producer," +
                                    "CONCAT('\"', COALESCE(is_operator::text, ''), '\"') AS is_operator," +
                                    "CONCAT('\"', COALESCE(is_authority::text, ''), '\"') AS is_authority," +
                                    "CONCAT('\"', COALESCE(attribution_url::text, ''), '\"') AS attribution_url," +
                                    "CONCAT('\"', COALESCE(attribution_email::text, ''), '\"') AS attribution_email," +
                                    "CONCAT('\"', COALESCE(attribution_phone::text, ''), '\"') AS attribution_phone" +
                                    " FROM attributions WHERE company_id = " + company_id + " ;");

        queries.put("transfers", "SELECT " +
                                 "'from_stop_id' AS from_stop_id," +
                                 "'to_stop_id' AS to_stop_id," +
                                 "'transfer_type' AS transfer_type," +
                                 "'min_transfer_time' AS min_transfer_time" +
                                 " UNION " +
                                 " SELECT " +
                                 "CONCAT('\"', COALESCE(from_stop_id::text, ''), '\"') AS from_stop_id," +
                                 "CONCAT('\"', COALESCE(to_stop_id::text, ''), '\"') AS to_stop_id," +
                                 "CONCAT('\"', COALESCE(transfer_type::text, ''), '\"') AS transfer_type," +
                                 "CONCAT('\"', COALESCE(min_transfer_time::text, ''), '\"') AS min_transfer_time" +
                                 " FROM transfers WHERE company_id = " + company_id + " ;");

        queries.put("translations", "SELECT " +
                                    "'table_name' AS table_name," +
                                    "'field_name' AS field_name," +
                                    "'language' AS language," +
                                    "'translation' AS translation," +
                                    "'record_id' AS record_id," +
                                    "'record_sub_id' AS record_sub_id," +
                                    "'field_value' AS field_value" +
                                    " UNION " +
                                    " SELECT " +
                                    "CONCAT('\"', COALESCE(table_name::text, ''), '\"') AS table_name," +
                                    "CONCAT('\"', COALESCE(field_name::text, ''), '\"') AS field_name," +
                                    "CONCAT('\"', COALESCE(language::text, ''), '\"') AS language," +
                                    "CONCAT('\"', COALESCE(translation::text, ''), '\"') AS translation," +
                                    "CONCAT('\"', COALESCE(record_id::text, ''), '\"') AS record_id," +
                                    "CONCAT('\"', COALESCE(record_sub_id::text, ''), '\"') AS record_sub_id," +
                                    "CONCAT('\"', COALESCE(field_value::text, ''), '\"') AS field_value" +
                                    " FROM working_data.translations WHERE company_id = " + company_id + " ;");

        return queries.get(tableName);

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public ImmutableSet<String> getFilenames() throws SQLException {
        ImmutableSet<String> tables = ImmutableSet.of("agency", "calendar", "stops", "routes", "trips", "stop_times", "calendar_dates", "fare_attributes", "fare_rules", "shapes", "frequencies", "transfers", "feed_info");
        List<String> tables_with_data = new ArrayList<>();
        for (String table : tables) {
            ResultSet limited_rs = this.ExecuteQuery("SELECT * FROM " + table + " WHERE company_id = " + this.company_id + "  LIMIT 1 ;");
            if (limited_rs.isBeforeFirst() ) {
                tables_with_data.add(table + ".txt");
            }
        }

        return ImmutableSet.copyOf(tables_with_data);
    }

    @Override
    public InputStream getFile(String filename) throws SQLException {

        String select_query = StringUtils.chop(getQueryString(filename).trim());
        int ColumnsCount = getColumns(filename).split(",").length;

        // The header row, then every row of the company in one query streamed through a cursor
        String header_query = select_query.split("UNION")[0];
        String rows_query = select_query.split("UNION")[1] + getOrderBy(filename);

        InputStream ResultSetInputStream = new ResultSetAsInputStream(this.conn, header_query, ColumnsCount);
        InputStream ResultSetInputStreamRows = new ResultSetAsInputStream(this.conn, rows_query, ColumnsCount);
        return new java.io.SequenceInputStream(ResultSetInputStream, ResultSetInputStreamRows);

    }
}



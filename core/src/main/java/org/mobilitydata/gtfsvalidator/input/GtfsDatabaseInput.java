package org.mobilitydata.gtfsvalidator.input;
import com.google.common.collect.ImmutableSet;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;

public class GtfsDatabaseInput extends GtfsInput {

    private final Connection conn;

    private final String company_id;


    public GtfsDatabaseInput(String db, String company_id) throws  SQLException {

        this.company_id = company_id;

        String host = null;
        String user = null;
        String password = null;

        if (System.getenv("DATABASE_HOST") != null || System.getenv("DATABASE_USER") != null || System.getenv("DATABASE_PASSWORD") != null) {
            host = System.getenv("DATABASE_HOST");
            user = System.getenv("DATABASE_USER");
            password = System.getenv("DATABASE_PASSWORD");
        } else {
            host = "localhost";
            user = "root";
            password = "0112358";
        }

        String connection_url = "jdbc:mysql://" + host + ":3306/" + db;

        try{
            Class.forName("com.mysql.cj.jdbc.Driver");
            this.conn = DriverManager.getConnection(connection_url, user, password);

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
                              "COALESCE(agency_id, '') AS agency_id," +
                              "COALESCE(agency_name, '') AS agency_name," +
                              "COALESCE(agency_url, '') AS agency_url," +
                              "COALESCE(agency_timezone, '') AS agency_timezone," +
                              "COALESCE(agency_lang, '') AS agency_lang," +
                              "COALESCE(agency_phone, '') AS agency_phone," +
                              "COALESCE(agency_fare_url, '') AS agency_fare_url," +
                              "COALESCE(agency_email, '') AS agency_email" +
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
                              "COALESCE(route_id, '') AS route_id," +
                              "COALESCE(agency_id, '') AS agency_id," +
                              "COALESCE(route_short_name, '') AS route_short_name," +
                              "COALESCE(route_long_name, '') AS route_long_name," +
                              "COALESCE(route_desc, '') AS route_desc," +
                              "COALESCE(route_type, '') AS route_type," +
                              "COALESCE(route_url, '') AS route_url," +
                              "COALESCE(route_color, '') AS route_color," +
                              "COALESCE(route_text_color, '') AS route_text_color," +
                              "COALESCE(route_sort_order, '') AS route_sort_order," +
                              "COALESCE(continuous_pickup, '') AS continuous_pickup," +
                              "COALESCE(continuous_drop_off, '') AS continuous_drop_off" +
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
                             "COALESCE(stop_id, '') AS stop_id," +
                             "COALESCE(stop_code, '') AS stop_code," +
                             "COALESCE(stop_name, '') AS stop_name," +
                             "COALESCE(stop_desc, '') AS stop_desc," +
                             "COALESCE(stop_lat, '') AS stop_lat," +
                             "COALESCE(stop_lon, '') AS stop_lon," +
                             "COALESCE(zone_id, '') AS zone_id," +
                             "COALESCE(stop_url, '') AS stop_url," +
                             "COALESCE(location_type, '') AS location_type," +
                             "COALESCE(parent_station, '') AS parent_station," +
                             "COALESCE(stop_timezone, '') AS stop_timezone," +
                             "COALESCE(wheelchair_boarding, '') AS wheelchair_boarding," +
                             "COALESCE(level_id, '') AS level_id," +
                             "COALESCE(platform_code, '') AS platform_code" +
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
                             "COALESCE(route_id, '') AS route_id," +
                             "COALESCE(service_id, '') AS service_id," +
                             "COALESCE(trip_id, '') AS trip_id," +
                             "COALESCE(trip_headsign, '') AS trip_headsign," +
                             "COALESCE(trip_short_name, '') AS trip_short_name," +
                             "COALESCE(direction_id, '') AS direction_id," +
                             "COALESCE(block_id, '') AS block_id," +
                             "COALESCE(shape_id, '') AS shape_id," +
                             "COALESCE(wheelchair_accessible, '') AS wheelchair_accessible," +
                             "COALESCE(bikes_allowed, '') AS bikes_allowed" +
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
                                  "COALESCE(trip_id, '') AS trip_id," +
                                  "COALESCE(arrival_time, '') AS arrival_time," +
                                  "COALESCE(departure_time, '') AS departure_time," +
                                  "COALESCE(stop_id, '') AS stop_id," +
                                  "COALESCE(stop_sequence, '') AS stop_sequence," +
                                  "COALESCE(stop_headsign, '') AS stop_headsign," +
                                  "COALESCE(pickup_type, '') AS pickup_type," +
                                  "COALESCE(drop_off_type, '') AS drop_off_type," +
                                  "COALESCE(continuous_pickup, '') AS continuous_pickup," +
                                  "COALESCE(continuous_drop_off, '') AS continuous_drop_off," +
                                  "COALESCE(ROUND(shape_dist_traveled, 2), '') AS shape_dist_traveled," +
                                  "COALESCE(timepoint, '') AS timepoint" +
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
                                "COALESCE(service_id, '') AS service_id," +
                                "COALESCE(monday, '') AS monday," +
                                "COALESCE(tuesday, '') AS tuesday," +
                                "COALESCE(wednesday, '') AS wednesday," +
                                "COALESCE(thursday, '') AS thursday," +
                                "COALESCE(friday, '') AS friday," +
                                "COALESCE(saturday, '') AS saturday," +
                                "COALESCE(sunday, '') AS sunday," +
                                "COALESCE(DATE_FORMAT(start_date, '%Y%m%d'), '') AS start_date," +
                                "COALESCE(DATE_FORMAT(end_date, '%Y%m%d'), '') AS end_date" +
                                " FROM calendar WHERE company_id = " + company_id + " ;");

        queries.put("calendar_dates", "SELECT " +
                                   "'service_id' AS service_id," +
                                   "'date' AS date," +
                                   "'exception_type' AS exception_type" +
                                   " UNION " +
                                   " SELECT " +
                                   "COALESCE(service_id, '') AS service_id," +
                                   "COALESCE(DATE_FORMAT(date, '%Y%m%d'), '') AS date," +
                                   "COALESCE(exception_type, '') AS exception_type" +
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
                                       "COALESCE(fare_id, '') AS fare_id," +
                                       "COALESCE(price, '') AS price," +
                                       "COALESCE(currency_type, '') AS currency_type," +
                                       "COALESCE(payment_method, '') AS payment_method," +
                                       "COALESCE(transfers, '') AS transfers," +
                                       "COALESCE(agency_id, '') AS agency_id," +
                                       "COALESCE(transfer_duration, '') AS transfer_duration" +
                                       " FROM fare_attributes WHERE company_id = " + company_id + " ;");

        queries.put("fare_rules", "SELECT " +
                                "'fare_id' AS fare_id," +
                                "'route_id' AS route_id," +
                                "'origin_id' AS origin_id," +
                                "'destination_id' AS destination_id," +
                                "'contains_id' AS contains_id" +
                                " UNION " +
                                "SELECT " +
                                "COALESCE(fare_id, '') AS fare_id," +
                                "COALESCE(route_id, '') AS route_id," +
                                "COALESCE(origin_id, '') AS origin_id," +
                                "COALESCE(destination_id, '') AS destination_id," +
                                "COALESCE(contains_id, '') AS contains_id" +
                                " FROM fare_rules WHERE company_id = " + company_id + " ;");

        queries.put("shapes", "SELECT " +
                              "'shape_id' AS shape_id," +
                              "'shape_pt_lat' AS shape_pt_lat," +
                              "'shape_pt_lon' AS shape_pt_lon," +
                              "'shape_pt_sequence' AS shape_pt_sequence," +
                              "'shape_dist_traveled' AS shape_dist_traveled" +
                              " UNION " +
                              " SELECT " +
                              "COALESCE(shape_id, '') AS shape_id," +
                              "COALESCE(shape_pt_lat, '') AS shape_pt_lat," +
                              "COALESCE(shape_pt_lon, '') AS shape_pt_lon," +
                              "COALESCE(shape_pt_sequence, '') AS shape_pt_sequence," +
                              "COALESCE(ROUND(shape_dist_traveled,2), '') AS shape_dist_traveled" +
                              " FROM shapes WHERE company_id = " + company_id + " ;");

        queries.put("frequencies", "SELECT " +
                                   "'trip_id' AS trip_id," +
                                   "'start_time' AS start_time," +
                                   "'end_time' AS end_time," +
                                   "'headway_secs' AS headway_secs," +
                                   "'exact_times' AS exact_times" +
                                   " UNION " +
                                   " SELECT " +
                                   "COALESCE(trip_id, '') AS trip_id," +
                                   "COALESCE(start_time, '') AS start_time," +
                                   "COALESCE(end_time, '') AS end_time," +
                                   "COALESCE(headway_secs, '') AS headway_secs," +
                                   "COALESCE(exact_times, '') AS exact_times" +
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
                                 "COALESCE(feed_publisher_name, '') AS feed_publisher_name," +
                                 "COALESCE(feed_publisher_url, '') AS feed_publisher_url," +
                                 "COALESCE(feed_lang, '') AS feed_lang," +
                                 "COALESCE(default_lang, '') AS default_lang," +
                                 "COALESCE(DATE_FORMAT(feed_start_date, '%Y%m%d'), '') AS feed_start_date," +
                                 "COALESCE(DATE_FORMAT(feed_end_date, '%Y%m%d'), '') AS feed_end_date," +
                                 "COALESCE(feed_version, '') AS feed_version," +
                                 "COALESCE(feed_contact_email, '') AS feed_contact_email," +
                                 "COALESCE(feed_contact_url, '') AS feed_contact_url" +
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
                                    "COALESCE(attribution_id, '') AS attribution_id," +
                                    "COALESCE(agency_id, '') AS agency_id," +
                                    "COALESCE(route_id, '') AS route_id," +
                                    "COALESCE(trip_id, '') AS trip_id," +
                                    "COALESCE(organization_name, '') AS organization_name," +
                                    "COALESCE(is_producer, '') AS is_producer," +
                                    "COALESCE(is_operator, '') AS is_operator," +
                                    "COALESCE(is_authority, '') AS is_authority," +
                                    "COALESCE(attribution_url, '') AS attribution_url," +
                                    "COALESCE(attribution_email, '') AS attribution_email," +
                                    "COALESCE(attribution_phone, '') AS attribution_phone" +
                                    " FROM attributions WHERE company_id = " + company_id + " ;");

        queries.put("transfers", "SELECT " +
                                 "'from_stop_id' AS from_stop_id," +
                                 "'to_stop_id' AS to_stop_id," +
                                 "'transfer_type' AS transfer_type," +
                                 "'min_transfer_time' AS min_transfer_time" +
                                 " UNION " +
                                 " SELECT " +
                                 "COALESCE(from_stop_id, '') AS from_stop_id," +
                                 "COALESCE(to_stop_id, '') AS to_stop_id," +
                                 "COALESCE(transfer_type, '') AS transfer_type," +
                                 "COALESCE(min_transfer_time, '') AS min_transfer_time" +
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
                                    "COALESCE(table_name, '') AS table_name," +
                                    "COALESCE(field_name, '') AS field_name," +
                                    "COALESCE(language, '') AS language," +
                                    "COALESCE(translation, '') AS translation," +
                                    "COALESCE(record_id, '') AS record_id," +
                                    "COALESCE(record_sub_id, '') AS record_sub_id," +
                                    "COALESCE(field_value, '') AS field_value" +
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

        String select_query = getQueryString(filename);
        int ColumnsCount = getColumns(filename).split(",").length;
        return new ResultSetAsInputStream(this.conn, select_query, ColumnsCount);
    }
}



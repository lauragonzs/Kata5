package kata5;

import java.io.File;
import java.sql.SQLException;
import java.util.Arrays;

public class Kata5 {
    
    public static void main(String[] args) throws SQLException {
        FlightStore store = new SqliteFlightStore(new File("flights.db"));
        int[] hours = new int[24];
        for (Flight flight : store.flights()) {
            hours[flight.getArrivalTime().getHour()]++;
        }
        System.out.println(Arrays.toString(hours));
    }
}
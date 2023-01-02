package kata5;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.DayOfWeek;
import java.time.LocalTime;
import static java.util.Collections.*;
import java.util.Iterator;

public class SqliteFlightStore implements FlightStore {
    private final File file;
    private final Connection connection;

    public SqliteFlightStore(File file) throws SQLException {
        this.file = file;
        this.connection = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
    }
    
    @Override
    public Iterable<Flight> flights() {
        return new Iterable<Flight>() {

            @Override
            public Iterator<Flight> iterator() {
                try {
                    return createIterator();
                } catch (SQLException ex) {
                    return emptyIterator();
                }
            };
            
        };
    }

    private Iterator<Flight> createIterator() throws SQLException {
        ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM flights");
        return new Iterator<Flight>() {
            Flight nextFlight = nextFlight(rs);

            @Override
            public boolean hasNext() {
                return nextFlight != null;
            }

            @Override
            public Flight next() {
                Flight flight = nextFlight;
                nextFlight = nextFlight(rs);
                return flight;
            }

        };
    }
    
    private Flight nextFlight(ResultSet rs) {
        if (!next(rs)) return null;
        return new Flight(
            DayOfWeek.of(getInt(rs,"DAY_OF_WEEK")),
            timeOf(getInt(rs,"DEP_TIME")),
            getInt(rs,"DEP_DELAY"),
            timeOf(getInt(rs,"ARR_TIME")),
            getInt(rs,"ARR_DELAY"),
            getInt(rs,"AIR_TIME"),
            getInt(rs,"DISTANCE"),
            getInt(rs,"CANCELLED") == 1,
            getInt(rs,"DIVERTED") == 1
        );

    }

    private LocalTime timeOf(int d) {
        return LocalTime.of(d / 100 % 24, d%100);
    }

    private int getInt(ResultSet rs, String field) {
        try {
            return rs.getInt(field);
        } catch (SQLException ex) {
            return 0;
        }
    }

    private boolean next(ResultSet rs) {
        try {
            boolean next = rs.next();
            if (next) return true;
            rs.close();            
        } catch (SQLException ex) {
        }
        return false;
    }
}

package fax.play.service;

import java.sql.SQLException;

@FunctionalInterface
public interface CheckedSQLFunction<T, R> {

   R apply(T t) throws SQLException;

}

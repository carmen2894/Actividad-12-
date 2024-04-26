import com.mysql.cj.jdbc.MysqlDataSource;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.util.*;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) throws SQLException {

        Properties props = new Properties();

        try{
            props.load(Files.newInputStream(Path.of("music.properties"),
                    StandardOpenOption.READ));
            props.load(new FileInputStream("music.properties"));
        }catch(IOException e){
            throw new RuntimeException(e);
        }
        var dataSource = new MysqlDataSource();
        dataSource.setServerName(props.getProperty("serverName"));
        dataSource.setPort(Integer.parseInt(props.getProperty("port")));
        dataSource.setDatabaseName(props.getProperty("databaseName"));
        dataSource.setServerTimezone("UTC");
        try(
                Connection connection = dataSource.getConnection(
                        props.getProperty("user"),
                        System.getenv("MYSQL_PASS")
                );
                Statement statement = connection.createStatement();
        ){
            System.out.println("Conexion realizada con exito");


            String tableName = "music.artists";
            String columnName = "artist_name";
            String columnValue = "Ariana Grande";
            String[] columnNames = {"track_number,song_title,album_id"};
           String[] columnValues = {"1","song1","881"};
            String[] songs = new String[]{
                    "I’m The Best",
                    "Fly",
                    "Save Me",
            };


            /* 4 updtate record

            String[] updateColumns = {"album_name"};
            String[] updateValues = {"Álbum Actualizado"};
            String conditionColumn = "album_name";
            String conditionValue = "Grace";

            updateRecord(statement, "music.albums", updateColumns, updateValues, conditionColumn, conditionValue);
            */


           //-- 2 Agregar un album
            /*           String[] albumColumns = {"album_name", "artist_id"};
            String[] albumValues = {"Save me", "210"};
            insertRecord(statement, "music.albums", albumColumns, albumValues);
            */


             // 1  Agregar un artista:
                    insertRecord(statement, "music.artists", new String[]{"artist_name"}, new String[]{"Nicki Minaj"});


             //  -   insertArtistAlbum(statement,"Nicki Minaj","Pink Friday",songs);


            // 3 Eliminar artista:

            //      deleteArtist(statement, "Nicki Minaj");

            //  2 Agregar un album
              //insertArtistAlbum(statement,"Nicki Minaj","Pink Friday",songs);

            // 5 nuevo album y artista
             //insertArtistAlbum(statement,"Selena Gomez ","Thank U, Next",songs);




        }catch (SQLException e){
            throw new RuntimeException(e);
        }
    }
    private static boolean printRecords(ResultSet resultSet) throws SQLException {
        boolean foundData = false;
        var meta = resultSet.getMetaData();
        System.out.println("====================");
        for(int i=1; i<meta.getColumnCount();i++){
            System.out.printf("%-15s",meta.getColumnName(i).toUpperCase());
        }
        System.out.println();

        while (resultSet.next()){
            for(int i=1; i<= meta.getColumnCount();i++){
                System.out.printf("%-15s ", resultSet.getString(i));
            }
            System.out.println();
            foundData = true;
        }
        return foundData;
    }

    private static boolean executeSelect(Statement statement, String table,
                                         String columnName,String columnValue) throws SQLException {
        String query = "SELECT * FROM %s WHERE %s='%s'".formatted(table,columnName,columnValue);
        var resultSet = statement.executeQuery(query);
        if(resultSet!=null){
            return printRecords(resultSet);
        }
        return false;
    }

    private static boolean insertRecord(Statement statement, String table,
                                        String[] columnNames, String[] columnValues) throws SQLException {
        String colNames = String.join(",", columnNames);
        String colValues = String.join(",", columnValues);
        String query = "INSERT INTO %s (%s) VALUES ('%s')".formatted(table, colNames, colValues);
        System.out.println(query);
        boolean insertResult = statement.execute(query);
        int recordsInserted = statement.getUpdateCount();
        return recordsInserted > 0;
    }
    private static void deleteArtist(Statement statement, String artistName) throws SQLException {
        // Primero, obtenemos el ID del artista
        String artistIdQuery = "SELECT artist_id FROM music.artists WHERE artist_name = '%s'".formatted(artistName);
        ResultSet resultSet = statement.executeQuery(artistIdQuery);
        if (!resultSet.next()) {
            System.out.println("No se encontró el artista con el nombre especificado.");
            return;
        }
        int artistId = resultSet.getInt("artist_id");

        // Luego, eliminamos todas las canciones asociadas a los álbumes del artista
        String deleteSongsQuery = "DELETE FROM music.songs WHERE album_id IN " +
                "(SELECT album_id FROM music.albums WHERE artist_id = %d)".formatted(artistId);
        System.out.println(deleteSongsQuery);
        statement.execute(deleteSongsQuery);

        // Después, eliminamos todos los álbumes asociados al artista
        String deleteAlbumsQuery = "DELETE FROM music.albums WHERE artist_id = %d".formatted(artistId);
        System.out.println(deleteAlbumsQuery);
        statement.execute(deleteAlbumsQuery);

        // Finalmente, eliminamos al artista
        String deleteArtistQuery = "DELETE FROM music.artists WHERE artist_id = %d".formatted(artistId);
        System.out.println(deleteArtistQuery);
        statement.execute(deleteArtistQuery);

        System.out.println("Artista y sus álbumes asociados eliminados con éxito.");


    }

    private static boolean updateRecord(Statement statement, String table,
                                        String[] updateColumns, String[] updateValues,
                                        String conditionColumn, String conditionValue) throws SQLException {
        String setClause = String.join(",", Arrays.stream(updateColumns)
                .map(col -> col + " = ?")
                .collect(Collectors.toList()));

        String query = "UPDATE %s SET %s WHERE %s = ?".formatted(table, setClause, conditionColumn);

        System.out.println(query);

        PreparedStatement preparedStatement = statement.getConnection().prepareStatement(query);
        for (int i = 0; i < updateValues.length; i++) {
            preparedStatement.setString(i + 1, updateValues[i]);
        }
        preparedStatement.setString(updateValues.length + 1, conditionValue);

        boolean updateResult = preparedStatement.executeUpdate() > 0;
        preparedStatement.close();

        if (updateResult) {
            System.out.println("Registro actualizado con éxito.");
        } else {
            System.out.println("No se pudo encontrar el registro para actualizar.");
        }

        return updateResult;
    }

    private static void insertArtistAlbum(Statement statement,
                                          String artistName,
                                          String albumName,String[] songs)
            throws SQLException {

        String artistInsert = "INSERT INTO music.artists (artist_name) VALUES (%s)"
                .formatted(statement.enquoteLiteral(artistName));
        System.out.println(artistInsert);
        statement.execute(artistInsert, Statement.RETURN_GENERATED_KEYS);

        ResultSet rs = statement.getGeneratedKeys();
        int artistId = (rs != null && rs.next()) ? rs.getInt(1) : -1;
        String albumInsert = ("INSERT INTO music.albums (album_name, artist_id)" +
                " VALUES (%s, %d)")
                .formatted(statement.enquoteLiteral(albumName), artistId);
        System.out.println(albumInsert);
        statement.execute(albumInsert, Statement.RETURN_GENERATED_KEYS);
        rs = statement.getGeneratedKeys();
        int albumId = (rs != null && rs.next()) ? rs.getInt(1) : -1;



        String songInsert = "INSERT INTO music.songs " +
                "(track_number, song_title, album_id) VALUES (%d, %s, %d)";

        for (int i = 0; i < songs.length; i++) {
            String songQuery = songInsert.formatted(i + 1,
                    statement.enquoteLiteral(songs[i]), albumId);
            System.out.println(songQuery);

            statement.execute(songQuery);
        }

    }
}



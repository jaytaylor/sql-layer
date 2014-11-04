SELECT * FROM artists WHERE (
 (artists.id NOT IN
    (SELECT join_albums_artists.artist_id FROM join_albums_artists
        INNER JOIN albums ON (albums.id = join_albums_artists.album_id)
        INNER JOIN join_albums_artists AS albums_artists_0 ON (albums_artists_0.album_id = albums.id)
        WHERE ((albums_artists_0.artist_id = 1) AND (join_albums_artists.artist_id IS NOT NULL))))
 OR (artists.id IS NULL))
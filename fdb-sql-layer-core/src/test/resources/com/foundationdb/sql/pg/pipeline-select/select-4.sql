SELECT artists.* FROM artists
     INNER JOIN artists AS b ON (b.id = artists.id)
     WHERE (artists.id IN (
         SELECT join_albums_artists.artist_id FROM join_albums_artists
             WHERE ((join_albums_artists.album_id IN (
                 SELECT albums.id FROM albums
                     INNER JOIN albums AS b ON (b.id = albums.id)
                     WHERE ((albums.id IN (1, 3)) AND (albums.id IS NOT NULL)))) AND
                 (join_albums_artists.artist_id IS NOT NULL))));

                             
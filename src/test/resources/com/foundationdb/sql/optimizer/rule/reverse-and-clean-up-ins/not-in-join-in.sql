SELECT * FROM artists
  WHERE ((artists.id NOT IN (SELECT albums.artist_id
                                   FROM albums
                                   INNER JOIN albums_tags ON (albums_tags.album_id = albums.id)
                                   WHERE ((albums_tags.tag_id IN (1, 2))))))
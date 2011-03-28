require 'rubygems'
gem 'memcached'
require 'json/ext'

class HomeController < ApplicationController

  def index

     @most_commented = Post.find_by_sql(<<-SQL
        SELECT p.*, count(*) num_comments 
          FROM posts p, comments c
         WHERE c.post_id = p.id
         GROUP BY p.id
         ORDER BY num_comments DESC
         LIMIT 10
      SQL
      )

     @highest_score = Post.find_by_sql(<<-SQL
        SELECT p.*, sum(pv.vote) calc_score
          FROM posts p, users u, post_votes pv
         WHERE p.user_id = u.id AND
               pv.post_id = p.id 
         GROUP BY p.id
         ORDER BY calc_score DESC
         LIMIT 10
      SQL
      )

     @most_friends = User.find_by_sql(<<-SQL
        SELECT u.*, count(*) num_friends
          FROM users u, user_friends uf
         WHERE u.id = uf.user_id
         GROUP BY u.id
         ORDER BY num_friends DESC
         LIMIT 10
     SQL
     )

  end

  def user
     userId = params[:id]

     @user = User.find( userId )

     @highest_score = Post.find_by_sql(<<-SQL
        SELECT p.*, sum(pv.vote) calc_score
          FROM posts p, users u, post_votes pv
         WHERE p.user_id = u.id AND
               pv.post_id = p.id AND
               pv.user_id IN (
                     SELECT uf.follow_user_id 
                       FROM user_friends uf 
                      WHERE uf.user_id = #{userId} )
         GROUP BY p.id
         ORDER BY calc_score DESC
         LIMIT 10
      SQL
      )
        
  end

  def post
    postId = params[:id]
    @post = Post.find( postId )
  end

  def postm
    postId = params[:id]
    if postId == nil
        raise "Couldn't find Post without an ID"
    end
    
    dbStr = MEMCACHED_CONFIG['database']
    connectStr = MEMCACHED_CONFIG['host'] + ":" + MEMCACHED_CONFIG['port']
    m = Memcached.new(connectStr);
    r = m.get( dbStr + ":posts:id=" + postId, false )
    j = JSON.parse( r );

    @post = j["@posts"][0]
    if @post == nil
        raise "Couldn't find Post with ID=" + postId
    end

    @users = Hash.new
    userKeys = @post["@comments"].collect {|p| dbStr + ":users:id=" + p["user_id"].to_s};
    userKeys << dbStr + ":users:id=" + @post["user_id"].to_s;
    results = m.get( userKeys, false );
    userKeys.each do |userKey|
      j = JSON.parse( results[userKey] )
      @users[ j["@users"][0]["id"] ] = j["@users"][0];      
    end

    @upVotes = @post["@post_votes"].select {|v| v["vote"] == 1}.size
    @downVotes = @post["@post_votes"].select {|v| v["vote"] == -1}.size
    @postScore = @upVotes - @downVotes
  end
end

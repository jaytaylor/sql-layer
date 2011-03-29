#
# Copyright (C) 2011 Akiban Technologies Inc.
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License, version 3,
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see http://www.gnu.org/licenses.
#

include Lipsum::Methods
include Power::Methods
include Name
include Title
include PostBody
include CommentMessage
include Tag
include Link

num_users = ARGV[0].to_i
num_posts = 10 * num_users
num_comments = 3 * num_posts
num_votes = 3 * num_comments
num_friends = 25 * num_users
num_tags = 5 * num_posts
num_links = 2 * num_posts

# Create users
(1..num_users).each do |t|
  user = User.new( :name => Name.full, :bio => Lipsum.words(15) )
  user.save!
end


# Create posts
(1..num_posts).each do |t|
  post = Post.new( :user_id => powerlaw(1,num_users,6), :title => Title.full, :body => PostBody.body )
  post.save!
end

# Create comments
(1..num_comments).each do |t|
  comment = Comment.new( :user_id => powerlaw(1,num_users,6), :post_id => powerlaw(1,num_posts,7), :message => CommentMessage.message )
  comment.save!
end

# Create votes
(1..num_votes).each do |t|
  vote = PostVote.new( :user_id => powerlaw(1,num_users,8), :post_id => powerlaw(1,num_posts,8), :vote => (rand 3) - 1 )   
  vote.save!
end

# Create friends
(1..num_friends).each do |t|
  friend = UserFriend.new( :user_id => powerlaw(1,num_users,8), :follow_user_id => powerlaw(1,num_users,8) );
  friend.save!
end

(1..num_tags).each do |t|
  tag = PostTag.new( :post_id => powerlaw(1,num_posts,8), :tag => Tag.tag );
  tag.save!
end

(1..num_links).each do |t|
  link = PostLink.new( :post_id => powerlaw(1,num_posts,2), :link_url => Link.url, :link_name => Title.full, :link_description => Lipsum.words(10) );
  link.save!
end

class Post < ActiveRecord::Base
  belongs_to :user
  has_many :post_votes
  has_many :comments
  has_many :post_tags
  has_many :post_links

  def score
    self.post_votes.map{|v| v.vote}.inject(0) {|s,v| s+v}
  end

  def down_votes
    self.post_votes.select{|v| v.vote == -1}
  end

  def up_votes
    self.post_votes.select{|v| v.vote == 1}
  end

end

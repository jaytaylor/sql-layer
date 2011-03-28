class CreatePostVotes < ActiveRecord::Migration
  def self.up
    create_table :post_votes do |t|
      t.references :user
      t.references :post
      t.integer :vote
    end
  end

  def self.down
    drop_table :post_votes
  end
end

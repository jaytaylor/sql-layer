class CreatePosts < ActiveRecord::Migration
  def self.up
    create_table :posts do |t|
      t.references :user
      t.string :title, :null => false, :default => "No Title"
      t.text :body
      t.timestamps
    end
  end

  def self.down
    drop_table :posts
  end
end

class CreatePostTags < ActiveRecord::Migration
  def self.up
    create_table :post_tags do |t|
      t.references :post
      t.string :tag
    end
  end

  def self.down
    drop_table :post_tags
  end
end

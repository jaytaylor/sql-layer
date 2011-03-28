class CreatePostLinks < ActiveRecord::Migration
  def self.up
    create_table :post_links do |t|
      t.references :post
      t.string :link_url
      t.string :link_name
      t.text :link_description
    end
  end

  def self.down
    drop_table :post_links
  end
end

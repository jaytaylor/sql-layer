class CreateUserFriends < ActiveRecord::Migration
  def self.up
    create_table :user_friends do |t|
      t.references :user
      t.integer :follow_user_id
    end
  end

  def self.down
    drop_table :user_friends
  end
end

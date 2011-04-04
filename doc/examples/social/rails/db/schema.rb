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

# This file is auto-generated from the current state of the database. Instead of editing this file, 
# please use the migrations feature of Active Record to incrementally modify your database, and
# then regenerate this schema definition.
#
# Note that this schema.rb definition is the authoritative source for your database schema. If you need
# to create the application database on another system, you should be using db:schema:load, not running
# all the migrations from scratch. The latter is a flawed and unsustainable approach (the more migrations
# you'll amass, the slower it'll run and the greater likelihood for issues).
#
# It's strongly recommended to check this file into your version control system.

ActiveRecord::Schema.define(:version => 20110224090819) do

  create_table "comments", :force => true do |t|
    t.integer  "post_id"
    t.integer  "user_id"
    t.text     "message"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "post_links", :force => true do |t|
    t.integer "post_id"
    t.string  "link_url"
    t.string  "link_name"
    t.text    "link_description"
  end

  create_table "post_tags", :force => true do |t|
    t.integer "post_id"
    t.string  "tag"
  end

  create_table "post_votes", :force => true do |t|
    t.integer "user_id"
    t.integer "post_id"
    t.integer "vote"
  end

  create_table "posts", :force => true do |t|
    t.integer  "user_id"
    t.string   "title",      :default => "No Title", :null => false
    t.text     "body"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "user_friends", :force => true do |t|
    t.integer "user_id"
    t.integer "follow_user_id"
  end

  create_table "users", :force => true do |t|
    t.string   "name",       :default => "No Name", :null => false
    t.text     "bio"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

end

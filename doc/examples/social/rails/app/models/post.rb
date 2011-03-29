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

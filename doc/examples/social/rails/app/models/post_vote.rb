class PostVote < ActiveRecord::Base
  belongs_to :user
  belongs_to :post
end

# frozen_string_literal: true

class User < ActiveRecord::Base
end

class UserWithDefaultScope < ActiveRecord::Base
  self.table_name = :users
  default_scope { order(name: :desc) }
end

class SpecialUserWithDefaultScope < ActiveRecord::Base
  self.table_name = :users
  default_scope { where(id: [1, 5, 6]) }
end

class Subscriber < ActiveRecord::Base
  self.primary_key = :nick
end

class Product < ActiveRecord::Base
  self.primary_key = [:shop_id, :id]
end

class Package < ActiveRecord::Base
end

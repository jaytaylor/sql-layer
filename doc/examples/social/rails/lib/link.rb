module Link

  URL = ["http://www.google.com", "http://www.akiban.com", "http://www.acquia.com", "http://www.yahoo.com", "http://www.microsoft.com", "http://www.oracle.com", "http://www.thestreet.com"]

  def self.url()
    URL[ rand( URL.size ) ]
  end

end

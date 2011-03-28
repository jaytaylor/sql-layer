module PostBody

  FIRST_PART = ["Several times a day I wonder if", "But, I remain commited to the idea of", "Certainly, there is a better way to", "Right, so to carry this idea even further down the road, what would happen if we moved everything to", "In closing, this is why I recommend", "I could have developed this application to use", "There are reports of people starting to demonstrate against the use of", "Taking a step back from these ideas, I had to check the project site to see if they still used", "No I won't use"]
  LAST_PART = ["Maven", "Ant", "Jenkins", "Java", "Ruby", "Rails", "Linux", "Ubuntu", "Redhat", "Oracle", "MySQL", "Drizzle", "Sybase", "Buildr", "Make", "GNU/Linux", "SPSS", "R", "SAS", "SQL Server", "Apache", "Nginx", "Lynx", "wget", "curl", "getopt", "bash", "tcsh"]

  def self.first()
    FIRST_PART[ rand( FIRST_PART.size ) ]
  end

  def self.last()
    LAST_PART[ rand( LAST_PART.size ) ]
  end

  def self.full()
    self.first + " " + self.last
  end

  def self.body()
    full + ". " + full + ". " + full
  end

end

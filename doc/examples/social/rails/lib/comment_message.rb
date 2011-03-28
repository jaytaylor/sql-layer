module CommentMessage

  FIRST_PART = ["Right on, I always though we should use", "First Comment about", "Carry that idea further, I'd like to learn more about", "I challenge you to a duel over your opinion on", "Yep, never liked", "Haven't had any problems with"]
  LAST_PART = ["Maven", "Ant", "Jenkins", "Java", "Ruby", "Rails", "Linux", "Ubuntu", "Redhat", "Oracle", "MySQL", "Drizzle", "Sybase", "Buildr", "Make", "GNU/Linux", "SPSS", "R", "SAS", "SQL Server", "Apache", "Nginx", "Lynx", "wget", "curl", "getopt", "bash", "tcsh"]

  def self.first()
    FIRST_PART[ rand( FIRST_PART.size ) ]
  end

  def self.last()
    LAST_PART[ rand( LAST_PART.size ) ]
  end

  def self.message()
    self.first + " " + self.last
  end

end

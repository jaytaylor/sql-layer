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

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

module Title

  FIRST_PART = ["The power of", "Three reasons to use", "Everything I ever needed to know about", "The first time I've ever used", "Honestly, I'm sick and tired of", "I hate", "What people need to know about", "Seventeen reasons I'm sick of", "Everyone should use", "Vote for", "I'm in love with", "Do you know what saved us money?", "Surprisingly enough, we're using", "Why do people still use", "Never again will I use", "Only idiots use", "I will quit if we use", "Ha! They use", "Open source", "5 Secrets of", "Who is Responsible for"]
  LAST_PART = ["Yum", "Maven", "Ant", "MySQL", "Drizzle", "Redhat", "Ubuntu", "Netscape", "Chrome", "Firefox", "Agile", "Waterfall", "edlin", "vi", "emacs", "GNU/Linux", "Java", "Ruby", "Python", "R", "SAS", "SPSS", "Karate", "Jujitsu", "Tae Kwon Do", "Spyware", "Shareware", "Open Core", "Linux", "Windows", "Cellphones", "for loops", "HTML", "Javascript", "Compilers", "Bankstreet Writer", "Wordperfect", "ATG", "Weblogic", "Websphere", "Drupal", "Wordpress", "Magento", "Ruby on Rails", "GWT", "Google", "Bing", "Cylons"]

  def self.first()
    FIRST_PART[ rand( FIRST_PART.size ) ]
  end

  def self.last()
    LAST_PART[ rand( LAST_PART.size ) ]
  end

  def self.full()
    self.first + " " + self.last
  end

end

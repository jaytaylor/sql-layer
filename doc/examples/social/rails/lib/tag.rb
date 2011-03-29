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

module Tag

  TAG = ["Yum", "Maven", "Ant", "MySQL", "Drizzle", "Redhat", "Ubuntu", "Netscape", "Chrome", "Firefox", "Agile", "Waterfall", "edlin", "vi", "emacs", "GNU/Linux", "Java", "Ruby", "Python", "R", "SAS", "SPSS", "Karate", "Jujitsu", "Tae Kwon Do", "Spyware", "Shareware", "Open Core", "Linux", "Windows", "Cellphones", "for loops", "HTML", "Javascript", "Compilers", "Bankstreet Writer", "Wordperfect", "ATG", "Weblogic", "Websphere", "Drupal", "Wordpress", "Magento", "Ruby on Rails", "GWT", "Google", "Bing", "Cylons", "news", "entertainment", "politics", "world", "science", "engineering", "technology", "development", "finance", "banking", "medical", "IT", "management", "school", "education", "budget", "protest", "fashion", "vacation", "recreation", "cms"]

  def self.tag()
    TAG[ rand( TAG.size ) ].downcase
  end

end

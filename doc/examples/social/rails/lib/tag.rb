module Tag

  TAG = ["Yum", "Maven", "Ant", "MySQL", "Drizzle", "Redhat", "Ubuntu", "Netscape", "Chrome", "Firefox", "Agile", "Waterfall", "edlin", "vi", "emacs", "GNU/Linux", "Java", "Ruby", "Python", "R", "SAS", "SPSS", "Karate", "Jujitsu", "Tae Kwon Do", "Spyware", "Shareware", "Open Core", "Linux", "Windows", "Cellphones", "for loops", "HTML", "Javascript", "Compilers", "Bankstreet Writer", "Wordperfect", "ATG", "Weblogic", "Websphere", "Drupal", "Wordpress", "Magento", "Ruby on Rails", "GWT", "Google", "Bing", "Cylons", "news", "entertainment", "politics", "world", "science", "engineering", "technology", "development", "finance", "banking", "medical", "IT", "management", "school", "education", "budget", "protest", "fashion", "vacation", "recreation", "cms"]

  def self.tag()
    TAG[ rand( TAG.size ) ].downcase
  end

end

module Name

  FIRST_NAMES = ["Al", "Bert", "Chuck", "Dennis", "Edgar", "Felix", "George", "Irwin", "Jack", "Kyle", "Luke", "Mel", "Ned", "Opie", "Pete", "Quark", "Randy", "Steve", "Ted", "Uwe", "Vinny", "Ward", "Xavier", "Yuval", "Zed", "Alfonso", "Bertice", "Cory", "Ducky", "Ewe", "Florian", "Gerrit", "Io", "Jules", "Korwyn", "Link", "Muxberry", "Newton", "Oslo", "Peoria", "Ranald", "Spaulding", "Tim", "Ulrich", "Vance", "Waldo", "Xen", "Yuri", "Zelda"]
  LAST_NAMES = ["Axeman", "Beckley", "Ciara", "Delgado", "Ette", "Fenwick", "Gardner", "Highland", "Icke", "Julian", "Kaufmann", "Lourdes", "Marvin", "Notworth", "O'Brien", "Poulenc", "Quixote", "Rock", "Sandler", "Tufte", "Urlacher", "Vechionne", "Waxman", "Xi", "Yu", "Zorro", "Artman", "Batman", "Cartman", "Darton", "Eaton", "Felton", "Garrick", "Helios", "Iona", "Jorpe", "Klupe", "Loral", "Mordor", "Nalnick", "Otte", "Purdue", "Ricky", "Sorenson", "Thiers"]

  def self.first()
    FIRST_NAMES[ rand( FIRST_NAMES.size ) ]
  end

  def self.last()
    LAST_NAMES[ rand( LAST_NAMES.size ) ]
  end

  def self.full()
    self.first + " " + self.last
  end

end

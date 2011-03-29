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

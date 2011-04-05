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

module Power
  module Methods
    # Power law (log tail) distribution
    # Copyright(C) 2010 Salvatore Sanfilippo
    # this code is under the public domain
    
    # min and max are both inclusive
    # n is the distribution power: the higher, the more biased
    def powerlaw(min,max,n)
      max += 1
      pl = ((max**(n+1) - min**(n+1))*rand() + min**(n+1))**(1.0/(n+1))
      (max-1-pl.to_i)+min
    end
  end
end

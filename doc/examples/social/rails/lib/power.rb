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

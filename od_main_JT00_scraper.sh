#!/bin/bash

declare -a arr=(al ak az ar ca co ct de dc fl ga hi id il in ia ks ky la me md ma mi mn ms mo mt ne nv nh nj nm ny nc nd oh ok or pa ri sc sd tn tx ut vt va wa wv wi wy)
for j in ${arr[@]}
do
for i in 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011
do
beginning="http://lehd.did.census.gov/onthemap/LODES7/$j/od/$j"
end="_od_main_JT00_$i.csv.gz"
wget $beginning$end | gzip -d >> states.csv
done
done

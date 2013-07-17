#!/bin/bash
for i in `seq 2002 2011`;
do
  wget http://lehd.did.census.gov/onthemap/LODES7/al/od/al_od_main_JT00_ + | gzip -d
  

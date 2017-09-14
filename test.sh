#!/bin/sh

TIME=`expr $(date +%s) \* 1000 - 60000`
SESSION="$TIME"

while read -r LINE || [[ -n "$LINE" ]]; do
  LINE=`sed "s/__TIME__/$TIME/g" <<< "$LINE"`
  LINE=`sed "s/__SESSION__/$SESSION/g" <<< "$LINE"`
  ./build/bin/python amplitude.py "$LINE"
  TIME=`expr $TIME + 1000`
done < ./fixtures.txt


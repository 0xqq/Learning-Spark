#!/usr/bin/env sh
rm /tmp/logs/logdata
touch /tmp/logs/logdata
tail -f /tmp/logs/logdata | nc -lk 9999 &
TAIL_NC_PID=$!
cat ./fake_logs/log1.log >> /tmp/logs/logdata
sleep 5
cat ./fake_logs/log2.log >> /tmp/logs/logdata
sleep 1
cat ./fake_logs/log1.log >> /tmp/logs/logdata
sleep 2
cat ./fake_logs/log1.log >> /tmp/logs/logdata
sleep 3
sleep 20
kill $TAIL_NC_PID

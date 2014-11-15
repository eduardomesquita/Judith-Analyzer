if [ -z "$PORT" ]; then
    echo "Need to set env var PORT, i will use 6060 by default"
else
    echo "Service is up on port $PORT ..."
fi

sudo -E forever start -a -l /var/log/judith.log -o /var/log/judith.out -e /var/log/judith.err --watchIgnore "*.log" -w bin/www

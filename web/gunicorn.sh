#!/bin/bash
set -e
PROJECT=/home/evz/projects/census-communities-usa
VENV=$PROJECT/venv
LOGFILE=$VENV/run/gunicorn.log
LOGDIR=$(dirname $LOGFILE)
NUM_WORKERS=3
# user/group to run as
source /home/evz/.zshenv
USER=evz
GROUP=evz
cd $PROJECT/web
source $VENV/bin/activate
test -d $LOGDIR || mkdir -p $LOGDIR
exec $VENV/bin/gunicorn -w $NUM_WORKERS --bind 127.0.0.1:7777 --user=$USER --group=$GROUP --log-level=debug --log-file=$LOGFILE 2>>$LOGFILE app:app -t 60

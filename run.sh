#!/bin/sh

SHARE_DIR=/share/kocom

mkdir -p $SHARE_DIR

if [ ! -f $SHARE_DIR/kocom.conf ]; then
    cp /kocom.conf $SHARE_DIR/kocom.conf
fi

if [ ! -f $SHARE_DIR/kocom.py ]; then
    cp /kocom.py $SHARE_DIR/kocom.py
fi

echo "[Info] Run Kocom Wallpad with RS485!"
cd $SHARE_DIR
python3 $SHARE_DIR/kocom.py

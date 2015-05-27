#!/bin/bash
_now=$(date +"%Y%m%d")
_file="/home/curhxerp/eve-demo/data/inventory.$_now.hk.inc.latest.xml"
scp "$_file" efashion@192.168.1.56:/home/efashion/efashion/scripts/data


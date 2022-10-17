#!/usr/bin/bash

sh hive2txt.sh
python3 ner_check.py virtualid_20220623_tmp.txt virtualid_20220623.txt
kdd-cup-2013-track1
===================
KDD CUP 2013 - Track 1 - 2nd place model

# License
Copyright [2013] [Dmitry Efimov, Lucas Silva, Ben Solecki ]
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

# How to use it
1. The hardware / OS platform you used
Windows 7 Professional x64 or Ubuntu (tested on 12.04)

2. Any necessary 3rd-party software (+ installation steps)

R 2.15.3 (http://www.r-project.org/)
Packages: rjson, RPostgreSQL, data.table, hexbin, gbm, tm, parallel, doSNOW, foreach, Metrics, cvTools, lme4, rlecuyer

Python 2.6 (http://www.python.org/download/releases/2.6/) 
Packages: csv, json, os, pickle, psycopg2, textmining, re

# How to train models and make predictions on a new test set

The file list with description
1) SETTINGS.json
File of paths and login information for Postgree SQL.

2) TDM_TitleKeywords.py
File to generate table Keywords in Postgree SQL.

3) feature_engineering.R
Main files of feature engineering

4) data.build.R
File with additional feature engineering

5) prediction.R
Main file for prediction

6) fn.base.R
File with helper functions

7) Track2_Dup.csv
File of author duplicates from Track 2

8) Track2_Dup_Greedy.csv
File of greedy author duplicates from Track 2

# To calculate model

1) Set paths and login information for Postgree SQL in SETTINGS.json.
2) Add all the inputs csvs to the directory data.
3) Run TDM_TitleKeywords.py in Python 2.6.
4) Set working directory in R to the directory contained R files.
5) Run feature_engineering.R.
6) Run prediction.R.
7) The file prediction.csv contains final prediction for the Test set.


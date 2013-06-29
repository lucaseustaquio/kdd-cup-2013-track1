#Copyright [2013] [Lucas Silva, Dmitry Efimov, Ben Solecki ]

#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#http://www.apache.org/licenses/LICENSE-2.0
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.

source("fn.base.R")

##############################################################
## load data
##############################################################
library("data.table")

tic()
cat("Loading csv data... ")

settings <- fromJSON(file="SETTINGS.json")
dbname <- unlist(regmatches(settings$postgres_conn_string,gregexpr("dbname='[A-Za-z0-9]*'",settings$postgres_conn_string)))
dbname <- unlist(regmatches(dbname,gregexpr("'[A-Za-z0-9]*'",dbname)))
dbname <- gsub("[^A-Za-z]","",dbname)

user <- unlist(regmatches(settings$postgres_conn_string,gregexpr("user='[A-Za-z0-9]*'",settings$postgres_conn_string)))
user <- unlist(regmatches(user,gregexpr("'[A-Za-z0-9]*'",user)))
user <- gsub("[^A-Za-z]","",user)

password <- unlist(regmatches(settings$postgres_conn_string,gregexpr("password='[A-Za-z0-9_]*'",settings$postgres_conn_string)))
password <- unlist(regmatches(password,gregexpr("'[A-Za-z0-9_]*'",password)))
password <- gsub("[^A-Za-z0-9_]","",password)

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = dbname, user=user, password=password)

#data.author <- fn.read.input.csv("Author.csv", key = "id")
data.author <- data.table(dbGetQuery(con, statement = "SELECT * FROM Author"),key=c("id"))
setnames(data.author, c("authorid", "a_name", "a_affiliation"))

#data.conference <- fn.read.input.csv("Conference.csv", key = "id")
data.conference <- data.table(dbGetQuery(con, statement = "SELECT * FROM Conference"),key=c("id"))
setnames(data.conference, c("conferenceid", "conf_shortname", 
                        "conf_fullname", "conf_homepage"))
data.conference$conf_hpdomain <- 
  gsub("^http://([^/]+).*$", "\\1", data.conference$conf_homepage)

#data.journal <- fn.read.input.csv("Journal.csv", key = "id")
data.journal <- data.table(dbGetQuery(con, statement = "SELECT * FROM Journal"),key=c("id"))
setnames(data.journal, c("journalid", "j_shortname", 
                        "j_fullname", "j_homepage"))
data.journal$j_hpdomain <- 
  gsub("^http://([^/]+).*$", "\\1", data.journal$j_homepage)

data.train.ids <- fn.read.input.csv("TrainInitial.csv")
data.train.confirm <- data.train.ids[
  ,list(paperid = as.integer(unlist(strsplit(confirmedpaperids, split = "\\s+"))),
        confirmed = 1),
  by="authorid"]
data.train.delete <- data.train.ids[
  ,list(paperid = as.integer(unlist(strsplit(deletedpaperids, split = "\\s+"))),
        confirmed = 0),
  by="authorid"]
data.train.ids <- rbind(data.train.confirm,
                        data.train.delete)

data.train.ids.feat <- fn.build.load.feat(data.train.ids)

data.train.ids.map <- data.table(data.train.ids)
data.train.ids.map <- data.train.ids.map[
  data.train.ids.map$confirmed == 1,]
setkeyv(data.train.ids.map, c("authorid", "paperid"))

data.train.ids <- unique(data.train.ids)
setkeyv(data.train.ids, c("authorid", "paperid"))
data.train.ids <- unique(data.train.ids)

data.valid.ids.map <- fn.read.input.csv("ValidInitialSolution.csv")
data.valid.ids.map <- data.valid.ids.map[
  ,list(paperid = as.integer(unlist(strsplit(paperids, split = "\\s+"))),
        confirmed = 1),
  by="authorid"]
setkeyv(data.valid.ids.map, c("authorid", "paperid"))

data.valid.ids <- fn.read.input.csv("ValidInitial.csv")
data.valid.ids <- data.valid.ids[
  ,list(
    paperid = as.integer(unlist(strsplit(paperids, split = "\\s+")))),
  by="authorid"]

data.valid.ids.feat <- fn.build.load.feat(data.valid.ids)

data.valid.ids <- unique(data.valid.ids)
setkeyv(data.valid.ids, c("authorid", "paperid"))

data.train.old.ids <- data.train.ids
data.train.old.ids.map <- data.train.ids.map
data.train.old.ids.feat <- data.train.ids.feat

data.valid.ids <- fn.join(data.valid.ids, unique(data.valid.ids.map))
data.valid.ids$confirmed[is.na(data.valid.ids$confirmed)] <- 0
data.train.ids <- fn.rbind(data.train.ids, 
                           data.valid.ids)
data.train.ids.map <- fn.rbind(data.train.ids.map, 
                               data.valid.ids.map)
data.train.ids.feat <- fn.rbind(data.train.ids.feat, 
                                data.valid.ids.feat)

data.test.ids <- fn.read.input.csv("TestPaper.csv")
data.test.ids.feat <- fn.build.load.feat(data.test.ids)

data.test.ids <- unique(data.test.ids)
setkeyv(data.test.ids, c("authorid", "paperid"))

#data.paper <- fn.read.input.csv("Paper.csv", key = "id")
data.paper <- data.table(dbGetQuery(con, statement = "SELECT * FROM Paper"),key="id")
setnames(data.paper, c(
  "paperid", "p_title", "p_year", "conferenceid", "journalid", "p_keyword"))


#data.paper.author <- fn.read.input.csv("PaperAuthor.csv", 
#                                       key = c("paperid","authorid"))
#data.paper.author <- data.frame(data.paper.author)

data.paper.author <- data.frame(dbGetQuery(con, statement = "SELECT * FROM PaperAuthor"))
setnames(data.paper.author, c(
  "paperid", "authorid", "pa_name", "pa_affiliation"))
data.paper.author <- data.paper.author[,c(
  "authorid", "paperid", "pa_name", "pa_affiliation")]
data.paper.author <- data.table(data.paper.author, 
                                key = c("authorid", "paperid"))

data.paper.author.clean <- fn.clean.paper.author(data.paper.author)

fn.save.data("data.author")
fn.save.data("data.conference")
fn.save.data("data.journal")
fn.save.data("data.paper")
fn.save.data("data.paper.author")
fn.save.data("data.paper.author.clean")

fn.save.data("data.train.old.ids")
fn.save.data("data.train.old.ids.map")
fn.save.data("data.train.old.ids.feat")

fn.save.data("data.train.ids")
fn.save.data("data.train.ids.map")
fn.save.data("data.train.ids.feat")

fn.save.data("data.valid.ids")
fn.save.data("data.valid.ids.map")
fn.save.data("data.valid.ids.feat")

fn.save.data("data.test.ids")
fn.save.data("data.test.ids.feat")

cat("done \n")
toc()

##############################################################
## loading author duplicates
##############################################################
source("fn.base.R")
library("data.table")
library("stringr")

tic()
cat("Building duplication stats... ")

data.author.dup <- fn.read.input.csv("Track2_Dup.csv")
data.author.dup <- unique(data.author.dup[
  ,list(dupauthorids = as.integer(unlist(strsplit(duplicateauthorids, split = "\\s+")))),
  by="authorid"])
data.author.dup <- data.author.dup[
  data.author.dup$authorid != data.author.dup$dupauthorids,]

data.author.dup.greedy <- fn.read.input.csv("Track2_Dup_Greedy.csv")
data.author.dup.greedy <- unique(data.author.dup.greedy[
  ,list(dupauthorids = as.integer(unlist(strsplit(duplicateauthorids, split = "\\s+")))),
  by="authorid"])
data.author.dup.greedy <- data.author.dup.greedy[
  data.author.dup.greedy$authorid != data.author.dup.greedy$dupauthorids,]

data.author.dup.fake <- data.table(data.author.dup,
                                   key = c("authorid", "dupauthorids"))
data.author.dup.fake$dummy <- 1

data.author.dup.fake <- fn.join(data.author.dup.greedy, data.author.dup.fake)
data.author.dup.fake <- data.author.dup.fake[is.na(data.author.dup.fake$dummy),]
data.author.dup.fake$dummy <- NULL

setkeyv(data.author.dup, "authorid")
setkeyv(data.author.dup.fake, "authorid")

fn.save.data("data.author.dup")
fn.save.data("data.author.dup.fake")

cat("done \n")
toc()

##############################################################
## cross validation indexes
##############################################################

source("fn.base.R")
fn.load.data("data.train.ids")

tic()
cat("Building lucas cv... ")


data.cv.folds <- fn.build.cv(data.train.ids)
# Instance CV distribution: 
# 
#     1     2     3     4     5 
# 64648 60047 70657 65358 58566 
# Author CV distribution: 
# 
#    1    2    3    4    5 
# 1047 1047 1047 1047 1047 
fn.save.data("data.cv.folds")

data.cv.folds10 <- fn.build.cv(data.train.ids, K=10)
# Instance CV distribution: 
# 
#     1     2     3     4     5     6     7     8     9    10 
# 27801 24265 21286 22020 20775 21502 22929 21190 25238 24327 
# Author CV distribution: 
# 
#   1   2   3   4   5   6   7   8   9  10 
# 374 374 374 374 374 374 374 374 374 373
fn.save.data("data.cv.folds10")

cat("done \n")
toc()

##############################################################
## build likelihood features - how i think it should be
##############################################################
source("fn.base.R")
library("data.table")

tic()
cat("creating likelihood features... ")

fn.load.data("data.author")
fn.load.data("data.conference")
fn.load.data("data.journal")
fn.load.data("data.paper")
fn.load.data("data.train.ids")
fn.load.data("data.valid.ids")
fn.load.data("data.test.ids")
fn.load.data("data.train.ids.map")
fn.load.data("data.train.old.ids.map")
fn.load.data("data.valid.ids.map")
fn.load.data("data.paper.author.clean")
data.paper.author <- data.paper.author.clean

# 451490

data.cv.folds.like <- fn.build.cv(data.train.ids, K = 10)
data.cv.folds.like.2 <- fn.build.cv(data.train.ids, K = 20)

col.key <- c("authorid", "paperid")
data.feat.likelihood.dt <- data.table(
  rbind(data.train.ids, data.frame(data.test.ids, confirmed = NA)),
  key = col.key)

data.feat.likelihood.stats.dt <- fn.join(data.feat.likelihood.dt, data.paper)
data.feat.likelihood.stats.dt <- fn.join(data.feat.likelihood.stats.dt, data.conference)
data.feat.likelihood.stats.dt <- fn.join(data.feat.likelihood.stats.dt, data.journal)
data.feat.likelihood.stats.dt <- fn.join(data.feat.likelihood.stats.dt, data.author)
data.feat.likelihood.stats.dt <- fn.join(data.feat.likelihood.stats.dt, data.paper.author)

if (!is.null(data.feat.likelihood.dt$confirmed)) {
  data.feat.likelihood.dt$confirmed  <- NULL
}

# fn.load.data("data.feat.likelihood.dt")

# data.feat.likelihood.stats.dt$dummy <- 1
# feats.dummy <- fn.build.confirm.stats(data.cv.folds.like, 
#                                     data.feat.likelihood.stats.dt, "dummy")
# feats.dummy <- fn.log.likelihood(feats.dummy)
# fn.log.likelihood.print(data.train.ids.map, feats.dummy)
# #   size       map
# # 1 5235 0.6774787
# fn.log.likelihood.print(data.train.old.ids.map, feats.dummy)
# #   size       map
# # 1 3739 0.6747384
# fn.log.likelihood.print(data.valid.ids.map, feats.dummy)
# #   size       map
# # 1 1496 0.6843276

feats.jid <- fn.build.confirm.stats(
  data.cv.folds.like, 
  data.feat.likelihood.stats.dt, "journalid")
feats.jid.ll <- fn.log.likelihood(feats.jid)
fn.log.likelihood.print(data.train.ids.map, feats.jid.ll)
#   size       map
# 1 5235 0.7632114
fn.log.likelihood.print(data.train.old.ids.map, feats.jid.ll)
#   size       map
# 1 3739 0.7638525
fn.log.likelihood.print(data.valid.ids.map, feats.jid.ll)
#   size       map
# 1 1496 0.7616091
data.feat.likelihood.dt <- fn.join2(data.feat.likelihood.dt, feats.jid.ll)

feats.confid <- fn.build.confirm.stats(
  data.cv.folds.like, 
  data.feat.likelihood.stats.dt, "conferenceid")
feats.confid.ll <- fn.log.likelihood(feats.confid)
fn.log.likelihood.print(data.train.ids.map, feats.confid.ll)
#   size       map
# 1 5235 0.7358547
fn.log.likelihood.print(data.train.old.ids.map, feats.confid.ll)
#   size       map
# 1 3739 0.7349231
fn.log.likelihood.print(data.valid.ids.map, feats.confid.ll)
#   size       map
# 1 1496 0.7381832
data.feat.likelihood.dt <- fn.join2(data.feat.likelihood.dt, feats.confid.ll)

feats.paperid <- fn.build.confirm.stats(
  data.cv.folds.like, 
  data.feat.likelihood.stats.dt, "paperid")
feats.paperid.ll <- fn.log.likelihood(feats.paperid)
fn.log.likelihood.print(data.train.ids.map, feats.paperid.ll)
#   size       map
# 1 5235 0.7061384
fn.log.likelihood.print(data.train.old.ids.map, feats.paperid.ll)
#   size       map
# 1 3739 0.7033864
fn.log.likelihood.print(data.valid.ids.map, feats.paperid.ll)
#   size       map
# 1 1496 0.7130165
data.feat.likelihood.dt <- fn.join2(data.feat.likelihood.dt, feats.paperid.ll)

library("stringr")
data.feat.likelihood.stats.dt$p_title <- 
  str_trim(tolower(data.feat.likelihood.stats.dt$p_title))
data.feat.likelihood.stats.dt$p_title <- 
  gsub("[^a-z]", " ", data.feat.likelihood.stats.dt$p_title)
data.feat.likelihood.stats.dt$p_title <- 
  gsub("\\s+", " ", data.feat.likelihood.stats.dt$p_title)
data.feat.likelihood.stats.dt$p_title <- 
  str_trim(data.feat.likelihood.stats.dt$p_title)
small.title <- nchar(data.feat.likelihood.stats.dt$p_title) < 10
data.feat.likelihood.stats.dt$p_title[small.title] <-
  paste0("_small_", which(small.title))

feats.title <- fn.build.confirm.stats(
  data.cv.folds.like, 
  data.feat.likelihood.stats.dt, "p_title")
feats.title.ll <- fn.log.likelihood(feats.title)
fn.log.likelihood.print(data.train.ids.map, feats.title.ll)
#   size       map
# 1 5235 0.7044105
fn.log.likelihood.print(data.train.old.ids.map, feats.title.ll)
#   size       map
# 1 3739 0.7019751
fn.log.likelihood.print(data.valid.ids.map, feats.title.ll)
#   size       map
# 1 1496 0.7104976
data.feat.likelihood.dt <- fn.join2(data.feat.likelihood.dt, feats.title.ll)

feats.year <- fn.build.confirm.stats(
  data.cv.folds.like.2, 
  data.feat.likelihood.stats.dt, "p_year")
feats.year.ll <- fn.log.likelihood(feats.year)
fn.log.likelihood.print(data.train.ids.map, feats.year.ll)
#   size       map
# 1 5235 0.7540992
fn.log.likelihood.print(data.train.old.ids.map, feats.year.ll)
#   size       map
# 1 3739 0.7525272
fn.log.likelihood.print(data.valid.ids.map, feats.year.ll)
#   size       map
# 1 1496 0.7580283
data.feat.likelihood.dt <- fn.join2(data.feat.likelihood.dt, feats.year.ll)

# feats.paff <- fn.build.confirm.stats(
#   data.cv.folds.like, 
#   data.feat.likelihood.stats.dt, "pa_affiliation")
# feats.paff.ll <- fn.log.likelihood(feats.paff)
# fn.log.likelihood.print(data.train.ids.map, feats.paff.ll)
# #   size       map
# # 1 5235 0.7019347
# fn.log.likelihood.print(data.train.old.ids.map, feats.paff.ll)
# #   size       map
# # 1 3739 0.6989904
# fn.log.likelihood.print(data.valid.ids.map, feats.paff.ll)
# #   size       map
# # 1 1496 0.7092933
# data.feat.likelihood.dt <- fn.join2(data.feat.likelihood.dt, 
#                                     feats.paff.ll)

fn.save.data("data.feat.likelihood.dt")

cat("done \n")
toc()

##############################################################
## source related
##############################################################
source("fn.base.R")
library("data.table")

tic()
cat("creating source related features... ")

fn.load.data("data.cv.folds")
fn.load.data("data.author")
fn.load.data("data.conference")
fn.load.data("data.journal")
fn.load.data("data.paper")
fn.load.data("data.train.ids")
fn.load.data("data.test.ids")
fn.load.data("data.train.ids.feat")
fn.load.data("data.test.ids.feat")
fn.load.data("data.paper.author.clean")
fn.load.data("data.paper.author")
fn.load.data("data.author.dup.fake")
fn.load.data("data.author.dup")
# fn.load.data("data.author.dup.greedy")

col.key <- c("authorid", "paperid")

data.ids.feat <- rbind(data.train.ids.feat,
                       data.test.ids.feat)
setkeyv(data.ids.feat, col.key)

library("stringr")
data.paper.rep <- data.table(data.paper)
data.paper.rep$p_title <-str_trim(tolower(data.paper.rep$p_title))
data.paper.rep$p_title <- gsub("[^a-z0-9]", " ", data.paper.rep$p_title)
data.paper.rep$p_title <- gsub("\\s+", " ", data.paper.rep$p_title)

small.title <- nchar(data.paper.rep$p_title) < 5
data.paper.rep$p_title[small.title] <- 
  paste("empty", data.paper.rep$paperid[small.title],
        which(small.title))


data.paper.rep <- data.paper.rep[
  , list(
    paperid = paperid,
    apf_dupsourcenomatch = length(unique(paperid))-1,
    pf_paperrepid = max(paperid, na.rm = T)
  ), by=c("p_title")]
data.paper.rep$p_title <- NULL
setkeyv(data.paper.rep, "paperid")
data.paper.rep <- fn.join(data.paper.author.clean, data.paper.rep)
data.paper.rep <- data.paper.rep[
  ,list(
    authorid = unique(authorid),
    apf_dupsourcenomatch = unique(apf_dupsourcenomatch),
    pf_paperrepid = unique(pf_paperrepid)),
  ,by="paperid"]
data.paper.rep <- data.paper.rep[
  ,c("authorid", "paperid", 
     "pf_paperrepid", "apf_dupsourcenomatch"),with = F]

data.paper.repid <- data.paper.rep[
  ,list(pf_paperrepid = unique(pf_paperrepid)), by="paperid"]
setkeyv(data.paper.repid, "paperid")

data.author.dup.fake.id <- data.author.dup.fake[
  ,list(authorfakedupid = min(authorid, dupauthorids)), by="authorid"]
setkeyv(data.author.dup.fake.id, "authorid")

data.paper.rep <- fn.join(data.paper.rep, data.author.dup.fake.id)
data.paper.fake.rep <- data.paper.rep[
  !is.na(data.paper.rep$authorfakedupid),]
data.paper.fake.rep <- data.paper.fake.rep[,
                                           list(apf_fakesource = length(unique(authorid))-1),
                                           by=c("pf_paperrepid", "authorfakedupid")]
setkeyv(data.paper.fake.rep, c("pf_paperrepid", "authorfakedupid"))


data.paper.rep$authorfakedupid[is.na(data.paper.rep$authorfakedupid)] <- -1
data.paper.rep <- fn.join(data.paper.rep, data.paper.fake.rep)

no.fakedup <- is.na(data.paper.rep$apf_fakesource)
data.paper.rep$apf_fakesource[no.fakedup] <- -1
data.paper.rep$authorfakedupid <- NULL
data.paper.rep$apf_dupsourcenomatch[!no.fakedup] <- 
  data.paper.rep$apf_dupsourcenomatch[!no.fakedup] -
  data.paper.rep$apf_fakesource[!no.fakedup]

data.author.dup.id <- data.author.dup[
  ,list(authorupid = min(authorid, dupauthorids)), by="authorid"]
setkeyv(data.author.dup.id, "authorid")

data.paper.rep <- fn.join(data.paper.rep, data.author.dup.id)
data.paper.dup.rep <- data.paper.rep[
  !is.na(data.paper.rep$authorupid),]
data.paper.dup.rep <- data.paper.dup.rep[,
                                         list(apf_dupsource = length(unique(authorid))-1),
                                         by=c("pf_paperrepid", "authorupid")]
setkeyv(data.paper.dup.rep, c("pf_paperrepid", "authorupid"))

data.paper.rep$authorupid[is.na(data.paper.rep$authorupid)] <- -1
data.paper.rep <- fn.join(data.paper.rep, data.paper.dup.rep)

no.dup <- is.na(data.paper.rep$apf_dupsource)
data.paper.rep$apf_dupsource[no.dup] <- -1
data.paper.rep$authorupid <- NULL
data.paper.rep$apf_dupsourcenomatch[!no.dup] <- 
  data.paper.rep$apf_dupsourcenomatch[!no.dup] -
  data.paper.rep$apf_dupsource[!no.dup]

data.paper.author.src.ratio <- data.paper.author[
  ,list(apf_sourceratio = length(authorid)/length(unique(authorid))),
  by="paperid"]
setkeyv(data.paper.author.src.ratio, "paperid")

data.paper.rep <- fn.join(data.paper.rep, data.paper.author.src.ratio)

data.paper.author.stats <- data.paper.author.clean[
  ,list(
    pf_authorall = paste(c(" ", sort(unique(authorid)), " "), collapse = " "),
    #     pf_authorpattern = paste(c("\\s", sort(unique(authorid)), "\\s"), collapse = ""),
    pf_authorcount = length(unique(authorid))),
  by="paperid"]
data.paper.author.stats <- fn.join(data.paper.author.stats, data.paper.repid)

data.paper.author.stats.same.set <- data.paper.author.stats[
  ,list(pf_countsameset = length(unique(pf_paperrepid))),
  by="pf_authorall"]
setkeyv(data.paper.author.stats.same.set, "pf_authorall")

data.paper.author.stats <- fn.join(data.paper.author.stats,
                                   data.paper.author.stats.same.set)

data.paper.author.stats$pf_countsameset[
  data.paper.author.stats$pf_authorcount == 1] <- 0

data.paper.author.stats$pf_authorall <- NULL
data.paper.author.stats$pf_authorcount <- NULL
data.paper.author.stats$pf_paperrepid <- NULL
setkeyv(data.paper.author.stats, "paperid")

data.paper.rep <- fn.join(data.paper.rep, data.paper.author.stats)

data.paper.rep <- fn.join(data.paper.rep, data.ids.feat)

data.paper.rep$pf_paperrepid <- NULL
data.paper.rep$apf_dupsourcenomatch <- NULL
# data.paper.rep$apf_dupsource <- NULL
# data.paper.rep$apf_fakesource <- NULL

setkeyv(data.paper.rep, c("authorid", "paperid"))

col.key <- c("authorid", "paperid")
paper.rep.dt <- unique(rbind(data.train.ids[,col.key,with=F],
                             data.test.ids[,col.key,with=F]))

data.dup.source.dt <- fn.join(paper.rep.dt, data.paper.rep)
setkeyv(data.dup.source.dt, c("authorid", "paperid"))

# fn.load.data("data.dup.source.dt")

fn.save.data("data.dup.source.dt")

cat("done... \n")
toc()

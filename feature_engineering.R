#Copyright [2013] [Dmitry Efimov, Lucas Silva, Ben Solecki ]

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
source("data.build.R")

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

data.author <- data.table(dbGetQuery(con, statement = "SELECT * FROM Author"),key=c("id"))
setnames(data.author, c("authorid", "a_name", "a_affiliation"))
data.author <- data.author[,list(authorid,a_name,a_name_cleaned=cleanTextField(a_name),a_affiliation,a_affiliation_cleaned = cleanTextField(a_affiliation))]

data.sameauthors <- data.table(read.csv(file="data/Track2_Dup.csv"))
setnames(data.sameauthors,c("authorid","duplicated_authorid"))
data.sameauthors[,duplicated_authorid:=as.character(duplicated_authorid)]
data.sameauthors <- data.sameauthors[,list(new_authorid = max(as.numeric(unlist(strsplit(duplicated_authorid," "))))),by="authorid"]
data.author <- merge(data.author,data.sameauthors,by="authorid",all.x=TRUE)

data.conference <- data.table(dbGetQuery(con, statement = "SELECT * FROM Conference"),key=c("id"))
setnames(data.conference, c("conferenceid", "conf_shortname", "conf_fullname", "conf_homepage"))
data.conference$conf_hpdomain <- gsub("^http://([^/]+).*$", "\\1", data.conference$conf_homepage)
data.conference <- data.conference[,list(conferenceid,conf_shortname,conf_fullname = cleanTextField(conf_fullname),conf_homepage = cleanWebField(conf_homepage))]
data.journal <- data.table(dbGetQuery(con, statement = "SELECT * FROM Journal"),key=c("id"))
setnames(data.journal, c("journalid", "j_shortname", "j_fullname", "j_homepage"))
data.journal$j_hpdomain <- gsub("^http://([^/]+).*$", "\\1", data.journal$j_homepage)
data.journal <- data.journal[,list(journalid,j_shortname,j_fullname = cleanTextField(j_fullname),j_homepage = cleanWebField(j_homepage))]

#data.train.confirm <- unique(data.table(dbGetQuery(con, statement = "SELECT * FROM TrainConfirmed"),key=c("authorid","paperid")))
#data.train.confirm <- data.train.confirm[,list(authorid,paperid,target=1)]
#data.train.delete <- unique(data.table(dbGetQuery(con, statement = "SELECT * FROM TrainDeleted"),key=c("authorid","paperid")))
#data.train.delete <- data.train.delete[,list(authorid,paperid,target=0)]
#data.train.ids <- unique(rbind(data.train.confirm, data.train.delete))
#data.test.ids <- unique(data.table(dbGetQuery(con, statement = "SELECT * FROM ValidPaper"),key=c("authorid","paperid")))
#data.test.ids <- data.test.ids[,list(authorid,paperid,target=-1)]
#data.test.ids <- unique(data.test.ids)
#data.feats <- rbind(data.train.ids,data.test.ids)

source("combine_train_valid.R")
setnames(data.feats,c("authorid","paperid","target"))
setkey(data.feats,authorid,paperid)
author.ids <- unique(data.feats[,authorid])
paper.ids <- unique(data.feats[,paperid])

data.paper <- data.table(dbGetQuery(con, statement = "SELECT * FROM Paper"))
setnames(data.paper, c("paperid", "title", "year", "conferenceid", "journalid", "keyword"))
data.paper[,year:=ifelse((year<1600 | year>2013),-1,year)]
data.paper <- data.paper[,list(paperid,conferenceid,journalid,year,keyword,new_paperid=max(paperid)),by=c("title")]
data.paper <- data.paper[,list(paperid,title,keyword,year,conferenceid,journalid,
                               new_paperid = ifelse(nchar(title)>20,new_paperid,paperid))]

data.paper.freq <- data.paper[,list(freq=length(paperid)),by="new_paperid"]
data.paper <- merge(data.paper,data.paper.freq,by="new_paperid")

setkey(data.paper,new_paperid)
data.paper.extraclean <- unique(data.paper)

data.paper.author <- data.frame(dbGetQuery(con, statement = "SELECT * FROM PaperAuthor"))
setnames(data.paper.author, c("paperid", "authorid", "pa_name", "pa_affiliation"))
data.paper.author <- data.paper.author[,c("authorid", "paperid", "pa_name", "pa_affiliation")]
data.paper.author <- data.table(data.paper.author, key = c("authorid", "paperid"))
data.paper.author.clean <- data.paper.author[,list(
  pa_name = chooseTextField(pa_name),
  pa_affiliation = chooseTextField(pa_affiliation)
),by=c("authorid","paperid")]

#decreasing PaperAuthor table
setkey(data.paper.author.clean,authorid)
data.feats.big <- merge(data.paper.author.clean[J(author.ids)][,list(authorid,paperid)],data.feats[,list(authorid,paperid,target)],by=c("authorid","paperid"),all.x=TRUE,allow.cartesian=TRUE)
papers.removed <- unique(data.feats.big[is.na(target)][,paperid])
papers.removed <- setdiff(papers.removed,paper.ids)
papers.left <- setdiff(data.paper.author.clean[,paperid],papers.removed)
setkey(data.paper.author,paperid)
data.paper.author <- data.paper.author[J(papers.left)]
setkey(data.paper.author.clean,paperid)
data.paper.author.clean <- data.paper.author.clean[J(papers.left)]

data.paper.author <- data.paper.author[,list(authorid,paperid,pa_name,pa_name_cleaned=cleanTextField(pa_name),pa_affiliation,pa_affiliation_cleaned = cleanTextField(pa_affiliation))]
data.paper.author.clean <- data.paper.author.clean[,list(authorid,paperid,pa_name,pa_name_cleaned=cleanTextField(pa_name),pa_affiliation,pa_affiliation_cleaned = cleanTextField(pa_affiliation))]
setkey(data.paper.author.clean,paperid)
data.paper.author.extraclean <- data.paper.author.clean[J(data.paper.extraclean[,paperid])]

data.keyword <- data.table(dbGetQuery(con, statement = "SELECT * FROM Keywords"))

data.pa.p <- merge(data.paper.author,data.paper,by="paperid")
data.pa.p <- merge(data.pa.p,data.author[,list(authorid,new_authorid)],by="authorid",all.x=TRUE)
data.pa.p[,new_authorid := ifelse(is.na(new_authorid),authorid,new_authorid)]

data.pa.p.clean <- merge(data.paper.author.clean,data.paper,by="paperid")
data.pa.p.clean <- merge(data.pa.p.clean,data.author[,list(authorid,new_authorid)],by="authorid",all.x=TRUE)
data.pa.p.clean[,new_authorid := ifelse(is.na(new_authorid),authorid,new_authorid)]

data.pa.p.extraclean <- merge(data.paper.author.extraclean,data.paper.extraclean,by="paperid")
data.pa.p.extraclean <- merge(data.pa.p.extraclean,data.author[,list(authorid,new_authorid)],by="authorid",all.x=TRUE)
data.pa.p.extraclean[,new_authorid := ifelse(is.na(new_authorid),authorid,new_authorid)]

##########################################################
############## AUXILIARY TABLES ##########################
##########################################################

setkey(data.keyword,paperid)
setkey(data.paper,paperid)
setkey(data.pa.p,paperid)
authors_chosen <- unique(data.pa.p[J(paper.ids)][,authorid])
#authorpaperkeyword
setkey(data.pa.p,authorid)
apk <- merge(data.pa.p[J(authors_chosen)][,list(paperid,authorid)],data.keyword,by=c("paperid"),allow.cartesian=TRUE)
apk <- apk[,list(authorid,paperid,keyword)]
setkey(apk,authorid,paperid,keyword)
apk_unique <- unique(apk)
#authorkeywordcount
akc <- apk[,list(authorkeywordcount=length(paperid)),by=c("authorid","keyword")]
akc_unique <- apk_unique[,list(authorkeywordcount=length(unique(paperid))),by=c("authorid","keyword")]
#journalkeywordcount
jkc <- merge(data.paper[,list(paperid,journalid)],data.keyword[,list(paperid,keyword)],by=c("paperid"))
jkc <- jkc[,list(journalkeywordcount=length(unique(paperid))),by=c("journalid","keyword")]
#conferencekeywordcount
ckc <- merge(data.paper[,list(paperid,conferenceid)],data.keyword[,list(paperid,keyword)],by=c("paperid"))
ckc <- ckc[,list(conferencekeywordcount=length(unique(paperid))),by=c("conferenceid","keyword")]
#keywordcount
kc <- data.keyword[,list(keywordcount=length(unique(paperid))),by="keyword"]

##########################################################
############## AUTHOR FEATURES ###########################
##########################################################
setkey(data.pa.p,authorid,paperid)

#authorcountcoauthors
data.paper.author.clean1 <- copy(data.paper.author.clean)
setnames(data.paper.author.clean1,"paperid","paperid1")
setkey(data.paper.author.clean1,paperid1)
setkey(data.paper.author.clean,authorid)
acoco_unique <- data.paper.author.clean[,list(af_countcoauthors=nrow(data.paper.author.clean1[J(paperid)])-length(paperid)),by="authorid"]
acoco_unique <- acoco_unique[,list(authorid,af_countcoauthors)]

#authorcountjournals, conferences, papers
authcountpapers <- data.pa.p.clean[,list(af_countjournals=length(unique(journalid)),
                                         af_countconferences=length(unique(conferenceid)),
                                         af_countpapers=length(unique(paperid))
                                         #af_countuniquepapers=length(unique(new_paperid)),
                                         #af_countpaperskeyword=length(unique(paperid[which(nchar(keyword)>0)])),
                                         #af_countpapersobject=length(unique(paperid[which(journalid>0|conferenceid>0)]))
                                         ),by=c("authorid")]

library("stringr")
data.author.rep <- data.table(data.paper.author.clean)
data.author.rep$pa_name <- tolower(data.author.rep$pa_name)
data.author.rep$pa_isemptyname <- nchar(str_trim(data.author.rep$pa_name)) == 0
data.author.rep$pa_name[data.author.rep$pa_isemptyname] <- paste("empty", data.author.rep$authorid[data.author.rep$pa_isemptyname])
data.author.rep$pa_name <- gsub("[^a-z]", "", data.author.rep$pa_name)
data.author.rep$pa_affiliation <- NULL
data.author.rep$paperid <- NULL
data.author.rep <- data.author.rep[!duplicated(data.author.rep$authorid),]
data.author.rep <- merge(data.author.rep, data.author,by="authorid",all.x=TRUE)
#data.author.rep <- fn.join(data.author.rep, data.author)
data.author.rep$pa_ismissing <- is.na(data.author.rep$a_name)
data.author.rep <- data.author.rep[, list(authorid = authorid,
                                          af_sumismissing = sum(pa_ismissing),
                                          af_nauthorsrep = length(authorid), 
                                          af_authorrepid = max(authorid, na.rm = T),
                                          af_ismaxauthorid  =  as.integer(length(authorid) > 1 & authorid == max(authorid)),
                                          af_isminauthorid  =  as.integer(length(authorid) > 1 & authorid == min(authorid))
                                          ), by=c("pa_name")]
data.author.rep$pa_name <- NULL
setkeyv(data.author.rep, "authorid")

#authorcountkeywords, diffkeywords
acdkk <- apk_unique[,list(af_countkeywords=length(keyword),
                          af_countdiffkeywords=length(unique(keyword))
                          ),by=c("authorid")]
#authorfidf (inside author)
at <- merge(akc_unique,acdkk,by=c("authorid"))
at <- merge(at,authcountpapers,by=c("authorid"))
at <- at[,list(af_tfidf=sum(log(af_countpapers/authorkeywordcount)/af_countdiffkeywords)
               #af_tfidf_def=sum(log(af_countpapers/authorkeywordcount))
               ),by=c("authorid")]
#authorfidfbypapers (inside all papers)
atp <- merge(akc_unique,kc,by=c("keyword"))
atp <- merge(atp,acdkk,by=c("authorid"))
atp <- atp[,list(af_tfidfbypapers=sum(log(2000000/keywordcount)/af_countkeywords),
                 af_tfidfbypapers_def=sum(log(2000000/keywordcount))
                 ),by=c("authorid")]

#conf_ratio, journal_ratio
feats.dt <- merge(data.feats,data.paper,by="paperid",all.x=TRUE)
setkey(feats.dt,authorid,paperid)
feats.dt <- unique(feats.dt)
ratios <- feats.dt[,list(af_confratio = sum(conferenceid > 0)/length(conferenceid),
                         af_journalratio = sum(journalid > 0)/length(journalid)
                         ), by="authorid"]
library("infotheo")
ratios[,c("af_confratio","af_journalratio")] <- discretize(data.frame(ratios)[,c("af_confratio","af_journalratio")], disc="equalfreq", nbins=5)

af <- merge(unique(data.feats),data.pa.p.clean,by=c("authorid","paperid"),all.x=TRUE)
af <- af[,list(authorid,paperid,target,year,conferenceid,journalid)]
af <- merge(af,acoco_unique,by="authorid",all.x=TRUE)
af <- merge(af,authcountpapers,by="authorid",all.x=TRUE)
af <- merge(af,data.author.rep,by="authorid",all.x=TRUE)
af$af_authorrepid <- NULL
af <- merge(af,acdkk,by="authorid",all.x=TRUE)
af[,af_countkeywords := ifelse(is.na(af_countkeywords),0,af_countkeywords)]
af[,af_countdiffkeywords := ifelse(is.na(af_countdiffkeywords),0,af_countdiffkeywords)]
af <- merge(af,at,by="authorid",all.x=TRUE)
af[,af_tfidf := ifelse(is.na(af_tfidf),0,af_tfidf)]
#af[,af_tfidf_def := ifelse(is.na(af_tfidf_def),0,af_tfidf_def)]
af <- merge(af,atp,by="authorid",all.x=TRUE)
af[,af_tfidfbypapers := ifelse(is.na(af_tfidfbypapers),0,af_tfidfbypapers)]
af[,af_tfidfbypapers_def := ifelse(is.na(af_tfidfbypapers_def),0,af_tfidfbypapers_def)]
af <- merge(af,ratios,by="authorid",all.x=TRUE)
af[,'af_confratio'] <- factor(paste(as.integer(af[,conferenceid] > 0), af[,af_confratio], sep = "_"))
af[,'af_journalratio'] <- factor(paste(as.integer(af[,journalid] > 0), af[,af_journalratio], sep = "_"))
setkey(af,authorid,paperid)
af$target <- NULL
af$year <- NULL
af$conferenceid <- NULL
af$journalid <- NULL

##########################################################
############## PAPER FEATURES ############################
##########################################################

#papercountauthors
pca <- data.pa.p[,list(pf_countauthors=length(unique(authorid)),
                       pf_countauthors_extraclean=length(unique(new_authorid))
                       ),by=c("paperid")]
#journalcountpapers, authors
jcpa <- data.pa.p[,list(pf_journalcountpapers=length(unique(paperid)),
                        pf_journalcountauthors=length(unique(authorid))
                        ),by=c("journalid")]
#conferencecountpapers, authors
ccpa <- data.pa.p[,list(pf_conferencecountpapers=length(unique(paperid)),
                        pf_conferencecountauthors=length(unique(authorid))
                        ),by=c("conferenceid")]

data.paper.rep <- data.table(data.paper)
data.paper.rep$isemptytitle <- nchar(str_trim(data.paper.rep$title)) == 0
data.paper.rep$title[data.paper.rep$isemptytitle] <- paste("empty", data.paper.rep$paperid[data.paper.rep$isemptytitle])
data.paper.rep$title <- tolower(data.paper.rep$title)
data.paper.rep <- data.paper.rep[, list(paperid = paperid,
                                        pf_npapersrep = length(paperid),
                                        pf_paperrepid = max(paperid, na.rm = T),
                                        #pf_isemptytitle = as.integer(isemptytitle),
                                        #pf_ismaxpaperid  =  as.integer(length(paperid) > 1 & paperid == max(paperid)),
                                        pf_isminpaperid  =  as.integer(length(paperid) > 1 & paperid == min(paperid))
                                        ), by=c("title", "year", "journalid", "conferenceid")]
data.paper.rep$title <- NULL
data.paper.rep$year <- NULL
data.paper.rep$journalid <- NULL
data.paper.rep$conferenceid <- NULL
setkeyv(data.paper.rep, "paperid")

data.paper.rep <- merge(data.paper.author.clean, data.paper.rep,by="paperid")
data.paper.rep$pa_name <- NULL
data.paper.rep$pa_affiliation <- NULL
data.paper.rep$pa_name_cleaned <- NULL
data.paper.rep$pa_affiliation_cleaned <- NULL
data.paper.rep <- merge(data.paper.rep, data.author.rep,by="authorid")
data.paper.rep <- data.paper.rep[!is.na(data.paper.rep$pf_paperrepid),]

data.paper.rep.auth.count <- data.paper.rep[,list(pf_nauthors = length(unique(authorid)),
                                                  pf_nuniqueauthors = length(unique(af_authorrepid))
                                                  ),by = "paperid"]
data.paper.rep <- merge(data.paper.rep, data.paper.rep.auth.count, by="paperid")

data.paper.rep.auth.count <- data.paper.rep[,list(pf_nauthorsrep = length(unique(authorid)),
                                                  pf_nuniqueauthorsrep = length(unique(af_authorrepid))
                                                  ),by = "pf_paperrepid"]
data.paper.rep <- merge(data.paper.rep, data.paper.rep.auth.count,by="pf_paperrepid")

data.paper.rep.auth.count <- data.paper.rep[,list(pf_nauthorveryhiddensources = length(authorid)
                                                  ), by = c("pf_paperrepid", "af_authorrepid")]
data.paper.rep <- merge(data.paper.rep, data.paper.rep.auth.count, by = c("pf_paperrepid", "af_authorrepid"))

data.paper.rep$pf_authorsprop <- data.paper.rep$pf_nauthors/data.paper.rep$pf_nauthorsrep
data.paper.rep$pf_nauthors <- NULL
data.paper.rep$pf_paperrepid <- NULL
data.paper.rep$af_authorrepid <- NULL
data.paper.rep$af_nauthorsrep <- NULL
data.paper.rep$af_sumismissing <- NULL
data.paper.rep$af_isminauthorid <- NULL
data.paper.rep$af_ismaxauthorid <- NULL
setkey(data.paper.rep, authorid, paperid)
data.paper.rep <- unique(data.paper.rep)

#papercountkeywords
pck <- data.keyword[,list(pf_countkeywords=length(unique(keyword))),by=c("paperid")]
#paperjournaltfidf
pjt <- merge(data.keyword[J(paper.ids)],jkc,by=c("journalid","keyword"))
pjt <- merge(pjt,jcpa,by=c("journalid"))
pjt <- merge(pjt,pck,by=c("paperid"))
pjt <- pjt[,list(pf_tfidfbyjournal=sum(log(pf_journalcountpapers/journalkeywordcount)/pf_countkeywords)),by=c("paperid","journalid")]
pjt <- pjt[,list(paperid,pf_tfidfbyjournal)]
#paperconferencetfidf
pct <- merge(data.keyword[J(paper.ids)],ckc,by=c("conferenceid","keyword"))
pct <- merge(pct,ccpa,by=c("conferenceid"))
pct <- merge(pct,pck,by=c("paperid"))
pct <- pct[,list(pf_tfidfbyconference=sum(log(pf_conferencecountpapers/conferencekeywordcount)/pf_countkeywords)),by=c("paperid","conferenceid")]
pct <- pct[,list(paperid,pf_tfidfbyconference)]
#papertfidf
pt <- merge(data.keyword[J(paper.ids)],pck,by=c("paperid"))
pt <- merge(pt,kc,by=c("keyword"))
pt <- pt[,list(pf_tfidfbyall=sum(log(2000000/keywordcount)/pf_countkeywords)),by=c("paperid")]

pf <- merge(unique(data.feats),data.pa.p.clean,by=c("authorid","paperid"),all.x=TRUE)
pf <- pf[,list(authorid,paperid,target,conferenceid,journalid,
               pf_year=year,
               pf_type=ifelse((conferenceid>0&journalid<=0),0,ifelse((journalid>0 & conferenceid<=0),1,ifelse((journalid>0 & conferenceid>0),2,-1))),
               apf_correctaffiliation = ifelse(nchar(pa_affiliation_cleaned)>0,1,0),
               pf_correctkeywords = ifelse(nchar(keyword)>0,1,0),
               pf_badconference = ifelse(conferenceid==-1,1,0))]
pf <- merge(pf,pca,by="paperid",all.x=TRUE)
pf <- merge(pf,jcpa,by="journalid",all.x=TRUE)
pf[,pf_journalcountpapers:=ifelse(journalid>0,pf_journalcountpapers,-1)]
pf[,pf_journalcountauthors:=ifelse(journalid>0,pf_journalcountauthors,-1)]
pf <- merge(pf,ccpa,by="conferenceid",all.x=TRUE)
pf[,pf_conferencecountpapers:=ifelse(conferenceid>0,pf_conferencecountpapers,-1)]
pf[,pf_conferencecountauthors:=ifelse(conferenceid>0,pf_conferencecountauthors,-1)]
pf <- merge(pf,data.paper.rep,by=c("authorid","paperid"),all.x=TRUE)
pf <- merge(pf,pck,by="paperid",all.x=TRUE)
pf[,pf_countkeywords:=ifelse(is.na(pf_countkeywords),0,pf_countkeywords)]
pf <- merge(pf,pjt,by="paperid",all.x=TRUE)
pf[,pf_tfidfbyjournal:=ifelse(journalid>0,pf_tfidfbyjournal,-1)]
pf[,pf_tfidfbyjournal:=ifelse(is.na(pf_tfidfbyjournal),0,pf_tfidfbyjournal)]
pf <- merge(pf,pct,by="paperid",all.x=TRUE)
pf[,pf_tfidfbyconference:=ifelse(conferenceid>0,pf_tfidfbyconference,-1)]
pf[,pf_tfidfbyconference:=ifelse(is.na(pf_tfidfbyconference),0,pf_tfidfbyconference)]
pf <- merge(pf,pt,by="paperid",all.x=TRUE)
pf[,pf_tfidfbyall:=ifelse(is.na(pf_tfidfbyall),0,pf_tfidfbyall)]
setkey(pf,authorid,paperid)
pf$conferenceid <- NULL
pf$journalid <- NULL
pf$target <- NULL

######################################################
#########       AUTHOR-PAPER FEATURES      ###########
######################################################

#authorpapersource
aps <- data.pa.p[,list(apf_source=length(pa_name_cleaned),
                       apf_sourceaff=length(which(nchar(pa_affiliation_cleaned)>0))
                       #apf_sourceyear=length(which(year>=1900 & year<=2013)),
                       #apf_sourceobject=length(which(conferenceid>0 | journalid>0))
                       ),by=c("authorid","paperid")]

#authorpapersource_extraclean
aps_extraclean <- data.pa.p.clean[,list(#apf_source_extraclean=length(pa_name_cleaned),
                                        apf_sourceaff_extraclean=length(which(nchar(pa_affiliation_cleaned)>0))
                                        ), by=c("authorid","new_paperid")]
aps_extraclean <- merge(data.pa.p.clean[,list(authorid,paperid,new_paperid)],aps_extraclean,by=c("authorid","new_paperid"))

#authorpaperhiddensource (by author name)
apwn <- data.pa.p[,list(wordname = unlist(strsplit(pa_name_cleaned," "))),by=c("authorid","paperid")]
apwn2 <- apwn[,list(wordcount_unique = length(unique(authorid)),
                    wordcount = length(authorid)
                    ),by=c("paperid","wordname")]
apwn2 <- merge(apwn,apwn2,by=c("paperid","wordname"),all.x=TRUE)
aphs <- apwn2[,list(apf_hiddensource_unique = sum(wordcount_unique)-length(wordcount_unique),
                    apf_hiddensource = sum(wordcount)-length(wordcount)
                    ),by=c("paperid","authorid")]

#authorpapersameauthortarget
setkey(apwn2,authorid,paperid)
paawn <- merge(apwn2[unique(data.feats)],apwn2,by=c("paperid","wordname"),allow.cartesian=TRUE)[authorid.x!=authorid.y]
setkey(paawn,paperid,authorid.x,authorid.y)
paawn <- unique(paawn)[,list(paperid,authorid.x,authorid.y,target)]
setnames(paawn,c("authorid.x","authorid.y","target"),c("authorid1","authorid","target1"))
paawn <- merge(paawn,data.feats,by=c("paperid","authorid"),all.x=TRUE,allow.cartesian=TRUE)
paawn[which(is.na(paawn[,target])),target:=-1]
setnames(paawn,c("authorid","target"),c("authorid2","target2"))
apsat <- paawn[,list(apf_sameauthortarget = max(target2)),by=c("paperid","authorid1")]
setnames(apsat,"authorid1","authorid")

#authorpaperaffiliationsource
setkey(data.pa.p,paperid)
apwaf <- data.pa.p[J(paper.ids)][,list(authorid,paperid,pa_affiliation_cleaned)]
apwaf <- apwaf[,list(wordaff = unique(unlist(strsplit(pa_affiliation_cleaned," ")))),by=c("authorid","paperid")]
apwaf <- apwaf[(!wordaff %in% stopwords("english"))]
apwaf <- merge(apwaf,pca,by="paperid")
apwaf <- apwaf[,list(wordaff,countwordaff = length(wordaff),pf_countauthors),by=c("authorid","paperid")]
setkey(apwaf,authorid,paperid)
apwaf <- merge(apwaf[data.feats],apwaf[,list(paperid,authorid,wordaff)],by=c("paperid","wordaff"),allow.cartesian=TRUE)
apafs <- apwaf[,list(apf_commonaffauthors = length(unique(authorid.y[which(authorid.y!=authorid.x)]))/max(pf_countauthors),
                     apf_commonaffwords = length(unique(wordaff[which(authorid.y!=authorid.x)]))/max(countwordaff)
                     ),by=c("paperid","authorid.x")]
setnames(apafs,"authorid.x","authorid")

#authorjournalcount (count papers of author in the same journal)
ajc <- data.pa.p.clean[,list(apf_authorjournalcount=length(unique(paperid)),
                             #apf_authorjournalcount_title=length(unique(paperid[which(nchar(title)>0)])),
                             apf_authorjournalcount_keyword=length(unique(paperid[which(nchar(keyword)>0)])),
                             apf_authorjournalcount_year=length(unique(paperid[which(year>=1900 & year<=2013)])),
                             apf_authorjournalcount_object=length(unique(paperid[which(conferenceid>0 | journalid>0)]))
                             ),by=c("authorid","journalid")]

#authorconferencecount (countpapers of author in the same conference)
acc <- data.pa.p.clean[,list(apf_authorconferencecount=length(unique(paperid)),
                             apf_authorconferencecount_title=length(unique(paperid[which(nchar(title)>0)])),
                             #apf_authorconferencecount_keyword=length(unique(paperid[which(nchar(keyword)>0)])),
                             apf_authorconferencecount_year=length(unique(paperid[which(year>=1900 & year<=2013)])),
                             apf_authorconferencecount_object=length(unique(paperid[which(conferenceid>0 | journalid>0)]))
                             ),by=c("authorid","conferenceid")]

#conferenceaffiliationcount
cac <- data.pa.p.clean[,list(apf_conferenceaffiliationcount=length(unique(paperid))
                             #apf_conferenceaffiliationcount_title=length(unique(paperid[which(nchar(title)>0)])),
                             #apf_conferenceaffiliationcount_keyword=length(unique(paperid[which(nchar(keyword)>0)])),
                             #apf_conferenceaffiliationcount_year=length(unique(paperid[which(year>=1900 & year<=2013)]))
                             ),by=c("conferenceid","pa_affiliation_cleaned")]

#authorpapercountpaperswithcoauthors
setkey(data.pa.p.clean,authorid,paperid)
coauth <- merge(data.pa.p.clean[data.feats],data.pa.p.clean,by=c("paperid"),allow.cartesian=TRUE)[authorid.x!=authorid.y]
setnames(coauth,c("authorid.x","authorid.y"),c("authorid1","authorid2"))
coauth <- coauth[,list(paperid,authorid1,authorid2)]
coauth_countpapers <- coauth[,list(countpaperstogether=length(unique(paperid))),by=c("authorid1","authorid2")]
apcpc <- merge(coauth,coauth_countpapers,by=c("authorid1","authorid2"))
apcpc <- apcpc[,list(apf_countpaperstogether=sum(countpaperstogether)),by=c("authorid1","paperid")]
setnames(apcpc,"authorid1","authorid")

sameauthors <- paawn[,list(sameauthor=1),by=c("authorid1","authorid2","paperid")]
coauth_nosameauthors <- merge(coauth,sameauthors,by=c("authorid1","authorid2","paperid"),all.x=TRUE)
coauth_nosameauthors <- coauth_nosameauthors[which(is.na(sameauthor))]
coauth_nosameauthors <- coauth_nosameauthors[,list(paperid,authorid1,authorid2)]
coauth_nosameauthors_countpapers <- coauth_nosameauthors[,list(countpaperstogether=length(unique(paperid))),by=c("authorid1","authorid2")]
apcpc_nosameauthors <- merge(coauth_nosameauthors,coauth_nosameauthors_countpapers,by=c("authorid1","authorid2"))
apcpc_nosameauthors <- apcpc_nosameauthors[,list(apf_countpaperstogether_nosameauthors=sum(countpaperstogether)),by=c("authorid1","paperid")]
setnames(apcpc_nosameauthors,"authorid1","authorid")

data.pa.p.keyword <- data.pa.p[nchar(keyword)>0]
setkey(data.pa.p.keyword,authorid,paperid)
coauth_keyword <- merge(data.pa.p.keyword[data.feats],data.pa.p.keyword,by=c("paperid"),allow.cartesian=TRUE)[authorid.x!=authorid.y]
setnames(coauth_keyword,c("authorid.x","authorid.y"),c("authorid1","authorid2"))
coauth_keyword <- coauth_keyword[,list(paperid,authorid1,authorid2)]
coauth_keyword_countpapers <- coauth_keyword[,list(countpaperstogether=length(unique(paperid))),by=c("authorid1","authorid2")]
apcpc_keyword <- merge(coauth_keyword,coauth_keyword_countpapers,by=c("authorid1","authorid2"))
apcpc_keyword <- apcpc_keyword[,list(apf_countpaperstogether_keyword=sum(countpaperstogether)),by=c("authorid1","paperid")]
setnames(apcpc_keyword,"authorid1","authorid")

setkey(data.pa.p.extraclean,authorid,paperid)
coauth_extraclean <- merge(data.pa.p.extraclean[data.feats],data.pa.p.extraclean,by=c("paperid"),allow.cartesian=TRUE)[authorid.x!=authorid.y]
setnames(coauth_extraclean,c("authorid.x","authorid.y"),c("authorid1","authorid2"))
coauth_extraclean <- coauth_extraclean[,list(paperid,authorid1,authorid2)]
coauth_extraclean_countpapers <- coauth_extraclean[,list(countpaperstogether=length(unique(paperid))),by=c("authorid1","authorid2")]
apcpc_extraclean <- merge(coauth_extraclean,coauth_extraclean_countpapers,by=c("authorid1","authorid2"))
apcpc_extraclean <- apcpc_extraclean[,list(apf_countpaperstogether_extraclean=sum(countpaperstogether)),by=c("authorid1","paperid")]
setnames(apcpc_extraclean,"authorid1","authorid")

#authorpapercountkeywordswithcoauthors
coakey <- merge(coauth,data.keyword[,list(paperid,keyword)],by="paperid",allow.cartesian=TRUE)
coakey_count <- coakey[,list(countkeywordstogether=length(keyword),
                             countuniquekeywordstogether=length(unique(keyword))
                             ),by=c("authorid1","authorid2")]
setkey(coakey,paperid,authorid1,authorid2)
apckc <- unique(coakey)
apckc <- merge(apckc,coakey_count,by=c("authorid1","authorid2"))
apckc <- apckc[,list(apf_countkeywordstogether=sum(countkeywordstogether)),by=c("authorid1","paperid")]
setnames(apckc,"authorid1","authorid")

#authorpapercommonjournalnameword
setkey(data.pa.p,paperid)
ajname <- merge(data.pa.p[J(paper.ids)],data.journal,by="journalid")[,list(authorid,j_fullname)]
ajname <- ajname[,list(j_nameword = unique(unlist(strsplit(j_fullname," ")))),by=c("authorid")]
ajname <- ajname[(!j_nameword %in% stopwords("english"))]
apwjn <- data.pa.p[J(paper.ids)][,list(authorid,paperid)]
apwjn <- merge(apwjn,ajname,by="authorid",allow.cartesian=TRUE)
setkey(apwjn,authorid,paperid)
apwjn <- merge(apwjn[data.feats],apwjn,by=c("paperid","j_nameword"),allow.cartesian=TRUE)[authorid.y!=authorid.x]
apwjn <- apwjn[,list(apf_commonjournalnameword = length(unique(j_nameword))),by=c("paperid","authorid.x")]
setnames(apwjn,"authorid.x","authorid")

#authorpapercommonjournalwebword
setkey(data.pa.p,paperid)
ajweb <- merge(data.pa.p[J(paper.ids)],data.journal,by="journalid")[,list(authorid,j_homepage)]
ajweb <- ajweb[,list(j_webword = unique(unlist(strsplit(j_homepage," ")))),by=c("authorid")]
ajweb <- ajweb[(!j_webword %in% stopwords_web)]
apwjw <- data.pa.p[J(paper.ids)][,list(authorid,paperid)]
apwjw <- merge(apwjw,ajweb,by="authorid",allow.cartesian=TRUE)
setkey(apwjw,authorid,paperid)
apwjw <- merge(apwjw[data.feats],apwjw,by=c("paperid","j_webword"),allow.cartesian=TRUE)[authorid.y!=authorid.x]
apwjw <- apwjw[,list(apf_commonjournalwebword = length(unique(j_webword))),by=c("paperid","authorid.x")]
setnames(apwjw,"authorid.x","authorid")

#authorpapercommonconferencenameword
setkey(data.pa.p,paperid)
acname <- merge(data.pa.p[J(paper.ids)],data.conference,by="conferenceid")[,list(authorid,conf_fullname)]
acname <- acname[,list(conf_nameword = unique(unlist(strsplit(conf_fullname," ")))),by=c("authorid")]
acname <- acname[(!conf_nameword %in% stopwords("english"))]
apwcn <- data.pa.p[J(paper.ids)][,list(authorid,paperid)]
apwcn <- merge(apwcn,acname,by="authorid",allow.cartesian=TRUE)
setkey(apwcn,authorid,paperid)
apwcn <- merge(apwcn[data.feats],apwcn,by=c("paperid","conf_nameword"),allow.cartesian=TRUE)[authorid.y!=authorid.x]
apwcn <- apwcn[,list(apf_commonconferencenameword = length(unique(conf_nameword))),by=c("paperid","authorid.x")]
setnames(apwcn,"authorid.x","authorid")

#authorpapercommonconferencewebword
setkey(data.pa.p,paperid)
acweb <- merge(data.pa.p[J(paper.ids)],data.conference,by="conferenceid")[,list(authorid,conf_homepage)]
acweb <- acweb[,list(conf_webword = unique(unlist(strsplit(conf_homepage," ")))),by=c("authorid")]
acweb <- acweb[(!conf_webword %in% stopwords_web)]
apwcw <- data.pa.p[J(paper.ids)][,list(authorid,paperid)]
apwcw <- merge(apwcw,acweb,by="authorid",allow.cartesian=TRUE)
setkey(apwcw,authorid,paperid)
apwcw <- merge(apwcw[data.feats],apwcw,by=c("paperid","conf_webword"),allow.cartesian=TRUE)[authorid.y!=authorid.x]
apwcw <- apwcw[,list(apf_commonconferencewebword = length(unique(conf_webword))),by=c("paperid","authorid.x")]
setnames(apwcw,"authorid.x","authorid")

#authoryearcount
ayc <- data.pa.p.clean[,list(apf_authoryearcount=length(unique(paperid))),by=c("authorid","year")]

#authorpaperuniquekeywords
apuk_unique <- merge(data.feats, data.keyword, by = "paperid", allow.cartesian=TRUE)
apuk_unique <- merge(apuk_unique,akc_unique,by=c("authorid","keyword"))
apuk_unique <- apuk_unique[,list(apf_uniquekeywords = length(which(authorkeywordcount==1)),
                                 apf_frequentkeywords = length(which(authorkeywordcount>1)),
                                 apf_veryfrequentkeywords = length(which(authorkeywordcount>3)),
                                 apf_ratefrequentkeywords = length(which(authorkeywordcount>3))/length(keyword)
                                 ),by=c("authorid","paperid")]

#authorpapercountsameaffiliation
setkey(data.pa.p,authorid,paperid)
coauth <- merge(data.pa.p[data.feats],data.pa.p,by=c("paperid"),allow.cartesian=TRUE)[authorid.x!=authorid.y]
coauth <- coauth[,list(paperid,authorid.x,authorid.y,pa_affiliation_cleaned.x,pa_affiliation_cleaned.y)]
aasameaff <- coauth[,list(apf_papersameaff = length(which(pa_affiliation_cleaned.y==pa_affiliation_cleaned.x))),by=c("authorid.x","paperid")]
setnames(aasameaff,"authorid.x","authorid")
#samename, sameaffiliation
sna <- merge(data.pa.p[,list(paperid,authorid,pa_name_cleaned,pa_affiliation_cleaned)],data.author,by="authorid",all.x=TRUE)
sna <- sna[,list(authorid,paperid,samename = ifelse(pa_name_cleaned==a_name_cleaned,1,0), sameaffiliation = ifelse(pa_affiliation_cleaned==a_affiliation_cleaned,1,0))]
sna <- sna[,list(apf_samename = min(samename),apf_sameaffiliation=min(sameaffiliation)),by=c("authorid","paperid")]
#apf_year
setkey(data.pa.p.clean,authorid)
apfyear <- data.pa.p.clean[J(author.ids)][,list(authorid,paperid,year)]
apfyear <- apfyear[,list(paperid,apf_year=yearFeature(year)),by="authorid"]

apf <- merge(unique(data.feats),data.pa.p.clean,by=c("authorid","paperid"),all.x=TRUE)
apf <- apf[,list(authorid,paperid,target,conferenceid,journalid,year,pa_affiliation_cleaned)]
apf <- merge(apf,apfyear,by=c("authorid","paperid"),all.x=TRUE)
apf <- merge(apf,aps,by=c("authorid","paperid"),all.x=TRUE)
apf <- merge(apf,aps_extraclean,by=c("authorid","paperid"),all.x=TRUE)
apf[,apf_sourceaff_extraclean:=ifelse(is.na(apf_sourceaff_extraclean),-1,apf_sourceaff_extraclean)]
apf <- merge(apf,aphs,by=c("authorid","paperid"),all.x=TRUE)
apf[,apf_hiddensource:=ifelse(is.na(apf_hiddensource),0,apf_hiddensource)]
apf[,apf_hiddensource_unique:=ifelse(is.na(apf_hiddensource_unique),0,apf_hiddensource_unique)]
apf <- merge(apf,apsat,by=c("authorid","paperid"),all.x=TRUE)
apf[,apf_sameauthortarget:=ifelse(is.na(apf_sameauthortarget),-1,apf_sameauthortarget)]
apf <- merge(apf,apafs,by=c("authorid","paperid"),all.x=TRUE)
apf[,apf_commonaffauthors:=ifelse(is.na(apf_commonaffauthors),-1,apf_commonaffauthors)]
apf[,apf_commonaffwords:=ifelse(is.na(apf_commonaffwords),-1,apf_commonaffwords)]
apf <- merge(apf,ajc,by=c("authorid","journalid"),all.x=TRUE)
apf[,apf_authorjournalcount:=ifelse(journalid>0,apf_authorjournalcount,-1)]
apf[,apf_authorjournalcount:=ifelse(is.na(apf_authorjournalcount),-1,apf_authorjournalcount)]
apf[,apf_authorjournalcount_keyword:=ifelse(journalid>0,apf_authorjournalcount_keyword,-1)]
apf[,apf_authorjournalcount_keyword:=ifelse(is.na(apf_authorjournalcount_keyword),-1,apf_authorjournalcount_keyword)]
apf[,apf_authorjournalcount_year:=ifelse(journalid>0,apf_authorjournalcount_year,-1)]
apf[,apf_authorjournalcount_year:=ifelse(is.na(apf_authorjournalcount_year),-1,apf_authorjournalcount_year)]
apf[,apf_authorjournalcount_object:=ifelse(journalid>0,apf_authorjournalcount_object,-1)]
apf[,apf_authorjournalcount_object:=ifelse(is.na(apf_authorjournalcount_object),-1,apf_authorjournalcount_object)]

apf <- merge(apf,acc,by=c("authorid","conferenceid"),all.x=TRUE)
apf[,apf_authorconferencecount:=ifelse(conferenceid>0,apf_authorconferencecount,-1)]
apf[,apf_authorconferencecount:=ifelse(is.na(apf_authorconferencecount),-1,apf_authorconferencecount)]
apf[,apf_authorconferencecount_title:=ifelse(conferenceid>0,apf_authorconferencecount_title,-1)]
apf[,apf_authorconferencecount_title:=ifelse(is.na(apf_authorconferencecount_title),-1,apf_authorconferencecount_title)]
apf[,apf_authorconferencecount_year:=ifelse(conferenceid>0,apf_authorconferencecount_year,-1)]
apf[,apf_authorconferencecount_year:=ifelse(is.na(apf_authorconferencecount_year),-1,apf_authorconferencecount_year)]
apf[,apf_authorconferencecount_object:=ifelse(conferenceid>0,apf_authorconferencecount_object,-1)]
apf[,apf_authorconferencecount_object:=ifelse(is.na(apf_authorconferencecount_object),-1,apf_authorconferencecount_object)]

apf <- merge(apf,cac,by=c("conferenceid","pa_affiliation_cleaned"),all.x=TRUE)
apf[,apf_conferenceaffiliationcount:=ifelse(conferenceid>0,apf_conferenceaffiliationcount,-1)]
apf[,apf_conferenceaffiliationcount:=ifelse(is.na(apf_conferenceaffiliationcount),-1,apf_conferenceaffiliationcount)]

apf <- merge(apf,apcpc,by=c("authorid","paperid"),all.x=TRUE) #countpaperstogether
apf[,apf_countpaperstogether:=ifelse(is.na(apf_countpaperstogether),0,apf_countpaperstogether)]
apf <- merge(apf,apcpc_nosameauthors,by=c("authorid","paperid"),all.x=TRUE) #countpaperstogether_nosameauthors
apf[,apf_countpaperstogether_nosameauthors:=ifelse(is.na(apf_countpaperstogether_nosameauthors),0,apf_countpaperstogether_nosameauthors)]
apf <- merge(apf,apcpc_keyword,by=c("authorid","paperid"),all.x=TRUE) #countpaperstogether_keyword
apf[,apf_countpaperstogether_keyword:=ifelse(is.na(apf_countpaperstogether_keyword),0,apf_countpaperstogether_keyword)]
apf <- merge(apf,apcpc_extraclean,by=c("authorid","paperid"),all.x=TRUE) #countpaperstogether_extraclean
apf[,apf_countpaperstogether_extraclean:=ifelse(is.na(apf_countpaperstogether_extraclean),0,apf_countpaperstogether_extraclean)]
apf <- merge(apf,apckc,by=c("authorid","paperid"),all.x=TRUE) #countkeywordstogether, countuniquekeywordstogether #2nd threshold
apf[,apf_countkeywordstogether:=ifelse(is.na(apf_countkeywordstogether),0,apf_countkeywordstogether)]

apf <- merge(apf,apwjn,by=c("authorid","paperid"),all.x=TRUE) #authorpapercommonjournalnameword
apf[,apf_commonjournalnameword:=ifelse(is.na(apf_commonjournalnameword),0,apf_commonjournalnameword)]
apf <- merge(apf,apwjw,by=c("authorid","paperid"),all.x=TRUE) #authorpapercommonjournalwebword
apf[,apf_commonjournalwebword:=ifelse(is.na(apf_commonjournalwebword),0,apf_commonjournalwebword)]
apf <- merge(apf,apwcn,by=c("authorid","paperid"),all.x=TRUE) #authorpapercommonconferencenameword
apf[,apf_commonconferencenameword:=ifelse(is.na(apf_commonconferencenameword),0,apf_commonconferencenameword)]
apf <- merge(apf,apwcw,by=c("authorid","paperid"),all.x=TRUE) #authorpapercommonconferencewebword #4th threshold
apf[,apf_commonconferencewebword:=ifelse(is.na(apf_commonconferencewebword),0,apf_commonconferencewebword)]
apf <- merge(apf,ayc,by=c("authorid","year"),all.x=TRUE) #authoryearcount
apf[,apf_authoryearcount:=ifelse(year==-1,-1,apf_authoryearcount)]
apf[,apf_authoryearcount:=ifelse(is.na(apf_authoryearcount),0,apf_authoryearcount)]

apf <- merge(apf,apuk_unique,by=c("authorid","paperid"),all.x=TRUE) #authorpaperuniquekeywords
apf[,apf_uniquekeywords:=ifelse(is.na(apf_uniquekeywords),0,apf_uniquekeywords)]
apf[,apf_frequentkeywords:=ifelse(is.na(apf_frequentkeywords),0,apf_frequentkeywords)]
apf[,apf_veryfrequentkeywords:=ifelse(is.na(apf_veryfrequentkeywords),0,apf_veryfrequentkeywords)]
apf[,apf_ratefrequentkeywords:=ifelse(is.na(apf_ratefrequentkeywords),-1,apf_ratefrequentkeywords)]
apf <- merge(apf,aasameaff,by=c("authorid","paperid"),all.x=TRUE) #sameaff
apf[,apf_papersameaff:=ifelse(is.na(apf_papersameaff),0,apf_papersameaff)]
apf <- merge(apf,sna,by=c("authorid","paperid"),all.x=TRUE) #samename, sameaffiliation #5th threshold
apf[,apf_sameaffiliation:=ifelse(is.na(apf_sameaffiliation),-1,apf_sameaffiliation)]
apf[,apf_samename:=ifelse(is.na(apf_samename),-1,apf_samename)]

apf$year <- NULL
apf$conferenceid <- NULL
apf$journalid <- NULL
apf$pa_affiliation_cleaned <- NULL
apf$new_paperid <- NULL
apf$target <- NULL
setkey(apf,authorid,paperid)

traintest <- merge(data.feats,af,by=c("authorid","paperid"),all.x=TRUE)
traintest <- merge(traintest,pf,by=c("authorid","paperid"),all.x=TRUE)
traintest <- merge(traintest,apf,by=c("authorid","paperid"),all.x=TRUE)
traintest <- as.data.frame(traintest)

train <- traintest[which(traintest$target!=-1),]
train <- data.table(train)
setkey(train,authorid,paperid)
train <- as.data.frame(unique(train))  
#cor(train,train$target)

test <- traintest[which(traintest$target==-1),]
test <- data.table(test)
setkey(test,authorid,paperid)
test <- as.data.frame(unique(test))  

load("data/data.feat.likelihood.dt.RData")
load("data/data.dup.source.dt.RData")

train <- data.table(train)
train <- merge(train,data.feat.likelihood.dt,by=c("authorid","paperid"),all.x=TRUE)
train <- merge(train,data.dup.source.dt,by=c("authorid","paperid"),all.x=TRUE)
setkey(train,authorid,paperid)
train <- data.frame(train)
test <- data.table(test)
test <- merge(test,data.feat.likelihood.dt,by=c("authorid","paperid"),all.x=TRUE)
test <- merge(test,data.dup.source.dt,by=c("authorid","paperid"),all.x=TRUE)
setkey(test,authorid,paperid)
test <- data.frame(test)

save(train,test,file="data/traintest_features.RData")

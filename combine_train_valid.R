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

library(data.table)
data.train.ids <- data.table(read.csv("data/TrainInitial.csv",stringsAsFactors=FALSE))
setnames(data.train.ids,c("authorid","confirmedpaperids","deletedpaperids"))
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

data.valid.ids <- data.table(read.csv("data/ValidInitial.csv",stringsAsFactors=FALSE))
setnames(data.valid.ids,c("authorid","paperids"))
data.valid.ids <- data.valid.ids[
  ,list(paperid = as.integer(unlist(strsplit(paperids, split = "\\s+"))),
        confirmed = 0),
  by="authorid"]
data.valid.ids <- data.valid.ids[,list(count=c(1:length(confirmed))),by=c("authorid","paperid")]

data.valid.confirm <- data.table(read.csv("data/ValidInitialSolution.csv",stringsAsFactors=FALSE)[,1:2])
setnames(data.valid.confirm,c("authorid","paperids"))
data.valid.confirm <- data.valid.confirm[
  ,list(paperid = as.integer(unlist(strsplit(paperids, split = "\\s+"))),
        confirmed = 1),
  by="authorid"]
data.valid.confirm <- data.valid.confirm[,list(confirmed,count=c(1:length(confirmed))),by=c("authorid","paperid")]

data.valid.ids <- merge(data.valid.ids,data.valid.confirm,by=c("authorid","paperid","count"),all.x=TRUE)
data.valid.ids <- data.valid.ids[,list(authorid,paperid,confirmed)]
data.valid.ids[,confirmed:=ifelse(is.na(confirmed),0,confirmed)]

data.train.ids <- rbind(data.train.ids,data.valid.ids)
data.train.new <- data.train.ids[,list(confirmedpaperids = paste(paperid[confirmed==1],collapse=" "), deletedpaperids = paste(paperid[confirmed==0],collapse=" ")),by="authorid"]
setnames(data.train.new,c("AuthorId","ConfirmedPaperIds","DeletedPaperIds"))
data.train.new <- data.frame(data.train.new)
write.csv(data.train.new,file="data/Train.csv",row.names=FALSE,quote=FALSE)

data.test.ids <- data.table(read.csv("data/TestPaper.csv",stringsAsFactors=FALSE))
setnames(data.test.ids,c("authorid","paperid"))
data.test.ids <- data.test.ids[,list(authorid,paperid,confirmed=-1)]

data.feats <- rbind(data.train.ids,data.test.ids)
data.feats.confirm <- unique(data.feats[confirmed==1])
data.feats.delete <- unique(data.feats[confirmed==0])
data.feats.unknown <- unique(data.feats[confirmed==-1])
data.feats <- rbind(data.feats.confirm,data.feats.delete,data.feats.unknown)

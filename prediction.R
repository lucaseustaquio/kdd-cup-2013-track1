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
load("data/traintest_features.RData")

flist <- setdiff(colnames(train), c("authorid","paperid"))
set.seed(3454789)
ntrees <- 4500
intdepth <- 2
shrinkage <- 0.06
model_bernoulli <- gbm(target~.,train[,flist],distribution="bernoulli",n.trees=ntrees,shrinkage=shrinkage,interaction.depth=intdepth,bag.fraction=0.5,train.fraction=1.0,n.minobsinnode=10,cv.folds=0,verbose=TRUE)
predicted_bernoulli <- predict(model_bernoulli,test[,flist],n.trees=ntrees,type="response")
predicted <- data.frame(authorid=test$authorid,paperid=test$paperid,pred=predicted_bernoulli)

result <- c()
authors <- unique(predicted$authorid)
for (i in 1:length(authors)) {
  ix <- which(predicted$authorid==authors[i])
  papers <- predicted$paperid[ix]
  pr <- predicted$pred[ix]
  papers.string <- paste(papers[sort(pr,decreasing=TRUE,index.return=TRUE)$ix], collapse=" ")
  result <- rbind(result, c(authors[i], papers.string))
}
result <- as.data.frame(result)
colnames(result) <- c('AuthorId','PaperIds')
write.csv(result,file='prediction.csv', quote = FALSE, row.names=FALSE)

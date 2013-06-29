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

require("compiler")
enableJIT(3) 
setCompilerOptions(suppressUndefined = T)
options(stringsAsFactors = FALSE)

path.input <- "./data"
path.wd <- getwd()

map.n <- 1000

#############################################################
# tic toc
#############################################################
tic <- function(gcFirst = TRUE, type=c("elapsed", "user.self", "sys.self")) {
  type <- match.arg(type)
  assign(".type", type, envir=baseenv())
  if(gcFirst) gc(FALSE)
  tic <- proc.time()[type]         
  assign(".tic", tic, envir=baseenv())
  invisible(tic)
}

toc <- function() {
  type <- get(".type", envir=baseenv())
  toc <- proc.time()[type]
  tic <- get(".tic", envir=baseenv())
  print(toc - tic)
  invisible(toc)
}

##############################################################
## Registers parallel workers
##############################################################
fn.register.wk <- function(n.proc = NULL) {
  if (file.exists("data/cluster.csv")) {
    cluster.conf <- read.csv("data/cluster.csv", 
                             stringsAsFactors = F,
                             comment.char = "#")
    n.proc <- NULL
    for (i in 1:nrow(cluster.conf)) {
      n.proc <- c(n.proc, 
                  rep(cluster.conf$host[i], 
                      cluster.conf$cores[i]))
    }
  }
  if (is.null(n.proc)) {
    n.proc = as.integer(Sys.getenv("NUMBER_OF_PROCESSORS"))
    if (is.na(n.proc)) {
      library(parallel)
      n.proc <-detectCores()
    }
  }
  workers <- mget(".pworkers", envir=baseenv(), ifnotfound=list(NULL));
  if (!exists(".pworkers", envir=baseenv()) || length(workers$.pworkers) == 0) {
    
    library(doSNOW)
    library(foreach)
    workers<-suppressWarnings(makeSOCKcluster(n.proc));
    suppressWarnings(registerDoSNOW(workers))
    #     clusterEvalQ(workers, Sys.getpid())
    clusterSetupRNG(workers, seed=3454789)#Sys.time())
    assign(".pworkers", workers, envir=baseenv());
    
    tic()
    cat("Workers start time: ", format(Sys.time(), format = "%Y-%m-%d %H:%M:%S"), "\n")
  }
  invisible(workers);
}

##############################################################
## Kill parallel workers
##############################################################
fn.kill.wk <- function() {
  library(doSNOW)
  library(foreach)
  workers <- mget(".pworkers", envir=baseenv(), ifnotfound=list(NULL));
  if (exists(".pworkers", envir=baseenv()) && length(workers$.pworkers) != 0) {
    stopCluster(workers$.pworkers);
    assign(".pworkers", NULL, envir=baseenv());
    cat("Workers finish time: ", format(Sys.time(), format = "%Y-%m-%d %H:%M:%S"), "\n")
    toc()
  }
  invisible(workers);
}

##############################################################
## init worker setting work dir and doing path redirect
##############################################################
fn.init.worker <- function(log = NULL, add.date = F) {
  setwd(path.wd)
  
  if (!is.null(log)) {
    date.str <- format(Sys.time(), format = "%Y-%m-%d_%H-%M-%S")
    
    if (add.date) {
      output.file <- fn.log.file(paste(log, "_",date.str,
                                       ".log", sep=""))
    } else {
      output.file <- fn.log.file(paste(log,".log", sep=""))
    }
    output.file <- file(output.file, open = "wt")
    sink(output.file)
    sink(output.file, type = "message")
    
    cat("Start:", date.str, "\n")
  }
  
  tic()
}

##############################################################
## get median or mode
##############################################################
fn.median.or.mode <- function(x) {
  if (is.factor(x)) {
    
  }
}


##############################################################
## clean worker resources
##############################################################
fn.clean.worker <- function() {
  gc()
  
  try(toc())
  suppressWarnings(sink())
  suppressWarnings(sink(type = "message"))
}

#############################################################
# log file path
#############################################################
fn.log.file <- function(name) {
  paste(path.wd, "log", name, sep="/")
}

#############################################################
# input file path
#############################################################
fn.input.file <- function(name) {
  paste(path.input, name, sep="/")
#   name
}

#############################################################
# python output file path
#############################################################
fn.python.out <- function(name) {
  paste("python/output", name, sep="/")
}

#############################################################
# python input file path
#############################################################
fn.python.in <- function(name) {
  paste("python/input", name, sep="/")
}

#############################################################
# matlab input file path
#############################################################
fn.matlab.in <- function(name) {
  paste("matlab/input", name, sep="/")
}

#############################################################
# matlab output file path
#############################################################
fn.matlab.out <- function(name) {
  paste("matlab/output", name, sep="/")
}

#############################################################
# matlab output file path
#############################################################
fn.libfm.file <- function(name) {
  paste("libfm", name, sep="/")
}

#############################################################
# sofiaml output file path
#############################################################
fn.sofiaml.out <- function(name) {
  paste("sofiaml/output", name, sep="/")
}

#############################################################
# sofiaml output file path
#############################################################
fn.sofiaml.in <- function(name) {
  paste("sofiaml/input", name, sep="/")
}

#############################################################
# submission file path
#############################################################
fn.submission.file <- function(name) {
  name <- paste0(name, ".csv")
  paste(path.wd, "submission", name, sep="/")
}

#############################################################
# data file path
#############################################################
fn.data.file <- function(name) {
  paste(path.wd, "data", name, sep="/")
}

#############################################################
# save data file
#############################################################
fn.save.data <- function(dt.name, envir = parent.frame()) {
  save(list = dt.name, 
       file = fn.data.file(paste0(dt.name, ".RData")), envir = envir)
}

#############################################################
# load saved file
#############################################################
fn.load.data <- function(dt.name, envir = parent.frame()) {
  load(fn.data.file(paste0(dt.name, ".RData")), envir = envir)
}

#############################################################
# read input csv
#############################################################
fn.read.input.csv <- function(file, key = NULL) {
  library("data.table")
  data.dt <- data.table(read.csv(fn.input.file(file)))
  setnames(data.dt, tolower(colnames(data.dt)))
  if (!is.null(key)) {
    setkeyv(data.dt, key)
  }
  data.dt
}


#############################################################
# error evaluation
#############################################################
fn.print.auc.err <- function(actual, pred, do.print = T) { 
  
  if (is.data.frame(actual)) {
    actual <- actual$target
  }
  
  if (is.data.frame(pred)) {
    pred <- pred$pred
  }
  
  if (is.null(actual) || all(is.na(actual))) {
    df <- summary(pred)
  } else {
    library("Metrics")
    df <- data.frame(Length = length(actual),
                     AUC = auc(actual,pred))
  }
  
  
  if (do.print) {
    print(df)
  }
  
  invisible(df)
}
# debug(fn.print.err)

#############################################################
# extract prediction
#############################################################
fn.extract.pred <- function(data.all, data.type) {
  #   data.extracted <- data.all
  library("data.table")
  data.extracted <- data.all[data.all$datatype == data.type, ]
  data.extracted <- data.table(data.extracted)
  if (is.null(data.extracted$paperid)) {
    data.extracted <- data.frame(data.extracted[
      ,list(
        authorid = unique(authorid),
        pred = mean(pred)), by="test.idx"])
  } else {
    data.extracted <- data.frame(data.extracted[
      ,list(
        paperid = unique(paperid),
        authorid = unique(authorid),
        pred = mean(pred)), by="test.idx"])
  }
  
  data.extracted[data.extracted$test.idx,] <- 
    data.extracted
  rownames(data.extracted) <- 1:nrow(data.extracted)
  
  cols.out <- colnames(data.extracted)[!(
    colnames(data.extracted) %in% c("test.idx", "datatype"))]
  
  data.extracted[, cols.out, drop = F]
}

fn.extract.tr <- function(data.all) {
  fn.extract.pred(data.all, "tr")
}

fn.extract.test <- function(data.all) {
  fn.extract.pred(data.all, "test")
}

#############################################################
# probabilities to factors
#############################################################
fn.write.submission <- function(pred, file.name) { 
  if (!is.null(pred$pred)) {
    pred <- fn.build.submission(pred)
  }
  
  pred <- data.frame(pred)
  write.csv(pred,
            file = paste0("submission/", file.name,".csv"), 
            row.names = F, quote = F)
}
# debug(fn.write.submission)

#############################################################
# build submission array
#############################################################
fn.print.map <- function(actual, pred, do.print = T) {

  library("Metrics")
  library("data.table")
  
  actual <- data.table(actual, key = c("authorid", "paperid"))
  
  if (!is.null(actual$target)) {
    setnames(actual, "target", "confirmed")
  }

  #actual <- unique(actual)
  actual <- actual[actual$authorid %in% pred$authorid,]
  actual <- actual[actual$confirmed == 1,]
  setkeyv(actual, "authorid")
  pred <- data.table(pred)
  setkeyv(pred, "authorid")
  
  scores <- NULL
  for (aid in sort(unique(actual$authorid))) {
    actual.papers <- actual[J(aid)]$paperid
    pred.papers <- pred[J(aid)]
    pred.papers <- head(pred.papers$paperid[order(-pred.papers$pred)], 
                        n = map.n)
    scores <- c(scores, 
               apk(map.n,
                   actual.papers,
                   pred.papers))
  }
  
  data.err <- data.frame(
    size = length(scores),
    map = mean(scores))
  
  if (do.print) {
    print(data.err)
  }
  invisible(data.err)
}
# debug(fn.print.map)

#############################################################
# build submission array
#############################################################
fn.print.sort.author.map <- function(actual, pred) {

  library("Metrics")
  library("data.table")

  actual <- unique(actual)
#   actual <- actual[actual$authorid %in% pred$authorid,]
  data.joined <- data.table(fn.join(pred, actual))
  data.joined <- data.joined[!is.na(data.joined$confirmed),]
  
  data.score <- suppressWarnings(data.joined[
    ,list(score = apk(map.n,
                      paperid[confirmed==1],
                      head(paperid[order(-pred)], n = map.n)),
          confirm.count = sum(confirmed),
          delete.count = sum(1-confirmed),
          pred.med = median(pred),
          pred.avg = mean(pred),
          pred.max = max(pred),
          pred.sd = sd(pred),
          max.gap = max(sort(pred)[-1] - sort(pred)[-length(pred)])),
    by="authorid"])
  data.score <- data.score[data.score$confirm.count > 0,]
  data.score <- data.score[order(data.score$score)]
  invisible(data.score)
}

#############################################################
# build submission array
#############################################################
fn.stats.map <- function(pred) {
  
  library("Metrics")
  library("data.table")
  
  data.stats <- data.table(pred)
  
  data.stats <- suppressWarnings(data.stats[
    ,list(
      pred.med = median(pred),
      pred.avg = mean(pred),
      pred.max = max(pred),
      pred.sd = sd(pred),
      max.gap = max(sort(pred)[-1] - sort(pred)[-length(pred)])),
    by="authorid"])
  invisible(data.stats)
}

#############################################################
# build submission array
#############################################################
fn.build.submission <- function(data.pred) {
  
  library("data.table")
  data.pred <- data.table(data.pred) 
  data.pred <- data.pred[
    ,list(
      paperids = paste(head(unique(paperid[order(-pred)]), n = map.n), collapse = " ")), 
    by="authorid"]
  data.pred <- data.pred[order(data.pred$authorid),]
  data.pred
}

#############################################################
# join data tables
#############################################################
fn.join <- function(df1, df2) {
  cols.key <- key(df2)
  if (is.data.table(df1)) {
    data.key <- df1[,cols.key,with=F]
  } else {
    data.key <- data.table(df1[,cols.key,drop=F])
  }
  df2.join <- df2[data.key,allow.cartesian=TRUE]
  df2.join <- df2.join[
    ,!(colnames(df2.join) %in% cols.key),with=F]
  cbind(df1, df2.join)
}
# debug(fn.join)

#############################################################
# join data tables - overwrite existing cols
#############################################################
fn.join2 <- function(df1, df2) {
  cols.key <- key(df2)
  if (is.data.table(df1)) {
    data.key <- df1[,cols.key,with=F]
    df1 <- data.table(df1, key = key(df1))
  } else {
    data.key <- data.table(df1[,cols.key,drop=F])
    df1 <- data.frame(df1)
  }
  df2.join <- df2[data.key,allow.cartesian=TRUE]
  df2.join <- df2.join[
    ,!(colnames(df2.join) %in% cols.key),with=F]
  cols.overwrite <- colnames(df2.join)[
    colnames(df2.join) %in% colnames(df1)]
  for (col.name in cols.overwrite) {
    df1[[col.name]] <- NULL
  }
  cbind(df1, df2.join)
}
# debug(fn.join2)

##############################################################
## tranform target of prediction
##############################################################
fn.target.to.xor <- function(data.df, col, thres) {
  thres.rng <- data.df[[col]] > thres
  target <- data.df$target == 1
  as.integer((thres.rng & !target) | (!thres.rng & target))
}
# debug(fn.target.to.xor)

fn.target.from.xor <- function(pred, data.df, col, thres) {
  thres.rng <- data.df[[col]] > thres
  
  new.pred <- pred
  new.pred[thres.rng] <-  1 - new.pred[thres.rng]
  new.pred
}

fn.extra.col.xor <- function(data.df, col, thres) {
  as.factor(as.integer(data.df[[col]] > thres))
}

##############################################################
## remove id cols
##############################################################
fn.remove.ids <- function(data.df) {
  col.ids <- which(colnames(data.df) %in% c("authorid", "paperid"))
  data.df[,-col.ids]
}

##############################################################
## get id cols
##############################################################
fn.get.ids <- function(data.df) {
  data.df[,c("authorid", "paperid")]
}

#############################################################
# print rf importance
#############################################################
fn.rf.print.imp <- function(rf) {
  invisible(try(print(rf$importance[order(-rf$importance[,1]),]), 
      silent = T))
}

##################################################
# write libsvm data
##################################################
fn.write.libsvm <- function(
  data.tr, 
  data.test, 
  name,
  fn.y.transf = NULL, 
  dir = "libsvm",
  col.y = "target",
  col.x = colnames(data.tr)[!(colnames(data.tr) %in% c(col.y, 
                                                       "authorid", 
                                                       "paperid"))],
  feat.start = 0)
{
  options(scipen=999)
  library("data.table")
  cat("Building feature map ...")
  tic()
  model.dts <- list()
  val.xi <- feat.start
  col.x.groups <- NULL
  feat.size <- 0
  for (i in (1:length(col.x))) {
    col <- col.x[i]
    if (is.factor(data.tr[[col]])) {
      
      col.ids.tr   <- as.factor(levels(data.tr[[col]]))
      model.dts[[col]] <- data.table(ID = col.ids.tr, key = "ID")
      if (!is.null(data.test)) {
        col.ids.test <- as.factor(levels(data.test[[col]]))
        model.dts[[col]] <- merge(model.dts[[col]], 
                                  data.table(ID = col.ids.test, key = "ID"))
      }
      feat.size <- feat.size + nrow(model.dts[[col]])
      
      model.dts[[col]]$X.Idx <- val.xi:(val.xi+nrow(model.dts[[col]])-1)
      
      val.xi <- val.xi+nrow(model.dts[[col]])
      
    } else {
      model.dts[[col]] <- val.xi
      val.xi <- val.xi + 1
    }
  }
  map.name <- paste(name, ".map", sep="")
  assign(map.name, model.dts)
  save(list = map.name, file = paste(dir, "/", map.name, ".RData", sep=""))
  cat("done \n")
  toc()
  
  if (!exists("cmpfun")) {
    cmpfun <- identity
  }
  write.file <- cmpfun(function (data, file) { 
    tic()
    col.chunk <- col.x
    if (!is.null(data[[col.y]])) {
      col.chunk <- c(col.y, col.x)
    }
    unlink(file)
    cat("Saving ", file, "...")
    fileConn <- file(file, open="at")
    
    data.chunk <- data[, col.chunk]
    if (is.null(data.chunk[[col.y]])) {
      data.chunk[[col.y]] <- 0
    }
    data.chunk[[col.y]][is.na(data.chunk[[col.y]])] <- 0
    if (!is.null(fn.y.transf)) {
      data.chunk[[col.y]] <- fn.y.transf(data.chunk[[col.y]])
    }
    data.chunk[[col.y]][data.chunk[[col.y]] == Inf] <- 0
    
    for (col in col.x) {
      if (is.numeric(data.chunk[[col]])) {
        data.chunk[[col]][data.chunk[[col]] == 0] <- NA
      }
    }
    
    for (col in col.x) {
      if (is.factor(data.chunk[[col]])) {
        data.chunk[[col]] <-  paste(
          model.dts[[col]][J(data.chunk[[col]])]$X.Idx,
          c(1), sep = ":")
      } else {
        data.chunk[[col]] <- paste(
          rep(model.dts[[col]], nrow(data.chunk)),
          data.chunk[[col]], sep = ":")
      }
    }
    
    data.chunk <- do.call(paste, data.chunk[, c(col.y, col.x)])
    chunk.size <- as.numeric(object.size(data.chunk))
    chunk.size.ch <- T
    while (chunk.size.ch) {
      data.chunk <- gsub(" [0-9]+\\:?NA", "", data.chunk)
      data.chunk <- gsub(" NA\\:-?[0-9]+", "", data.chunk)
      chunk.size.ch <- chunk.size != as.numeric(object.size(data.chunk))
      chunk.size <- as.numeric(object.size(data.chunk))
    }
    #     data.chunk <- gsub("^([0-9]+(\\.[0-9]+)?)\\s+", "\\1 | ", data.chunk)
    
    writeLines(c(data.chunk), fileConn)
    
    close(fileConn)
    cat("done.\n")
    toc()
  })
  #   debug(write.file)
  write.file(data.tr, paste(dir, "/", name, ".tr.libsvm", sep=""))
  if (!is.null(data.test)) {
    write.file(data.test, paste(dir, "/", name, ".test.libsvm", sep=""))
  }
}
# debug(fn.write.libsvm)

##################################################
# expand factors
##################################################
fn.expand.factors <- function(data.df) {
  data.frame(model.matrix( ~ . - 1, data=data.df ))
}

##############################################################
## calculates confirm status for other authors
##############################################################
fn.build.confirm.stats <- function(cv.folds, df, 
                                   stats.cols,
                                   fn.weight = function (x) log(1+x)) {
  library("data.table")
  df.new <- data.frame(df)
  if ("confirmed" %in% colnames(df.new)) {
    setnames(df.new, "confirmed", "target")
  } 
  for (stats.col in stats.cols ) {
    df.new[[stats.col]][is.na(df.new[[stats.col]])] <- -1
  }
  df.new$target[is.na(df.new$target)] <- -1
  
  col.pos <- paste(paste(stats.cols, collapse = "_"), "other_pos", sep = "_")
  col.neg <- paste(paste(stats.cols, collapse = "_"), "other_neg", sep = "_")
  
  data.stats.by.author.col.cv <- NULL
  
  fn.register.wk()
  data.stats.by.author.col.cv <- foreach(k=1:(cv.folds$K+1),.combine=rbind, 
                                         .export = c(lsf.str(.GlobalEnv))) %dopar% {
                                           #   for (k in 1:(cv.folds$K+1)) {
                                           library("data.table")
                                           library("cvTools")
                                           data.stats <- data.table(df.new)
                                           data.stats$select <- F
                                           
                                           
                                           if (k <= cv.folds$K) {
                                             data.stats$select <- fn.cv.which(cv.folds,data.stats,k)
                                             data.stats$target[data.stats$select] <- -1
                                           } else {
                                             data.stats$select <- data.stats$target == -1
                                           }
                                           
                                           data.stats.by.author.col <- data.stats[
                                             ,list(aut.pos = sum(target == 1)/sum(target >= 0),
                                                   aut.neg = sum(target == 0)/sum(target >= 0),
                                                   aut.weight =  fn.weight(sum(target >= 0)),
                                                   select = select[1]),
                                              by = c("authorid", stats.cols)]
                                           data.stats.by.author.col$aut.pos <- 
                                             data.stats.by.author.col$aut.pos*data.stats.by.author.col$aut.weight
                                           data.stats.by.author.col$aut.neg <- 
                                             data.stats.by.author.col$aut.neg*data.stats.by.author.col$aut.weight
                                           data.stats.by.author.col$aut.pos[is.na(data.stats.by.author.col$aut.pos)] <- 0
                                           data.stats.by.author.col$aut.neg[is.na(data.stats.by.author.col$aut.neg)] <- 0
                                           
                                           data.stats.by.col <- data.stats.by.author.col[
                                             ,list(col.pos = sum(aut.pos, na.rm = T),
                                                   col.neg = sum(aut.neg, na.rm = T)),
                                              by = stats.cols]
                                           setkeyv(data.stats.by.col, stats.cols)
                                           
                                           data.stats.by.author.col <- fn.join(data.stats.by.author.col,
                                                                               data.stats.by.col)
                                           data.stats.by.author.col$other.pos <- 
                                             data.stats.by.author.col$col.pos - 
                                             data.stats.by.author.col$aut.pos
                                           
                                           data.stats.by.author.col$other.neg <- 
                                             data.stats.by.author.col$col.neg - 
                                             data.stats.by.author.col$aut.neg
                                           
                                           data.stats.by.author.col$type <- ifelse(k <= cv.folds$K, "tr", "test")
                                           data.stats.by.author.col <-
                                             data.stats.by.author.col[
                                               ,c("authorid", stats.cols, "other.pos", "other.neg", 
                                                  "select", "type"),
                                                with = F]
                                           
                                           setnames(data.stats.by.author.col, "other.pos", col.pos)
                                           setnames(data.stats.by.author.col, "other.neg", col.neg)
                                           
                                           #     data.stats.by.author.col.cv <- rbind(
                                           #       data.stats.by.author.col.cv,
                                           #       data.stats.by.author.col[data.stats.by.author.col$select,])
                                           
                                           data.stats.by.author.col[data.stats.by.author.col$select,]
                                         }
  fn.kill.wk()
  
  data.stats.by.author.col <- data.stats.by.author.col.cv
  data.stats.by.author.col$select <- NULL
  setkeyv(data.stats.by.author.col, c("authorid", stats.cols))
  
  df.new <- fn.join(df.new, data.stats.by.author.col)
  df.new <- df.new[,c("authorid", "paperid", col.pos, col.neg, "type"),]
  df.new <- data.table(df.new, key = c("authorid", "paperid"))
  df.new
}
# debug(fn.build.confirm.stats)

##############################################################
## Training lmer function function
##############################################################
fn.train.lmer <-  function(formula,
                           data,
                           family = binomial(),
                           weights = NULL,
                           verbose = T,
                           ...) {
  library("lme4")
  library("data.table")
  
  # model object
  model <- list();
  model$formula <- formula;
  model$family <- family;
  
  
  # Estimate LMER
  lme.model = glmer(
    formula=formula,
    data=data,
    family = family,
    weights = weights,
    verbose = verbose);
  
  # get the constant term
  model$fixef <- fixef(lme.model)
  model$ranef <- ranef(lme.model)
  
  model$const <- as.numeric(model$fixef)
  
  model$dt <- list();
  model.names <- names(model$ranef)
  for (name in model.names) {
    vals <- model$ranef[[name]];
    model$dt[[name]] <- data.table(
      as.numeric(rownames(vals)),
      vals[,])
    col.n <- NCOL(vals); 
    col.values <- paste(rep("V", col.n), c(1:col.n), sep = "")
    setnames(model$dt[[name]], c(name, col.values))
    setkeyv(model$dt[[name]], name)
  }
  gc()
  
  model$predict <- function(data) {
    library("data.table")
    pred <- rep(model$const, nrow(data))
    for (name in model.names) {
      pred.key <- data.table(data[[name]])
      pred.cur <- model$dt[[name]][pred.key]$V1
      pred.cur[is.na(pred.cur)] <- 0
      pred <- pred + pred.cur
    }
    return (model$family$linkinv(pred));
  }
  #   debug(model$predict)
  
  invisible (model)
};
# debug(fn.train.lmer)

##############################################################
## creates likelihood feature
##############################################################
fn.log.likelihood <- function(feats.stats, 
                              family = "gaussian",
                              type.keep = NULL) {
  
  col.pos <- colnames(feats.stats)[grepl("other_pos", colnames(feats.stats))]
  col.neg <- colnames(feats.stats)[grepl("other_neg", colnames(feats.stats))]
  feats.stats.df <- data.frame(feats.stats)
  feats.stats.df$sum_pos_neg <- feats.stats.df[[col.pos]] + 
    feats.stats.df[[col.neg]]
  feats.stats.df$pos_prob <- 
    feats.stats.df[[col.pos]]/feats.stats.df$sum_pos_neg
  feats.stats.df$pos_prob[is.na(feats.stats.df$pos_prob)] <- 0
  feats.stats.df$neg_prob <- 
    feats.stats.df[[col.neg]]/feats.stats.df$sum_pos_neg
  feats.stats.df$neg_prob[is.na(feats.stats.df$neg_prob)] <- 0
  
  library("data.table")
  feats.stats.df.key <- unique(data.table(feats.stats.df[
    ,c(col.pos, col.neg, "sum_pos_neg", "pos_prob", "neg_prob")]))
  setkeyv(feats.stats.df.key, c(col.pos, col.neg, "sum_pos_neg"))
  feats.stats.df.key$id <- 1:nrow(feats.stats.df.key)
  
  if (family == "gaussian") {
    df.all <- data.frame(
      target = feats.stats.df.key$pos_prob,
      id = feats.stats.df.key$id,
      weights = feats.stats.df.key$sum_pos_neg)
    df.all <- rbind(df.all,
                    data.frame(target = c(0,1), 
                               id = c(-1,-1),
                               weights = c(1,1)))
    df.lmer <- df.all[df.all$weights > 0 ,]
    lmer.model <- fn.train.lmer(formula=target ~ 1 + (1|id),
                                data = df.lmer,
                                weights = weights,
                                family = gaussian(),
                                verbose = T)
  }
  
  if (family == "binomial") {
    df.all.pos <- data.frame(
      target = 1,
      id = feats.stats.df.key$id,
      weights = 
        feats.stats.df.key$pos_prob*
        feats.stats.df.key$sum_pos_neg)
    df.all.neg <- data.frame(
      target = 0,
      id = feats.stats.df.key$id,
      weights = 
        feats.stats.df.key$neg_prob*
        feats.stats.df.key$sum_pos_neg)
    
    df.lmer <- rbind(df.all.pos[df.all.pos$weights > 0,],
                     df.all.neg[df.all.neg$weights > 0,])
    
    #   cat("Training ", col.pos, "likelihood model: \n")
    lmer.model <- suppressWarnings(
      fn.train.lmer(formula=target ~ 1 + (1|id),
                    data = df.lmer,
                    weights = weights,
                    family = binomial(),
                    verbose = T))
  }
  
  col.likelihood <- sub("other_pos", "prob", col.pos)
  feats.stats.df.key[[col.likelihood]] <- lmer.model$predict(data.frame(feats.stats.df.key))
  feats.stats.df <- fn.join(feats.stats.df, feats.stats.df.key)
  feats.stats.df <- feats.stats.df[, c("authorid", "paperid", col.likelihood)]
  
  
  result.dt <- data.table(feats.stats.df, key = c("authorid", "paperid"))
  
  if (!is.null(type.keep)) {
    feats.stats.type <- feats.stats[, c("authorid", "paperid", "type"), with = F]
    result.dt <- fn.join(result.dt, feats.stats.type)
    result.dt <- result.dt[result.dt$type == type.keep,]
    result.dt$type <- NULL
  }
  
  if (!is.null(result.dt$type)) {
    result.dt$type <- NULL
  }
  
  result.dt
}
# debug(fn.log.likelihood)

##############################################################
## calculates confirm status for other authors 
## leave one out
##############################################################
fn.build.confirm.stats2 <- function(df, 
                                    stats.cols,
                                    fn.weight = function (x) log(1+x)) {
  library("data.table")
  df.new <- data.frame(df)
  if ("confirmed" %in% colnames(df.new)) {
    setnames(df.new, "confirmed", "target")
  } 
  for (stats.col in stats.cols ) {
    df.new[[stats.col]][is.na(df.new[[stats.col]])] <- -1
  }
  df.new$target[is.na(df.new$target)] <- -1
  
  col.pos <- paste(paste(stats.cols, collapse = "_"), "other_pos", sep = "_")
  col.neg <- paste(paste(stats.cols, collapse = "_"), "other_neg", sep = "_")
  
  data.stats.author <- data.table(df.new)[
      ,list(aut.pos = sum(target == 1),
            aut.neg = sum(target == 0)),
      by = c("authorid", stats.cols)]
  setkeyv(data.stats.author, c("authorid", stats.cols))
  
  data.stats.all <- data.table(df.new)[
      ,list(all.pos = sum(target == 1),
            all.neg = sum(target == 0)),
      by = c(stats.cols)]
  setkeyv(data.stats.all, c(stats.cols))
  
  df.stats <- df.new[,c("authorid", "paperid", stats.cols)]
  df.stats <- fn.join(df.stats, data.stats.author)
  df.stats <- fn.join(df.stats, data.stats.all)
  df.stats$col.pos <- df.stats$all.pos - df.stats$aut.pos
  df.stats$col.neg <- df.stats$all.neg - df.stats$aut.neg
  df.stats$col.n <- df.stats$col.pos + df.stats$col.neg
  df.stats$col.weight <- fn.weight(df.stats$col.pos + df.stats$col.neg)
  df.stats$type <- "tr"
  df.stats$type[(df.stats$aut.pos+df.stats$aut.neg) == 0] <- "test"
  
  has.weight <- which(df.stats$col.n > 0)
  
  df.stats[[col.pos]] <- 0
  df.stats[[col.pos]][has.weight] <- df.stats$col.weight[has.weight]*
    (df.stats$col.pos[has.weight]/df.stats$col.n[has.weight])
  
  df.stats[[col.neg]] <- 0
  df.stats[[col.neg]][has.weight] <- df.stats$col.weight[has.weight]*
    (df.stats$col.neg[has.weight]/df.stats$col.n[has.weight])
  
  df.result <- df.stats[,c("authorid", "paperid", col.pos, col.neg, "type")]
  data.table(df.result, key = c("authorid", "paperid"))
}

# debug(fn.build.confirm.stats2)

##############################################################
## creates likelihood feature
##############################################################
fn.log.likelihood <- function(feats.stats, type.keep = NULL) {
  
  col.pos <- colnames(feats.stats)[grepl("other_pos", colnames(feats.stats))]
  col.neg <- colnames(feats.stats)[grepl("other_neg", colnames(feats.stats))]
  feats.stats.df <- data.frame(feats.stats)
  feats.stats.df$sum_pos_neg <- feats.stats.df[[col.pos]] + 
    feats.stats.df[[col.neg]]
  
  library("data.table")
  feats.stats.df.key <- unique(data.table(feats.stats.df[
    ,c(col.pos, col.neg, "sum_pos_neg")]))
  setkeyv(feats.stats.df.key, c(col.pos, col.neg, "sum_pos_neg"))
  feats.stats.df.key$id <- 1:nrow(feats.stats.df.key)
  
  df.all <- data.frame(target = feats.stats.df.key[[col.pos]]/
                         feats.stats.df.key$sum_pos_neg,
                       id = feats.stats.df.key$id,
                       weights = feats.stats.df.key$sum_pos_neg)
  df.all <- df.all[df.all$weights > 0 ,]
  df.all <- rbind(df.all,
                  data.frame(target = c(0,1), 
                             id = c(-1,-1),
                             weights = c(1,1)))
  
  df.lmer <- df.all
  
#   cat("Training ", col.pos, "likelihood model: \n")
  lmer.model <- fn.train.lmer(formula=target ~ 1 + (1|id),
                              data = df.lmer,
                              weights = weights,
                              family = gaussian(),
                              verbose = T)
  
  col.likelihood <- sub("other_pos", "prob", col.pos)
  feats.stats.df.key[[col.likelihood]] <- lmer.model$predict(data.frame(feats.stats.df.key))
  feats.stats.df <- fn.join(feats.stats.df, feats.stats.df.key)
  feats.stats.df <- feats.stats.df[, c("authorid", "paperid", col.likelihood)]
  
  
  result.dt <- data.table(feats.stats.df, key = c("authorid", "paperid"))
  
  if (!is.null(type.keep)) {
    feats.stats.type <- feats.stats[, c("authorid", "paperid", "type"), with = F]
    result.dt <- fn.join(result.dt, feats.stats.type)
    result.dt <- result.dt[result.dt$type == type.keep,]
    result.dt$type <- NULL
  }
  
  result.dt
}
# debug(fn.log.likelihood)

##############################################################
## prints likelihood status
##############################################################
fn.log.likelihood.print <- function(data.train.ids, feats.stats) {
  col.likelihood <- tail(colnames(feats.stats), n=1)
  data.err <- fn.print.map(data.train.ids, data.frame(feats.stats, 
                                          pred = feats.stats[[col.likelihood]]))
  tr.idx <- feats.stats$authorid %in% data.train.ids$authorid
#   cat("Tr: ", sum(tr.idx), " Test: ", sum(!tr.idx), "\n")
  if (any(tr.idx)) {
    print(summary(data.table(
      train = feats.stats[[col.likelihood]][tr.idx])))
  }
  if (any(!tr.idx)) {
    print(summary(data.table(
      test = feats.stats[[col.likelihood]][!tr.idx])))
  }
  
  invisible(data.err)
}

##############################################################
## choose text field
##############################################################
fn.choose.txt <- function(field) {
  if (length(field)==1) return (field[1])
  ix <- which(nchar(field)>0)
  if (length(ix) == 0) {
    return ("")
  } else {
    return (field[ix][which.min(nchar(field[ix]))[1]])
  }
}

##############################################################
## creates likelihood feature
##############################################################
fn.clean.paper.author <- function(paper.author.dt) {
  library("data.table")
  dt_pa_unique <- paper.author.dt[,list(
    pa_name = fn.choose.txt(pa_name),
    pa_affiliation = fn.choose.txt(pa_affiliation)
  ),by=c("authorid","paperid")]
  setkeyv(dt_pa_unique, c("authorid","paperid"))
  dt_pa_unique
}

##############################################################
## creates likelihood feature
##############################################################
fn.cv.which <- function(cv.data, df, k) {
  library("data.table")
  if (is.data.table(df)) {
    which.key <- df[,key(cv.data$which),with=F]
  } else {
    which.key <- data.table(df[,key(cv.data$which)])
  }
  
  cv.k <- cv.data$which[which.key]$k
  cv.choose <- rep(F, length(cv.k))
  cv.choose[which(cv.k == k)] <- T
  cv.choose
}

#############################################################
# create normalized data frame
#############################################################
fn.ens.df <- function(df.name, 
                      ens.names = c(
                        "gbm.dtry.t1"),
                      normalize = F) {
  library("data.table")
  df.base.name <- paste0("data.", df.name, ".dtry")
  fn.load.data(df.base.name)
  data.ens <- get(df.base.name)[,c("authorid", "paperid", "target")]
  
  for (ens.name in ens.names) {
    model.name <- paste0("data.", df.name, ".", ens.name)
    fn.load.data(model.name)
    
    tmp <- data.table(
      authorid = data.ens$authorid,
      paperid = data.ens$paperid,
      pred = get(model.name)$pred)
    
    if (normalize) {
      tmp <- tmp[, list(
        paperid = paperid,
        pred = ((pred - min(pred))/(max(pred) - min(pred))) - 0.5), 
                 by="authorid" ]
      tmp$pred[is.na(tmp$pred)] <- 1
    }
    
    
    setkeyv(tmp, c("authorid", "paperid"))
    data.ens[[ens.name]] <- tmp[J(data.ens$paperid, data.ens$authorid)]$pred
  }
  
  data.ens
}

##################################################
# expand factors
##################################################
fn.expand.factors <- function(data.df) {
  data.frame(model.matrix( ~ . - 1, data=data.df))
}

##############################################################
## get median or mode
##############################################################
fn.median.or.mode <- function(x) {
  if (is.factor(x)) {
    fn.mode(x)
  } else {
    median(x)
  }
}

##############################################################
## get mode
##############################################################
fn.mode <- function(x) {
  x.tbl <- table(as.vector(x))
  x.vals <- names(x.tbl)[x.tbl == max(x.tbl)][1]
  factor(x.vals, levels(x))
}

##############################################################
## read nearest cluster data from sofia-ml
##############################################################
fn.read.sofiaml.clust <- function(file.name, col.key, col.clust) {
  clust.nearest <- read.table(
    fn.sofiaml.out(file.name), 
    head = F, sep="\t")
  clust.nearest[,1:2] <- clust.nearest[,2:1]
  setnames(clust.nearest, 
           c(col.key, paste0(col.clust)))
  clust.nearest[[2]] <- factor(clust.nearest[[2]]+1)
  data.table(clust.nearest, key = col.key)
}

##############################################################
## read cluster coordinates data from sofia-ml
##############################################################
fn.read.sofiaml.coord <- function(file.name, col.key, col.suffix) {
  clust.coord <- read.csv(fn.sofiaml.out(file.name), head = F)
  setnames(clust.coord, 
           c(col.key, paste0(col.suffix, 1:(ncol(clust.coord)-1))))
  data.table(clust.coord, key = col.key)
}

##############################################################
## read cluster coordinates data from sofia-ml
##############################################################
fn.merge.low.freq <- function(val, low.thres = 150) {
  val <- as.character(val)
  val.tbl <- table(val)
  val.merge <- names(val.tbl)[val.tbl < low.thres]
  val[val %in% val.merge] <- "-1"
  factor(val)
}

##############################################################
## get with default value
##############################################################
fn.get <- function(var.name, def.val = NULL) {
  if ( exists(var.name)) {
    return(get(var.name))
  } else {
    return(def.val)
  }
}

##############################################################
## build cv
##############################################################
fn.build.cv <- function(train.ids, K = 5, seed = 3847569) {
  library("cvTools")
  library("data.table")
  set.seed(seed)
  author.unique <- sort(unique(train.ids$authorid))
  data.cv.folds.raw <- cvFolds(length(author.unique), K = K)
  data.train.ids.unique <- unique(data.frame(train.ids)[
    ,c("authorid", "paperid")])
  data.cv.folds <- rep(-1, nrow(data.train.ids.unique))
  for (k in 1:data.cv.folds.raw$K) {
    author.fold <- author.unique[data.cv.folds.raw$which == k]
    data.cv.folds[data.train.ids.unique$authorid %in% author.fold] <- k
  }
  data.cv.folds <- list(
    K = data.cv.folds.raw$K, 
    which = data.table(data.train.ids.unique, k = data.cv.folds))
  cat("Instance CV distribution: \n")
  print(table(data.cv.folds$which$k))
  
  data.cv.folds$which <- unique(data.cv.folds$which[
    ,c("authorid", "k"), with = F])
  setkeyv(data.cv.folds$which, c("authorid"))
  
  cat("Author CV distribution: \n")
  print(table(data.cv.folds$which$k))
  data.cv.folds
}

##############################################################
## build data load features
##############################################################
fn.build.load.feat <- function(data.ids) {
  library("data.table")
  data.ids <- data.table(data.ids)
  if (is.null(data.ids$confirmed)) {
    data.ids$confirmed <- -1
  }
  data.ids.feat <- data.ids[
    ,list(
      lf_countsource = length(confirmed)
    ), by = c("authorid", "paperid")]
  setkeyv(data.ids.feat, c("authorid", "paperid"))
  
  data.ids.feat
}

# debug(fn.build.load.feat)

##############################################################
## join data load features
##############################################################
fn.join.load.feat <- function(data.df, data.load.feat) {
  library("data.table")
  data.dt <- data.table(data.df, 
                        row.idx = 1:nrow(data.df),
                        key = key(data.load.feat))
  data.dt <- fn.join(data.load.feat, data.dt)
  data.dt <- data.frame(data.dt)
  data.dt <- data.dt[
    ,unique(c(colnames(data.df), colnames(data.load.feat), "row.idx"))]
  data.dt$target <- data.dt$lf_confirmed
  data.dt$lf_confirmed <- NULL
  data.dt[data.dt$row.idx,] <- data.dt
  data.dt$row.idx <- NULL
  
  data.dt
}

##############################################################
## join data load features
##############################################################
fn.rbind <- function(...) {
  df.all <- rbind(...)
  df.1 <- list(...)[[1]]
  if ("data.table" %in% class(df.1)) {
    library("data.table")
    if (!is.null(key(df.1))) {
      setkeyv(df.all, key(df.1))
    }
  }
  df.all
}

#############################################################
# Dmitry's utils.R
#############################################################
library(rjson)
library(RPostgreSQL)
library(data.table)
library(hexbin)
library(gbm)
library(tm)
stopwords_web <- c('www','html','com','org','journal','journals','index','openurl','url','htm','shtml','php')

fn.print.map.err <- function(actual, pred, do.print = T) { 
  actual <- data.table(actual)
  pred <- data.table(pred)
  actual.pred <- merge(actual[,list(authorid,paperid,target)],pred[,list(authorid,paperid,pred)],by=c("authorid","paperid"))
  actual.pred <- data.frame(actual.pred)
  authors.unique <- unique(actual.pred$authorid)
  actual.list <- list()
  predicted.list <- list()
  apks <- c()
  for (j in 1:length(authors.unique)) {
    author <- authors.unique[j]
    ix <- which(actual.pred$authorid==author)
    actual.j <- as.numeric(actual.pred$paperid[which(actual.pred$authorid==author & actual.pred$target==1)])
    predicted.j <- as.numeric(actual.pred$paperid[ix][sort(actual.pred$pred[ix],decreasing=TRUE,index.return=TRUE)$ix])
    actual.list[[j]] <- actual.j
    predicted.list[[j]] <- predicted.j
    apks <- c(apks,apk_m(actual.j,predicted.j))
  }
  df <- data.frame(Length = nrow(actual.pred),
                   MAPerror = mapk_m(actual.list,predicted.list))
  
  if (do.print) {
    print (df)
  }
  
  invisible(df)
}

apk_m <- function(actual, predicted) {
  score <- 0.0
  cnt <- 0.0
  for (i in 1:length(predicted)) {
    if (predicted[i] %in% actual && !(predicted[i] %in% predicted[0:(i-1)])) {
      cnt <- cnt + 1
      score <- score + cnt/i
    }
  }
  score <- score / length(actual)
  return (score)
}

mapk_m <- function (actual, predicted) {
  scores <- rep(0, length(actual))
  for (i in 1:length(scores)){
    scores[i] <- apk_m(actual[[i]], predicted[[i]])
  }
  score <- mean(scores)
  return (score)
}

cleanTextField <- function(field) {
  field1 <- tolower(gsub("[^A-Za-z0-9 _]"," ",field))
  field1 <- gsub(" . |^. | .$"," ",field1)
  field1 <- gsub("^ +","",field1)
  field1 <- gsub(" +$","",field1)
  field1 <- gsub(" +"," ",field1)
  return (field1)
}

cleanWebField <- function(field) {
  field1 <- tolower(gsub("http://","",field))
  field1 <- gsub("/"," ",field1)
  field1 <- gsub("\\."," ",field1)
  field1 <- gsub(" . |^. | .$"," ",field1)
  field1 <- gsub("^ +","",field1)
  field1 <- gsub(" +$","",field1)
  field1 <- gsub(" +"," ",field1)
  return (field1)
}

chooseTextField <- function(field) {
  if (length(field)==1) return (field[1])
  ix <- which(nchar(field)>0)
  if (length(ix) == 0) {
    return ("")
  } else {
    return (field[ix][which.min(nchar(field[ix]))[1]])
  }
}

checkEquals <- function(field1,field2) {
  field1 <- field1[!is.na(field1)]
  field1 <- field1[which(nchar(field1)>0)]
  field2 <- field2[!is.na(field2)]
  field2 <- field2[which(nchar(field2)>0)]
  if (length(intersect(field1,field2))>0) return (1)
  return (0)
}


yearFeature <- function(years) {
  years_sorted <- unique(sort(years))
  years_codes <- unlist(sapply(years,function(x) which(years_sorted==x)))-1
  return (years_codes)
}

distCenter <- function(fea) {
  sds <- apply(fea,2,sd)
  ix <- which(sds!=0)
  fea_scaled <- scale(fea[,ix])
  center <- matrix(rep(colMeans(fea_scaled), nrow(fea_scaled)), byrow=T, ncol=ncol(fea_scaled))
  return (sqrt(rowSums((fea_scaled-center)^2)))
}

export2libfm <- function(df,targ,filename) {
  if (nchar(targ) > 0) {
    df_libfm <- data.frame(target=df[,targ])
  } else {
    df_libfm <- data.frame(target=rep(-1,nrow(df)))
  }
  cols <- setdiff(colnames(df),targ)
  for (i in 1:length(cols)) {
    df_libfm <- cbind(df_libfm, paste(i,":",format(df[,cols[i]],trim=TRUE,scientific=FALSE),sep=""))
  }
  #if (nchar(targ) > 0) {
    write.table(df_libfm,file=filename,quote=FALSE,sep=" ",row.names=FALSE,col.names=FALSE)
  #} else {
  #write.table(df_libfm[,-1],file=filename,quote=FALSE,sep=" ",row.names=FALSE,col.names=FALSE)
  #}
}

export2ranklib <- function(df,targ,groupname,filename) {
  if (nchar(targ) > 0) {
    df_ranklib <- data.frame(target=df[,targ])
  } else {
    df_ranklib <- data.frame(target=rep(-1,nrow(df)))
  }
  df_ranklib[,'group'] <- paste("qid:",format(df[,groupname],trim=TRUE,scientific=FALSE),sep="")

  cols <- setdiff(colnames(df),c(targ,groupname))
  for (i in 1:length(cols)) {
    df_ranklib <- cbind(df_ranklib, paste(i,":",format(df[,cols[i]],trim=TRUE,scientific=FALSE),sep=""))
  }
  write.table(df_ranklib,file=filename,quote=FALSE,sep=" ",row.names=FALSE,col.names=FALSE)
}
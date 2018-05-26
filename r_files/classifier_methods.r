#############################################################
# R script for building multiple models and their evaluation 
#############################################################
library(caret)
library(pROC)

# parallel execution - Windows
#library(doParallel);
#cl <- makeCluster(4); #4 cores
#registerDoParallel(cl) 

# parallel execution - Linux
#library(doMC)
#registerDoMC(cores = 4)

#####################################
# load the dataset
#####################################
#i.i.d assumption
#hdd_iid_df <- read.csv("D:\\NUS\\IVLE\\DATA MINING METHODOLOGY AND METHODS\\CA\\DM\\data\\hdd_iid.csv", header=TRUE)

# reusing same name only; the below file is in time series format!
#EMA
hdd_iid_df <- read.csv("D:\\NUS\\IVLE\\DATA MINING METHODOLOGY AND METHODS\\CA\\DM\\data\\ewa_30.csv", header=TRUE)

# tsfresh
#hdd_iid_df <- read.csv("D:\\NUS\\IVLE\\DATA MINING METHODOLOGY AND METHODS\\CA\\DM\\data\\features_filtered_direct_7days.csv", header=TRUE)


# data type fomatting for target
hdd_iid_df$failure <- as.factor(hdd_iid_df$failure)


#####################################
# split into train and test datasets
#####################################
#split = 0.80
split = 0.70
#split = 0.50

trainIndex <- createDataPartition(hdd_iid_df$failure, p=split, list=FALSE)
hdd_train <- hdd_iid_df[ trainIndex,]
hdd_test <- hdd_iid_df[-trainIndex,]

# create copies for preprocessing step using PCA

# EMA
hdd_train_without_target <- hdd_train[,3:18]
hdd_train_with_target <- hdd_train[,c(1,3:18)] # column 1 is the target 'failure'

# 3 days
#hdd_train_without_target <- hdd_train[,3:855]
#hdd_train_with_target <- hdd_train[,c(1,3:855)] # column 1 is the target 'failure'

# 7 days
#hdd_train_without_target <- hdd_train[,3:1407]
#hdd_train_with_target <- hdd_train[,c(1,3:1407)] # column 1 is the target 'failure'

# 15 days
#hdd_train_without_target <- hdd_train[,3:1966]
#hdd_train_with_target <- hdd_train[,c(1,3:1966)] # column 1 is the target 'failure'

# 30 days
#hdd_train_without_target <- hdd_train[,3:2250]
#hdd_train_with_target <- hdd_train[,c(1,3:2250)] # column 1 is the target 'failure'


##################################
# cross validation train controls
##################################
train_control_kcv <- trainControl(method="cv", number=5, savePredictions = T, preProcOptions=list(thresh = 0.90))
#train_control_loocv <- trainControl(method="LOOCV")


##################################
# test data preparation
##################################
#x_test <- hdd_test[,3:9] #iid
#y_test <- hdd_test[,2] #iid

# EMA
x_test <- hdd_test[,3:18]

# 3 days
#x_test <- hdd_test[,3:855] #temporal

# 7 days
#x_test <- hdd_test[,3:1407] #temporal

# 15 days
#x_test <- hdd_test[,3:1966] #temporal

# 30 days
#x_test <- hdd_test[,3:2250] #temporal

y_test <- hdd_test[,1] #temporal


##################################
# preprocessing steps
##################################
# PCA by limiting variance threshold = 90% yields 48 Principal Components
preProc  <- preProcess(hdd_train_without_target, method = c("center", "scale", "pca"), thresh=0.90)
# Applying the processing steps to test and generating the Principal Components for test from the PC coefficients
x_test <- predict(preProc, x_test)

# this is the final training set with PCs and target.
# After cross validation is verified, the model will be trained again using this dataset.
hdd_train_final <- predict(preProc, hdd_train_without_target)
hdd_train_final$failure <- hdd_train[,1]


##################################
# train using decision tree model
##################################
# cross validation
# k=5 folds
#model_rpart <- train(failure~ R10_smart_5_raw + R10_smart_184_raw + R10_smart_187_raw + R10_smart_188_raw + R10_smart_198_raw + R10_smart_183_raw + R10_smart_7_raw, data=hdd_train, trControl=train_control_kcv, method = "rpart")
#model_rpart <- train(failure~ R10_smart_5_raw + R10_smart_184_raw + R10_smart_187_raw + R10_smart_188_raw + R10_smart_198_raw + R10_smart_183_raw + R10_smart_7_raw, data=hdd_train, trControl=train_control_loocv, method = "rpart")
model_rpart <- train(failure~., data=hdd_train_with_target, trControl=train_control_kcv, method = "rpart", preProcess=c("center", "scale", "pca"))
print(model_rpart)
roc_rpart<- roc(predictor = as.numeric(model_rpart$pred$pred), response = model_rpart$pred$obs, plot=TRUE, auc=TRUE)

# train using the final trainset and make predictions on test data
model_rpart <- train(failure~., data=hdd_train_final, method = "rpart")
predictions_rpart <- predict(model_rpart, x_test)
# summarize results
confusionMatrix(data=predictions_rpart, reference=y_test, positive="1")



##################################
# train using svm
##################################
# cross validation
# k=5 folds
#model_svmLinear <- train(failure~ R10_smart_5_raw + R10_smart_184_raw + R10_smart_187_raw + R10_smart_188_raw + R10_smart_198_raw + R10_smart_183_raw + R10_smart_7_raw, data=hdd_train, trControl=train_control_kcv, method = "svmLinear")
#model_svmLinear <- train(failure~ R10_smart_5_raw + R10_smart_184_raw + R10_smart_187_raw + R10_smart_188_raw + R10_smart_198_raw + R10_smart_183_raw + R10_smart_7_raw, data=hdd_train, trControl=train_control_loocv, method = "svmLinear")
model_svmLinear <- train(failure~., data=hdd_train_with_target, trControl=train_control_kcv, method = "svmLinear")
print(model_svmLinear)
roc_svmLinear<- roc(predictor = as.numeric(model_svmLinear$pred$pred), response = model_svmLinear$pred$obs, plot=TRUE, auc=TRUE)

# train using the final trainset and make predictions on test data
model_svmLinear <- train(failure~., data=hdd_train_final, method = "svmLinear")
predictions_svmLinear <- predict(model_svmLinear, x_test)
# summarize results
confusionMatrix(data=predictions_svmLinear, reference=y_test, positive="1")


##################################
# train using Naive Bayes
##################################
# cross validation
# k=5 folds
#model_nb <- train(failure~ R10_smart_5_raw + R10_smart_184_raw + R10_smart_187_raw + R10_smart_188_raw + R10_smart_198_raw + R10_smart_183_raw + R10_smart_7_raw, data=hdd_train, trControl=train_control_kcv, method = "nb")
#model_nb <- train(failure~ R10_smart_5_raw + R10_smart_184_raw + R10_smart_187_raw + R10_smart_188_raw + R10_smart_198_raw + R10_smart_183_raw + R10_smart_7_raw, data=hdd_train, trControl=train_control_loocv, method = "nb")
model_nb <- train(failure~., data=hdd_train_with_target, trControl=train_control_kcv, method = "nb")
print(model_nb)
roc_nb<- roc(predictor = as.numeric(model_nb$pred$pred), response = model_nb$pred$obs, plot=TRUE, auc=TRUE)

# train using the final trainset and make predictions on test data
model_nb <- train(failure~., data=hdd_train_final, method = "nb")
predictions_nb <- predict(model_nb, x_test)
# summarize results
confusionMatrix(data=predictions_nb, reference=y_test, positive="1")


##################################
# train using Random Forest
##################################
# cross validation
# k=5 folds
# number of trees = 51
ntree = 51
#model_rf <- train(failure~ R10_smart_5_raw + R10_smart_184_raw + R10_smart_187_raw + R10_smart_188_raw + R10_smart_198_raw + R10_smart_183_raw + R10_smart_7_raw, data=hdd_train, method="rf", trControl=train_control_kcv, ntree=ntree)
#model_rf <- train(failure~ R10_smart_5_raw + R10_smart_184_raw + R10_smart_187_raw + R10_smart_188_raw + R10_smart_198_raw + R10_smart_183_raw + R10_smart_7_raw, data=hdd_train, method="rf", trControl=train_control_loocv, ntree=ntree)
model_rf <- train(failure~., data=hdd_train_with_target, method="rf", trControl=train_control_kcv, ntree=ntree)
print(model_rf)
roc_rf<- roc(predictor = as.numeric(model_rf$pred$pred), response = model_rf$pred$obs, plot=TRUE, auc=TRUE)

# train using the final trainset and make predictions on test data
model_rf <- train(failure~., data=hdd_train_final, method = "rf")
predictions_rf <- predict(model_rf, x_test)
# summarize results
confusionMatrix(data=predictions_rf, reference=y_test, positive="1")


##################################
# train using Logistic Regression
##################################
# cross validation
# k=5 folds
#model_logit <- train(failure~ R10_smart_5_raw + R10_smart_184_raw + R10_smart_187_raw + R10_smart_188_raw + R10_smart_198_raw + R10_smart_183_raw + R10_smart_7_raw, data=hdd_train, method="glm", family="binomial", trControl=train_control_kcv)
#model_logit <- train(failure~ R10_smart_5_raw + R10_smart_184_raw + R10_smart_187_raw + R10_smart_188_raw + R10_smart_198_raw + R10_smart_183_raw + R10_smart_7_raw, data=hdd_train, method="glm", family="binomial", trControl=train_control_loocv)
model_logit <- train(failure~., data=hdd_train_with_target, method="glm", family="binomial", trControl=train_control_kcv)
print(model_logit)
roc_logit<- roc(predictor = as.numeric(model_logit$pred$pred), response = model_logit$pred$obs, plot=TRUE, auc=TRUE)

# train using the final trainset and make predictions on test data
model_logit <- train(failure~., data=hdd_train_final, method="glm", family="binomial")
predictions_logit <- predict(model_logit, x_test)
# summarize results
confusionMatrix(data=predictions_logit, reference=y_test, positive="1")



##################################
# train using XGBoost Linear
##################################
# cross validation
# k=5 folds
model_xgb <- train(failure~., data=hdd_train_with_target, method="xgbLinear", trControl=train_control_kcv)
print(model_xgb)
roc_xgb<- roc(predictor = as.numeric(model_xgb$pred$pred), response = model_xgb$pred$obs, plot=TRUE, auc=TRUE)

# train using the final trainset and make predictions on test data
model_xgb <- train(failure~., data=hdd_train_final, method="xgbLinear")
predictions_xgb <- predict(model_xgb, x_test)
# summarize results
confusionMatrix(data=predictions_xgb, reference=y_test, positive="1")


##########################################################
# Plot ROC with Area Under the Curve for all models
##########################################################
plot(roc_rf, col="blue")
plot(roc_logit, add=TRUE, col="red")
plot(roc_rpart, add=TRUE, col="green")
plot(roc_svmLinear, add=TRUE, col="black")
plot(roc_nb, add=TRUE, col="orange")
plot(roc_xgb, add=TRUE, col="yellow")
op <- par(cex = 0.7)
legend("bottomright", legend = c("RF", "Logit", "DecisionTree", "SVM-Linear","NaiveBayes", "XGBoost"), col = c("blue", "red", "green", "black","orange", "yellow"), lty = 1)
legend("topright", legend = c(paste("RF: ", round(roc_rf$auc, digits = 4), sep=" "),paste("Logit: ", round(roc_logit$auc, digits = 4)),paste("DecisionTree: ", round(roc_rpart$auc, digits = 4)),paste("SVM-Linear: ", round(roc_svmLinear$auc, digits = 4)),paste("NB: ", round(roc_nb$auc, digits = 4)), paste("XGBoost: ", round(roc_xgb$auc, digits = 4)), sep=""))


# stop and deregister the parallel execution allocation - Windows
#stopCluster(cl)
#registerDoSEQ()

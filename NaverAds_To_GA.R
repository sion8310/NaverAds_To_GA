library(RCurl)
library(digest)
library(openssl)
library(XML)
library(data.table)
library(httr)
library(plyr)
library(dplyr)
library(jsonlite)
library(googleAnalyticsR)
library(googleAuthR)
library(tidyr)
library(urltools)
library(stringr)



setwd("C:/Users/Sion/Desktop/PZ/MAPPED/UNICEF KR")

#Signature Fucntion
#1. Create Timestamp
#2. Create sha256 hashed parmeter with bytes type.
sig <- function(method, uri, secret_key, customer_id){
  timestamp <- as.character(round(as.numeric(Sys.time()) * 1000))
  message <- paste(timestamp, method, uri, sep = ".")
  hmac_binary = sha256(charToRaw(message), key = charToRaw(SECRET_KEY))
  hmac_encoded = base64Encode(hmac_binary)
  return(as.character(hmac_encoded))
}



########################################################################################################################
#Naver API
BASE_URL = 'https://api.naver.com'
API_KEY = '<API_KEY>'
SECRET_KEY = '<SECRET_KEY>'
CUSTOMER_ID = '<CUSTOMER_ID>'

########################################################################################################################
gar_set_client(json = "<YOUR_GCP_CLIENT>")

gar_auth(email = "pluszero@pluszero.co.kr",
         scopes = c("https://www.googleapis.com/auth/bigquery",
                    "https://www.googleapis.com/auth/analytics",
                    "https://www.googleapis.com/auth/cloud-platform",
                    "https://www.googleapis.com/auth/analytics.edit",
                    "https://www.googleapis.com/auth/analytics.manage.users",
                    "https://www.googleapis.com/auth/analytics.manage.users.readonly",
                    "https://www.googleapis.com/auth/analytics.readonly",
                    "https://www.googleapis.com/auth/analytics.user.deletion"))


account_list <- ga_account_list()
#View(account_list)

target_view_id <- "<GA_VIEW_ID>"
target_property_id <- "<GA_PROPERTY_ID>"

########################################################################################################################
date_set <- "<DATE>"

#Yesterday
yesterday <- as.Date(format(Sys.time(), "%Y-%m-%d")) - 1

date_list <- format(seq(as.Date(date_set), yesterday, by="days"), format="%Y%m%d")






#GET: Naver Adgroup Info
method = 'GET'
uri = '/ncc/adgroups'

flag <- T
while(flag == T){
  request_adgroup <- GET(url = paste0(BASE_URL, uri),
                         add_headers("Content-Type" = "application/json; charset=UTF-8",
                                     "X-Timestamp" = as.character(round(as.numeric(Sys.time()) * 1000)),
                                     "X-API-KEY" = API_KEY,
                                     "X-Customer" = CUSTOMER_ID, "X-Signature" = sig(method, uri, SECRET_KEY, CUSTOMER_ID)))
  
  
  if(request_adgroup$status_code == 200){
    print("네이버 광고 그룹 가져오기 성공")
    dfs_adgroup <- as.data.frame(fromJSON(content(request_adgroup, "text")))
    adgroup_id_name <- dfs_adgroup[, c("nccAdgroupId", "name")]
    flag = F
  }
  
}

#View(dfs_adgroup)


########################################################################################################################
#GET: Naver Campaign Info
method <- "GET"
uri_campaign <- '/ncc/campaigns'
flag <- T

while(flag == T){
  request_campaign <- GET(url = paste0(BASE_URL, uri_campaign),
                          add_headers("Content-Type" = "application/json; charset=UTF-8",
                                      "X-Timestamp" = as.character(round(as.numeric(Sys.time()) * 1000)),
                                      "X-API-KEY" = API_KEY,
                                      "X-Customer" = CUSTOMER_ID, "X-Signature" = sig(method, uri_campaign, SECRET_KEY, CUSTOMER_ID)))
  
  if(request_campaign$status_code == 200){
    print("네이버 캠페인 가져오기 성공")
    dfs_campaign <- as.data.frame(fromJSON(content(request_campaign, "text")))
    campaign_id_name <- dfs_campaign[, c("nccCampaignId", "name")]
    flag = F
  }
  
}

########################################################################################################################

#GET: Naver Keyword Info

flatten <- function(lst) {
  do.call(c, lapply(lst, function(x) if(is.list(x)) flatten(x) else list(x)))
}


uri2 = '/ncc/keywords'
method <- "GET"
raw_keyword_data <- data.frame()
keyword_df <- data.frame()
keyword_df2 <- data.frame()
flag <- T
idx <- 1

while(idx <= length(dfs_adgroup$nccAdgroupId)){
  
  request2 <- GET(url = paste0(BASE_URL, uri2),
                  query = list(
                    nccAdgroupId = dfs_adgroup$nccAdgroupId[idx]
                  ), add_headers("Content-Type" = "application/json; charset=UTF-8",
                                 "X-Timestamp" = as.character(round(as.numeric(Sys.time()) * 1000)),
                                 "X-API-KEY" = API_KEY,
                                 "X-Customer" = CUSTOMER_ID, "X-Signature" = sig(method, uri2, SECRET_KEY, CUSTOMER_ID)))
  
  if(request2$status_code == 200)
  {
    tmp <- as.data.frame(fromJSON(content(request2, "text")))
    
    if(nrow(tmp) > 0){
      if(sum(names(tmp) %in% "links") > 0){
        GOD <- flatten(tmp$links)
        except_for_link <- tmp[, !names(tmp) %in% "links"]
        
        except_for_link$PC_URL <- GOD$pc.final
        except_for_link$MO_URL <- GOD$mobile.final
        
        tmp <- except_for_link
      }
      
      else{
        tmp$PC_URL <- NA
        tmp$MO_URL <- NA
      }
      
      tmp <- tmp[, !names(tmp) %in% "nccQi"]
      raw_keyword_data <- rbind(raw_keyword_data, tmp)
      idx <- idx + 1
    }
    else{
      idx <- idx + 1
    }
  }
  
  else{
    Sys.sleep(1)
    
  }
  
}
print("네이버 검색광고 등록 키워드 수집 완료")


########################################################################################################################
df <- merge(raw_keyword_data, dfs_adgroup[, names(dfs_adgroup) %in% c("nccAdgroupId", "name", "pcChannelKey", "adgroupType")], by = "nccAdgroupId")
df <- merge(df, dfs_campaign[, names(dfs_campaign) %in% c("nccCampaignId", "name")], by = "nccCampaignId")

table(df$adgroupType)

########################################################################################################################
#브랜드 검색
#1. 메인 : naver / cpc / / brandsearchPC
#2. 



pre_df <- df[df$adgroupType == "WEB_SITE", 
             names(df) %in% c("nccCampaignId", "nccAdgroupId", "nccKeywordId", "name.y", "name.x", "keyword", "PC_URL", "pcChannelKey", "adgroupType")]

#View(pre_df)

names(pre_df) <- c("nccCampaignId", "nccAdgroupId", "nccKeywordId", "keyword", "links", "Adgroup_name", "pcChannelKey", "adgroupType", "Campaign_name")

#URL Decode 해주고
pre_df$links <- url_decode(pre_df$links)

#UTF-8로 변환시킨다.
Encoding(pre_df$links) <- "UTF-8"

#연결 URL을 파싱한다.
parsed_df <- as.data.frame(param_get(pre_df$links))
parsed_df <- parsed_df[, grepl("utm.*", names(parsed_df))]

#파싱한 데이터를 확인한 다음에, 필요 없는건 삭제해버리자.
#근데 나중에 자동화를 위해서 그냥 일단 한꺼번에 합쳐버리고, 나중에 Upload하기 전에 필요한거만 빼가자.

Naver_df <- cbind(pre_df, parsed_df)
#View(Naver_df)

########################################################################################################################
#광고효과보고서 다운로드

uri = '/stat-reports'
method = "POST"
#날짜를 정해줘야 한다.
#일단은 임시로 date_list로 만든다.

#date_list <- format(seq(as.Date("2020-04-27"), as.Date("2020-04-27"), by="days"), format="%Y%m%d")
report_ids = c()

#선택한 날짜 만큼
for(dte in date_list){
  
  flag = T
  #성공할 때 까지 반복한다.
  while(flag == T){
    #광고효과보고서 신청 중임.
    request = POST(url = paste0(BASE_URL, uri),
                   body = list(
                     reportTp = 'AD',
                     statDt = dte
                   ), encode = "json",
                   add_headers("Content-Type" = "application/json; charset=UTF-8",
                               "X-Timestamp" = as.character(round(as.numeric(Sys.time()) * 1000)),
                               "X-API-KEY" = API_KEY,
                               "X-Customer" = CUSTOMER_ID, "X-Signature" = sig(method, uri, SECRET_KEY, CUSTOMER_ID)))
    if(request$status_code != 200){
      sprintf("%s 보고서 요청 실패", dte)
    }
    
    else{
      sprintf("%s 보고서 요청 성공", dte)
      
      #임시로 df로 치환해주고
      tmp <- as.data.frame(fromJSON(content(request, "text")))
      
      #report_ids에 넣어준다.
      report_ids <- c(report_ids, tmp$reportJobId)
      #성공했으니, Flag는 F로 해줘서, 무한 루프에서 빠져나온다.
      flag = F
    }
  }
}

#다운로드 신청 완료 했으니, 이제 조금 쉬었다가 다운로드 시작
#Sys.sleep(length(report_ids)*2)



performance = data.frame()


#content(request, "text")

#이제 리포트 별로 Download 링크 가져오고 데이터 다운 받기.
for(report_id in report_ids){
  
  #처음 쉬는 시간 1초
  wait_time = 0.5
  method2 = 'GET'
  uri2 = paste0('/stat-reports/', as.character(report_id))
  flag = T
  
  while (flag == T){
    print(report_id)
    request <- GET(url = paste0(BASE_URL, uri2),
                   add_headers("Content-Type" = "application/json; charset=UTF-8",
                               "X-Timestamp" = as.character(round(as.numeric(Sys.time()) * 1000)),
                               "X-API-KEY" = API_KEY,
                               "X-Customer" = CUSTOMER_ID, "X-Signature" = sig(method2, uri2, SECRET_KEY, CUSTOMER_ID)))
    
    if(request$status_code != 200){
      Sys.sleep(wait_time)
      wait_time = wait_time + 0.5
      print("요청 실패 다시 시작합니다.")
      
      if(wait_time > 1.5){
        print("너무 많이 실패했음으로, 그 다음번 Report로 옮깁니다")
        flag <- F
      }
    }
    else{
      #임시로 DF로 만들기
      tmp <- as.data.frame(fromJSON(content(request, "text")))
      #Status
      download_status <- tmp$status
      #URL
      download_URL <- tmp$downloadUrl
      #성공했으니 무한 Loop에서 탈출.
      flag <- F
      
      if(download_status == "BUILT"){
        print("다운로드 성공")
      }
      else{print("날짜에 데이터가 없음")}
      
      
      #새로운 loop에 도입
      #Wait_Time 초기화
      flag2 = T
      wait_time = 1
      while(flag2 == T){
        if(download_status != "BUILT"){
          print("보고서 missing, Skip this process")
          break
        }
        
        else{
          complete <- T
          while(complete == T){
            download_URL = parse_url(download_URL)
            download_query = toJSON(download_URL$query, auto_unbox = T)
            download_uri = '/report-download'
            
            request <- GET(url = paste0(BASE_URL, download_uri), query = download_URL$query,
                           add_headers("Content-Type" = "application/json; charset=UTF-8",
                                       "X-Timestamp" = as.character(round(as.numeric(Sys.time()) * 1000)),
                                       "X-API-KEY" = API_KEY,
                                       "X-Customer" = CUSTOMER_ID, "X-Signature" = sig(method2, download_uri, SECRET_KEY, CUSTOMER_ID)))
            
            if(request$status_code != 200){
              Sys.sleep(wait_time)
              wait_time = wait_time + 1
              
            }
            else{
              print("리포트 다운로드 성공")
              Sys.sleep(0.5)
              tmp <- read.table(text = rawToChar(request$content), sep = '\t')
              tmp <- tmp[,c(1:13)]
              performance <- rbind(performance,tmp)
              flag2 <- F
              complete <- F
            }
            
            
            
            
          }
          
          
        }
      }
    }
  }
}


names(performance) <- c('Date', 'CUSTOMER_ID', 'nccCampaignId', 'nccAdgroupId', 'nccKeywordId', 'AD_ID', 'Business_Channel_ID',
                        'Media_Code_ID', 'PC_Mobile_Type', 'Impression', 'Click', 'Cost', 'Sum_of_AD_Rank')

#Keyword 광고만 뽑아내기
Naver_Keyword_Performance <- performance[!performance$nccKeywordId %in% '-',]
#View(Naver_Keyword_Performance)
sum(Naver_Keyword_Performance$Click)
sum(Naver_Keyword_Performance$Impression)
sum(Naver_Keyword_Performance$Cost)
#View(Naver_Keyword_Performance)
########################################################################################################################

#write.csv(Naver_Keyword_Performance, "PZ.csv", fileEncoding = "euc-kr")


#캠페인 > ga:campaign
#키워드 > ga:keyword
#일별 > ga:date
#노출수 > ga:impressions
#클릭수 > ga:adClicks
#총비용 > ga:adCost
#소스 > ga:source -> naver
#매체 > ga:medium -> cpc

#View(Naver_Data)



Naver_Data <- Naver_Keyword_Performance
will_be_merged <- Naver_Data[, c("Click", "Impression","Cost","nccKeywordId","Date", "AD_ID")]



#네이버 API에서 뽑은 것과, 광고효과 보고서 합쳐버리기
merged <- merge(will_be_merged, Naver_df, by = "nccKeywordId", all.x = T)


#브랜드 검색을 위한 데이터
#campaign_fil <- dim_filter(dimension = "campaign", operator = "PARTIAL", expressions = "brandsearch")
#sourceMedium_fil <- dim_filter(dimension = "sourceMedium", operator = "PARTIAL", expressions = "naver")

#filter_clause <- filter_clause_ga4(list(campaign_fil, sourceMedium_fil), operator = "AND")


#prc <- google_analytics(target_view_id, date_range = c(as.character(date_set), as.character(yesterday)),
#                        dimensions = c("sourceMedium", "campaign", "keyword", "adContent"), dim_filters = filter_clause,
#                        metrics = c("sessions"),
#                        anti_sample = TRUE)


#sum(prc$sessions)

#prc_brand_only <- filter(merged, adgroupType == "BRAND_SEARCH")

#sum(prc_brand_only$Click)


#필요없는 데이터 다 뽑아버리기
#그리고 설정된 UTM 기준으로 데이터 가져온다!
upload_this <- merged[, !names(merged) %in% c("nccKeywordId", "nccAdgroupId", "links", "pcChannelKey", "theme", "Campaign_name", 
                                              "keyword", "AD_ID", "Adgroup_name", "nccCampaignId", "productGroupCode",
                                              "adgroupType")]


colnames(upload_this)[which(names(upload_this) == "Click")] <- "adClicks"
colnames(upload_this)[which(names(upload_this) == "Impression")] <- "impressions"
colnames(upload_this)[which(names(upload_this) == "Cost")] <- "adCost"
colnames(upload_this)[which(names(upload_this) == "Date")] <- "date"
colnames(upload_this)[which(names(upload_this) == "utm_source")] <- "source"
colnames(upload_this)[which(names(upload_this) == "utm_medium")] <- "medium"
colnames(upload_this)[which(names(upload_this) == "utm_term")] <- "keyword"
colnames(upload_this)[which(names(upload_this) == "utm_content")] <- "adContent"
colnames(upload_this)[which(names(upload_this) == "utm_campaign")] <- "campaign"

#필요한 컬럼만 걸러내기
upload_this <- upload_this[, names(upload_this) %in% c("adClicks", "impressions", "adCost", "date", "campaign", "medium", "source",
                                                                      "keyword", "adContent")]


#만약에 모든 컬럼들을 확인해보고, 존재하지 않으면, NA로 넣어라.
required_columns <- c("adClicks", "impressions", "adCost", "date", "campaign", "medium", "source", "keyword", "adContent")
upload_this <- upload_this[intersect(required_columns, names(upload_this))]
upload_this[setdiff(required_columns, names(upload_this))] <- ""

str(upload_this)
View(upload_this)
##############################################################################################################
#첫 세팅에만 직접 보고 확인하기!!
#View(ga_account_list())

#내가 원하는 어카운트 정보만 가져온다.
target_account_list <- account_list[account_list$webPropertyId %in% target_property_id & account_list$viewId %in% target_view_id, ]

#그리고 그 계정 정보를 활용해서, Custom Upload ID를 가져온다.
custom_info <- ga_custom_datasource(accountId = target_account_list$accountId, webPropertyId = target_account_list$webPropertyId)
target_upload_id <- custom_info[custom_info$name %in% "<CUSTOM_UPLOAD_ID>", "id"]



write.csv(upload_this, "tmp.csv", row.names = FALSE, fileEncoding = "utf-8")
Sys.setlocale("LC_ALL","C") # 강제 언어 삭제
TARGET <- read.csv("tmp.csv", encoding = 'utf-8')
Sys.setlocale("LC_ALL","Korean") # 언어 다시 한글로


str(upload_this)


#Upload ID가 필요하다.
obj <- ga_custom_upload_file(target_view_id, 
                             target_property_id, 
                             target_upload_id, 
                             TARGET)

obj <- ga_custom_upload(upload_object = obj)

obj

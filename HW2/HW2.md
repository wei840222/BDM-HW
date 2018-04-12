#BDM HW2
**106598018 萬俊瑋**

* Environment setup in your cluster
1 PC, Intel I7-6700 CPU, 8G memory, 480GB SSD, 100Mbps network bandwidth
1 master node, 3 slave node in docker containers

* Source codes
```python
news_data=sqlContext.read.format('csv')\
                    .option("header", "true")\
                    .option("inferschema", "true")\
                    .option("mode", "DROPMALFORMED")\
                    .load('hdfs:///user/hadoop/News_Final.csv')
news_data=news_data.withColumn('SentimentTitle', news_data['SentimentTitle'].cast('double'))
news_data=news_data.withColumn('SentimentHeadline', news_data['SentimentHeadline'].cast('double'))
news_data.show()

TitleCount_total=news_data.select('Title').rdd.flatMap(list)\
                                   .flatMap(lambda x:x.split(' '))\
                                   .map(lambda x:(x, 1))\
                                   .reduceByKey(add)\
                                   .sortBy(lambda x:x[1], False)\
                                   .collect()
out=open('output/TitleCount_total.txt','w')
for i in range(len(TitleCount_total)):
    out.write(str(TitleCount_total[i]) + '\n')
out.close()

HeadlineCount_total=news_data.select('Headline').rdd.flatMap(list)\
                                         .filter(lambda x:type(x)==str)\
                                         .flatMap(lambda x:x.split(' '))\
                                         .map(lambda x:(x, 1))\
                                         .reduceByKey(add)\
                                         .sortBy(lambda x:x[1], False)\
                                         .collect()
out=open('output/HeadlineCount_total.txt','w')
for i in range(len(HeadlineCount_total)):
    out.write(str(HeadlineCount_total[i]) + '\n')
out.close()

TitleCount_day=news_data.select('PublishDate', 'Title').rdd.map(list)\
                                                .map(lambda x:(x[0].split(' ')[0].replace('-', '') ,x[1].split(' ')))\
                                                .flatMap(lambda x:[(x[0], element) for element in x[1]])\
                                                .map(lambda x:(x, 1))\
                                                .reduceByKey(add)\
                                                .sortBy(lambda x:(x[0][0], x[1]), False)\
                                                .collect()
out=open('output/TitleCount_day.txt','w')
currentDate=TitleCount_day[0][0][0]
for i in range(len(TitleCount_day)):
    if currentDate!=TitleCount_day[i][0][0]:
        out.write('\n')
        currentDate=TitleCount_day[i][0][0]
    out.write(str(TitleCount_day[i]) + '\n')
out.close()

HeadlineCount_day=news_data.select('PublishDate', 'Headline').rdd.map(list)\
                                                      .filter(lambda x:type(x[1])==str)\
                                                      .map(lambda x:(x[0].split(' ')[0].replace('-', '') ,x[1].split(' ')))\
                                                      .flatMap(lambda x:[(x[0], element) for element in x[1]])\
                                                      .map(lambda x:(x, 1))\
                                                      .reduceByKey(add)\
                                                      .sortBy(lambda x:(x[0][0], x[1]), False)\
                                                      .collect()
out=open('output/HeadlineCount_day.txt','w')
currentDate=HeadlineCount_day[0][0][0]
for i in range(len(HeadlineCount_day)):
    if currentDate!=HeadlineCount_day[i][0][0]:
        out.write('\n')
        currentDate=HeadlineCount_day[i][0][0]
    out.write(str(HeadlineCount_day[i]) + '\n')
out.close()

TitleCount_topic=news_data.select('Topic', 'Title').rdd.map(list)\
                                            .map(lambda x:(x[0] ,x[1].split(' ')))\
                                            .flatMap(lambda x:[(x[0], element) for element in x[1]])\
                                            .map(lambda x:(x, 1))\
                                            .reduceByKey(add)\
                                            .sortBy(lambda x:(x[0][0], -x[1]))\
                                            .collect()
out=open('output/TitleCount_topic.txt','w')
currentTopic=TitleCount_topic[0][0][0]
for i in range(len(TitleCount_topic)):
    if currentTopic!=TitleCount_topic[i][0][0]:
        out.write('\n')
        currentTopic=TitleCount_topic[i][0][0]
    out.write(str(TitleCount_topic[i]) + '\n')
out.close()

HeadlineCount_topic=news_data.select('Topic', 'Headline').rdd.map(list)\
                                                  .filter(lambda x:type(x[1])==str)\
                                                  .map(lambda x:(x[0] ,x[1].split(' ')))\
                                                  .flatMap(lambda x:[(x[0], element) for element in x[1]])\
                                                  .map(lambda x:(x, 1))\
                                                  .reduceByKey(add)\
                                                  .sortBy(lambda x:(x[0][0], -x[1]))\
                                                  .collect()
out=open('output/HeadlineCount_topic.txt','w')
currentTopic=HeadlineCount_topic[0][0][0]
for i in range(len(HeadlineCount_topic)):
    if currentTopic!=HeadlineCount_topic[i][0][0]:
        out.write('\n')
        currentTopic=HeadlineCount_topic[i][0][0]
    out.write(str(HeadlineCount_topic[i]) + '\n')
out.close()

fileList=['Facebook_Economy', 'Facebook_Microsoft', 'Facebook_Obama', 'Facebook_Palestine',\
          'GooglePlus_Economy', 'GooglePlus_Microsoft', 'GooglePlus_Obama', 'GooglePlus_Palestine',\
          'LinkedIn_Economy', 'LinkedIn_Microsoft', 'LinkedIn_Obama', 'LinkedIn_Palestine']

header_per_hour=['IDLink'] + ['TS'+str((count+1)*3) for count in range(48)]
header_per_day=['IDLink'] + ['TS'+str((count+1)*72) for count in range(2)]

platforms='Facebook'
out_hour=open('output/' + platforms + '_hour.txt','w')
out_day=open('output/' + platforms + '_day.txt','w')
for fileName in fileList:
    social_feedback_data=sqlContext.read.format('csv')\
    .option("header", "true")\
    .option("inferschema", "true")\
    .load('hdfs:///user/hadoop/' + fileName + '.csv')
    popularityAvg_per_hour=social_feedback_data.select(header_per_hour).rdd.map(list)\
                                               .flatMap(lambda x:((x[0], element) for element in x[1:]))\
                                               .reduceByKey(add).map(lambda x:(x[0], x[1]/48)).sortByKey()\
                                               .map(lambda x:('ID'+str(x[0]), x[1])).collect()
    popularityAvg_per_day=social_feedback_data.select(header_per_day).rdd.map(list)\
                                              .flatMap(lambda x:((x[0], element) for element in x[1:]))\
                                              .reduceByKey(add).map(lambda x:(x[0], x[1]/2)).sortByKey()\
                                              .map(lambda x:('ID'+str(x[0]), x[1])).collect()
    if fileName.split('_')[0] != platforms:
        out_hour.close()
        out_day.close()
        platforms=fileName.split('_')[0]
        out_hour=open('output/' + platforms + '_hour.txt','w')
        out_day=open('output/' + platforms + '_day.txt','w')
    out_hour.write(fileName + '\n')
    out_day.write(fileName + '\n')
    for i in range(len(popularityAvg_per_hour)):
        out_hour.write(str(popularityAvg_per_hour[i]) + '\n')
    out_hour.write('\n')
    for i in range(len(popularityAvg_per_day)):
        out_day.write(str(popularityAvg_per_day[i]) + '\n')
    out_day.write('\n')
out_hour.close()
out_day.close()

SentimentScore=news_data.select('Topic', 'SentimentTitle', 'SentimentHeadline').rdd.map(list).map(lambda x:(x[0], [x[1], x[2], 1])).reduceByKey(lambda x, y:[x[0]+y[0], x[1]+y[1], x[2]+y[2]])
SentimentScore_sum=SentimentScore.sortByKey(False).map(lambda x:[x[0], x[1][0], x[1][1]]).collect()
SentimentScore_average=SentimentScore.sortByKey(False).map(lambda x:[x[0], x[1][0]/x[1][2], x[1][1]/x[1][2]]).collect()

out=open('output/SentimentScore.txt','w')
out.write('SentimentScore_sum\n')
for i in range(len(SentimentScore_sum)):
    out.write(str(SentimentScore_sum[i]) + '\n')
out.write('\nSentimentScore_average\n')
for i in range(len(SentimentScore_average)):
    out.write(str(SentimentScore_average[i]) + '\n')
out.close()

k=100
topics=sorted(news_data.select('Topic').rdd.flatMap(list).distinct().collect())
for topic in topics:
    top_k_frequent_words=news_data.select('Topic', 'Title').rdd.map(list)\
                                            .map(lambda x:(x[0] ,x[1].split(' ')))\
                                            .flatMap(lambda x:[(x[0], element) for element in x[1]])\
                                            .map(lambda x:(x, 1))\
                                            .reduceByKey(add)\
                                            .sortBy(lambda x:(x[0][0], -x[1]))\
                                            .filter(lambda x:x[0][0]==topic)\
                                            .map(lambda x:x[0][1])\
                                            .collect()
    top_k_frequent_words=top_k_frequent_words[:k]
    title=news_data.select('Title', 'Topic').rdd.map(list).filter(lambda x:x[1]==topic).map(lambda x:x[0]).collect()
    co_occurrence_matrices=[[0]*k for i in range(k)]
    out=open('output/top_' + str(k) + '_frequent_words_co_occurrence_matrices_in_Title_of_topic_' + topic + '.txt','w')
    for i in range(k):
        for j in range(k):
            for t in range(len(title)):
                if top_k_frequent_words[i] in title[t] and top_k_frequent_words[j] in title[t]:
                    co_occurrence_matrices[i][j]+=1
        out.write(str(co_occurrence_matrices[i]) + '\n')
    out.close()
    
k=100
topics=sorted(news_data.select('Topic').rdd.flatMap(list).distinct().collect())
for topic in topics:
    top_k_frequent_words=news_data.select('Topic', 'Headline').rdd.map(list)\
                                            .filter(lambda x:type(x[1])==str)\
                                            .map(lambda x:(x[0] ,x[1].split(' ')))\
                                            .flatMap(lambda x:[(x[0], element) for element in x[1]])\
                                            .map(lambda x:(x, 1))\
                                            .reduceByKey(add)\
                                            .sortBy(lambda x:(x[0][0], -x[1]))\
                                            .filter(lambda x:x[0][0]==topic)\
                                            .map(lambda x:x[0][1])\
                                            .collect()
    top_k_frequent_words=top_k_frequent_words[:k]
    headline=news_data.select('Headline', 'Topic').rdd.map(list).filter(lambda x:x[1]==topic).map(lambda x:x[0]).filter(lambda x:type(x)==str).collect()
    co_occurrence_matrices=[[0]*k for i in range(k)]
    out=open('output/top_' + str(k) + '_frequent_words_co_occurrence_matrices_in_Headline_of_topic_' + topic + '.txt','w')
    for i in range(k):
        for j in range(k):
            for t in range(len(headline)):
                if top_k_frequent_words[i] in headline[t] and top_k_frequent_words[j] in headline[t]:
                    co_occurrence_matrices[i][j]+=1
        out.write(str(co_occurrence_matrices[i]) + '\n')
    out.close()
```

* The generated output
[https://drive.google.com/drive/folders/1bkGgDzh4WiOiZENQmrCirDgWrjGWeG81?usp=sharing](https://drive.google.com/drive/folders/1bkGgDzh4WiOiZENQmrCirDgWrjGWeG81?usp=sharing)
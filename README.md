send review/data to kafka(producer/raw) as it is raw and then kafka (consumer) to process/analyse the input to get the sentiment and we are storing that sentiment in elastic search.

Consumer will consume the data. after consuming the data we will pass the data inside method analyze_sentiment, then we will store the data into the elasic.

Reading the data and taking only input part of it.

Inside my elasic account, we have kibana,

The polarity, subjectivity, and sentiment 

UI stremlit.


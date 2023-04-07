import snscrape.modules.twitter as sntwitter
import pandas as pd
import streamlit as st
import datetime
import pymongo
import time
 
# REQUIRED VARIABLES
pym = pymongo.MongoClient("mongodb://localhost:27017/")  # To connect to MONGODB
mydb = pym["Twitter_Scraped_Database"]    # To create a DATABASE
tweets_df = pd.DataFrame()
dfm = pd.DataFrame()
st.set_page_config(page_title="Twitter Data Scraping")
st.header("Twitter Data scraping:wave:")
option = st.selectbox("Select Keyword or Hashtag",('Keyword', 'Hashtag'))
word = st.text_input("Enter a "+option)
start = st.date_input("Start date", datetime.date(2022, 1, 1))
end = st.date_input("End date", datetime.date(2023, 1, 1))
limit = st.slider("Select range to scrape tweets", 0, 1000, 1)
tweets= []
data = "Query"

# SCRAPE DATA USING TwitterSearchScraper
if option=='Keyword':
    for i, tweet in enumerate(sntwitter.TwitterSearchScraper(data).get_items()):
        if i>limit:
            break
        tweets.append([ tweet.id, tweet.date,  tweet.content, tweet.lang, tweet.user.username, tweet.replyCount, tweet.retweetCount,tweet.likeCount, tweet.source, tweet.url ])
    tweets_df = pd.DataFrame(tweets, columns=['ID','Date','Content', 'Language', 'Username', 'ReplyCount', 'RetweetCount', 'LikeCount','Source', 'Url'])
else:
    for i, tweet in enumerate(sntwitter.TwitterHashtagScraper(data).get_items()):
        if i>limit:
            break            
        tweets.append([ tweet.id, tweet.date,  tweet.content, tweet.lang, tweet.user.username, tweet.replyCount, tweet.retweetCount,tweet.likeCount, tweet.source, tweet.url ])
    tweets_df = pd.DataFrame(tweets, columns=['ID','Date','Content', 'Language', 'Username', 'ReplyCount', 'RetweetCount', 'LikeCount','Source', 'Url'])


# DOWNLOAD AS CSV
@st.cache_data # IMPORTANT: Cache the conversion to prevent computation on every rerun
def convert_df(df):    
    return df.to_csv().encode('utf-8')

if not tweets_df.empty:
    csv = convert_df(tweets_df)
    st.download_button(label="Download as CSV",data=csv,file_name='Twitter_Scrap.csv',mime='text/csv')

    # DOWNLOAD AS JSON
    json= tweets_df.to_json(orient ='records')
    st.download_button(label="Download as JSON",file_name="Twitter_Scrap.json",mime="application/json",data=json)

    # UPLOAD DATA TO DATABASE
    if st.button('Upload Tweets to Database'):
        coll=word
        coll=coll.replace(' ','_')+'_Tweets'
        mycoll=mydb[coll]
        dict=tweets_df.to_dict('records')
        if dict:
            mycoll.insert_many(dict)
            ts = datetime.datetime.now()
            mycoll.update_many({}, {"$set": {"KeyWord_or_Hashtag": word+str(ts)}}, upsert=False, array_filters=None)
            st.success('Tweets uploaded to database')



    # SHOW TWEETS
    if st.button('Show Tweets'):
        st.write(tweets_df)

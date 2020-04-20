from selenium import webdriver
import time
import csv
from bs4 import BeautifulSoup
from lxml import html
import pandas as pd

def getHashtagList(temp_tweet):
    temp_hashtags = temp_tweet.find_elements_by_xpath(
        ".//div[@class ='js-tweet-text-container']//a[starts-with(@href,'/hashtag')]")
    hashtags = []
    for hash in temp_hashtags:
        hashtags.append(hash.text) #already decoded
    #print(hashtags)
    return hashtags

def getTweetId(temp_tweet):
    id = temp_tweet.get_attribute("data-item-id")
    return id #already decoded
    #print(id)

months = ["gen", "feb","mar","apr","mag","giu","lug","ago","set","ott","nov","dic"]
def to_numeric_timestamp(timestamp):
  for i in range(0,12):
    if months[i] in timestamp:
      if i <= 8:
          return timestamp.replace(months[i],"0"+str(i+1))
      else:
          return timestamp.replace(months[i], str(i+1))

def getTweetTimeStamp(temp_tweet):
    timestamp = temp_tweet.find_element_by_xpath(".//a[starts-with(@class,'tweet-timestamp')]").get_attribute("title")#"data-original-title")
    if timestamp == "":
        return 'TimeNotAvailable'

    timestamp = to_numeric_timestamp(timestamp)
    print(timestamp)
    return timestamp#.decode('utf-8')



def getTweetCreator(temp_tweet):
    userId = temp_tweet.find_element_by_xpath("./div[starts-with(@class,'tweet')]").get_attribute("data-screen-name")
    #print(userId)
    return userId#.decode('utf-8')

def getTweetText(temp_tweet):
    text = temp_tweet.find_element_by_xpath(".//div[@class ='js-tweet-text-container']").text
    return text#.decode('utf-8')
    #print(text)

def getTweetLikes(temp_tweet):
    likeCount = temp_tweet.find_element_by_xpath(".//div[contains(@class,'ProfileTweet-action--favorite')]//span[@class = 'ProfileTweet-actionCountForPresentation']").text
    if likeCount == '':
        return '0'

    return likeCount

def getTweetReshareCount(temp_tweet):
    reshareCount = temp_tweet.find_element_by_xpath(
        ".//div[contains(@class,'ProfileTweet-action--retweet')]//span[@class = 'ProfileTweet-actionCountForPresentation']").text

    if reshareCount == '':
        return '0'

    return reshareCount

def getTweetReplyCount(temp_tweet):
    replyCount = temp_tweet.find_element_by_xpath(
        ".//div[contains(@class,'ProfileTweet-action--reply')]//span[@class = 'ProfileTweet-actionCountForPresentation']").text
    if replyCount == '':
        return '0'

    return replyCount

#potresti recuperare anche le risposte ...
def getTweetReplies(temp_tweet):
    pass
def getTweetImage(temp_tweet):
    pass
def getTweetVideo(temp_tweet):
    pass
# df = pd.DataFrame(data=d)
# df.to_csv('data/BASICDATA.csv')


options = webdriver.ChromeOptions()
# options.add_argument('headless')
driver = webdriver.Chrome(chrome_options=options,
                          executable_path="C:\\Users\\Utente\\PycharmProjects\\untitled\\WebScienceProject\\chromedriver.exe")

print("Starting")
driver.get("https://twitter.com/search?f=tweets&vertical=default&q=%23comolake%20since%3A2018-01-01%20until%3A2018-08-01&l=en&src=typd")
scroll_pause_time = 10

# Get scroll height
last_height = driver.execute_script("return document.body.scrollHeight")

print("inizio a scorrere la schermata")
while True:
#for i in range(1,4):
    print(last_height)
    # Scroll down to bottom
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    # Wait to load page
    time.sleep(scroll_pause_time)

    # Calculate new scroll height and compare with last scroll height
    new_height = driver.execute_script("return document.body.scrollHeight")
    print(new_height, last_height)

    if new_height == last_height:
        break
    last_height = new_height

temp_tweets = driver.find_elements_by_xpath("//div[@class = 'stream']/ol/li[starts-with(@class,'js-stream-item')]")#/div[starts-with(@class,'tweet')]")
#print("Tweets: {tweets}".format(tweets =len(temp_tweets)))

print("Ho finito di scorrere, estraggo i dati")

with open("scrapingTwitterBackup.html",'w',encoding = "utf-8") as html_file:
    html_file.write(driver.page_source)


hashtags = []
tweets = []
for temp_tweet in temp_tweets:
      hashtags = getHashtagList(temp_tweet)
      id = getTweetId(temp_tweet)
      creator = getTweetCreator(temp_tweet)
      likes = getTweetLikes(temp_tweet)
      num_replies = getTweetReplyCount(temp_tweet)
      reshares = getTweetReshareCount(temp_tweet)
      timestamp = getTweetTimeStamp(temp_tweet)
      text = getTweetText(temp_tweet)

      tweets.append({"id":id, "creator":creator,"hashtags":hashtags, "timestamp":timestamp, "reshares":reshares, "likes":likes, "num_replies":num_replies, "text":text})

#print(tweets)

with open('F:\\TwitterDataSet\\ComoLake_2018_Gennaio_Luglio\\tweets_2018_comolake_gen_lug_no_emoticons.csv', 'w',encoding = "utf-8",newline="") as csv_file:
#with open('tweets_first_text.csv', 'w', encoding = "utf-8") as csv_file:
    headers = tweets[0].keys();
    dict_writer = csv.DictWriter(csv_file, headers, delimiter = ',', lineterminator = "\n")
    dict_writer.writeheader()

    for row in tweets:
        dict_writer.writerow(row)
    #rows = []
    #for key,row in tweets:

    #writer = csv.writer(csv_file, delimiter = ",")
    #for tweet in tweets:
     #   for key, value in tweet.items():
      #     writer.writerow([key, value])

print("File Salvato con successo")
'''anchors = driver.find_elements_by_xpath("//a[starts-with(@href,'/hashtag')]")
a = []
for anchor in anchors[0:10]:
    print(anchor.get_attribute('innerHTML')) #<- questo stampa quello che ti serve ... devi trovare il modo di estrarre solo il testo da dentro il <b> ...
    a.append(BeautifulSoup(anchor.get_attribute('innerHTML'), 'lxml'))
#print(a[0])
print(a[0].get_text)
#print(a[0].stripped_strings)
#from pprint import pprint
#pprint(vars(tweets[0]))
#for tweet in driver.find_elements_by_xpath("//a[starts-with(@href,'/hashtag')]"):
 #   print(tweet.get_tag)'''
'''
html_source = driver.page_source
#print(html_source)
#sourcedata= html_source.encode('utf-8')
#print(sourcedata)
doc = html.fromstring(html_source)
#soup=BeautifulSoup(sourcedata,'lxml')
tweets = doc.xpath("//a")#"//a[starts-with(@href,'/hashtag')]")
print(tweets[0].__dict__)
from pprint import pprint
pprint(vars(tweets[0]))

#print(list(tweets))
#help(tweets[0])
#for tweet in tweets:
#    help(tweet)
'''
'''
print(tweets)
tweets_soup = []
for tweet_selenium in tweets:
      tweets_soup.append(BeautifulSoup(tweet_selenium.get_attribute('innerHTML'), 'lxml'))
extractHashtag(tweets_soup)
#arr = [x.div['data-screen-name'] for x in soup.body.findAll('div', attrs={'data-item-type':'user'})]
#bios = [x.p.text for x in soup.body.findAll('div', attrs={'data-item-type':'user'})]
#fullnames = [x.text.strip() for x in soup.body.findAll('a', 'fullname')][1:] # avoid your own name
#d = {'usernames': arr, 'bios': bios, 'fullnames': fullnames}
#print(arr)
#print(bios)
#print(fullnames)
#print(d)'''



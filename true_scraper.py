from lxml import html
from bs4 import BeautifulSoup
import re
import sys,getopt
from selenium import webdriver
import time
import csv


def getHashtagList(temp_tweet):
    temp_hashtags = temp_tweet.xpath(
        ".//div[@class ='js-tweet-text-container']//a[starts-with(@href,'/hashtag')]")

    hashtags = []
    for hash in temp_hashtags:
        hashtags.append(hash.text_content()) #already decoded
    #print(hashtags)
    return hashtags

def getTweetId(temp_tweet):
    id = temp_tweet.get("data-item-id")
    return id #already decoded
    #print(id)

months = ["gen", "feb","mar","apr","mag","giu","lug","ago","set","ott","nov","dic"]
days = ["- 1 ","- 2 ","- 3 ","- 4 ","- 5 ","- 6 ","- 7 ","- 8 ","- 9 "]
def to_numeric_timestamp(timestamp):
    temp = ""
    if timestamp is None:
        return temp

    for i in range(0,12):
        if months[i] in timestamp:
          if i <= 8:
              temp = timestamp.replace(months[i],"0"+str(i+1))
          else:
              temp =  timestamp.replace(months[i], str(i+1))

    for j in range(0,9):
          if days[j] in timestamp:
              temp = temp.replace(days[j],"- 0"+str(j+1)+" ")
    return temp


def getTweetTimeStamp(temp_tweet):
    timestamp = temp_tweet.xpath(".//a[starts-with(@class,'tweet-timestamp')]")[0].get("title")#"data-original-title")
    if timestamp == "":
        return 'TimeNotAvailable'

    timestamp = to_numeric_timestamp(timestamp)
    #print(timestamp)
    return timestamp#.decode('utf-8')



def getTweetCreator(temp_tweet):
    userId = temp_tweet.xpath("./div[starts-with(@class,'tweet')]")[0].get("data-screen-name")
    #print(userId)
    return userId#.decode('utf-8')

def getTweetText(temp_tweet):
    text = temp_tweet.xpath(".//div[@class ='js-tweet-text-container']")[0].text_content()
    return text#.decode('utf-8')
    #print(text)

def getTweetCleanText(temp_tweet):
    text = temp_tweet.xpath(".//div[@class ='js-tweet-text-container']")[0]
    #print(text.text_content())
    temp = html.tostring(text, encoding="unicode")
    #print(temp)
    temp = re.sub(r'<a href="(.*?)".*>(.*)</a>',"",temp)
   # for link in text.xpath(".//a"):
    #    print(link.text_content())
     #   temp = re.sub(link.text_content(),"",temp)

    text = html.fromstring(temp)
    #print(text.text_content())
    return text.text_content()


def getTweetLikes(temp_tweet):
    likeCount = temp_tweet.xpath(".//div[contains(@class,'ProfileTweet-action--favorite')]//span[@class = 'ProfileTweet-actionCountForPresentation']")[0].text_content()
    if likeCount == '':
        return '0'

    return likeCount

def getTweetReshareCount(temp_tweet):
    reshareCount = temp_tweet.xpath(
        ".//div[contains(@class,'ProfileTweet-action--retweet')]//span[@class = 'ProfileTweet-actionCountForPresentation']")[0].text_content()

    if reshareCount == '':
        return '0'

    return reshareCount

def getTweetReplyCount(temp_tweet):
    replyCount = temp_tweet.xpath(
        ".//div[contains(@class,'ProfileTweet-action--reply')]//span[@class = 'ProfileTweet-actionCountForPresentation']")[0].text_content()
    if replyCount == '':
        return '0'

    return replyCount

def data_ingestion(online_mode, url, uri):
    if online_mode == True:
        options = webdriver.ChromeOptions()
        # options.add_argument('headless')
        driver = webdriver.Chrome(chrome_options=options,
                                  executable_path="C:\\Users\\Utente\\PycharmProjects\\untitled\\WebScienceProject\\chromedriver.exe")

        print("Starting")
        driver.get(url)
        scroll_pause_time = 10

        # Get scroll height
        last_height = driver.execute_script("return document.body.scrollHeight")

        print("inizio a scorrere la schermata")
        while True:
            # for i in range(1,4):
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

        #temp_tweets = driver.find_elements_by_xpath(
         #   "//div[@class = 'stream']/ol/li[starts-with(@class,'js-stream-item')]")  # /div[starts-with(@class,'tweet')]")
        # print("Tweets: {tweets}".format(tweets =len(temp_tweets)))

        with open(uri, 'w', encoding="utf-8") as html_file:
            html_file.write(driver.page_source)

    with open(uri, 'r', encoding="utf-8") as readfile:
        filestring = readfile.read().replace('\n', '')
        tree = html.fromstring(filestring)
    return tree

def data_extraction(query_for_objects, tree,endpoint_index):
    temp_tweets = tree.xpath(
        query_for_objects)  # /div[starts-with(@class,'tweet')]")

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
        no_link_text = getTweetCleanText(temp_tweet)

        tweets.append({"id": id, "creator": creator, "hashtags": hashtags, "timestamp": timestamp, "reshares": reshares,
                       "likes": likes, "num_replies": num_replies, "text": text, "clean_text": no_link_text})

    return tweets
    # print(tweets)

def write_data_to_csv(csvuri,extracted_data):
    with open(
            csvuri,
            'w', encoding="utf-8", newline="") as csv_file:
        # with open('tweets_first_text.csv', 'w', encoding = "utf-8") as csv_file:
        headers = extracted_data[0].keys();
        dict_writer = csv.DictWriter(csv_file, headers, delimiter=',', lineterminator="\n")
        dict_writer.writeheader()

        for row in extracted_data:
            print(row)
            dict_writer.writerow(row)

        # rows = []
        # for key,row in tweets:

        # writer = csv.writer(csv_file, delimiter = ",")
        # for tweet in tweets:
        #   for key, value in tweet.items():
        #     writer.writerow([key, value])

    print("File Salvato con successo")


def main(argv):
    try:
        opts, args = getopt.getopt(argv, "", [])
    except getopt.GetoptError:
        print("wrong arguments")
        sys.exit(2)

    #per stabilire se fare il download o meno della pagina (se non l'ho salvata già da prima)
    online_mode = True
    #per stabilire quale endpoint/file/oggetti invocare
    endpoint_index = args[1]

    if args[0] == "1":
        online_mode = True
    else:
        online_mode = False

    print(online_mode)
    #time.sleep(4)

    if args[1] == "1":#Query_1 = talking of como #ComoLake <---- da fare, attenzione ad andare in recenti NON in popolari
        uri = "TweetsTalkingOfComo_Query_1.html"
        url = "https://twitter.com/search?" #da continuare
        csvuri = "F:\\TwitterDataSet\\Query_1.csv"
        query_for_objects = "//div[@class = 'stream']/ol/li[starts-with(@class,'js-stream-item')]"
    elif args[1] == "2": #Query_2 = IMPLEMENTARE IN FUTURO UN MECCANISMO PER ESTENDERE A CASCATA LA QUERY DI COMOLAKE
        #STUB: fingo sia query 1 per ora
        uri = "TweetsTalkingOfComo_Query_1.html"
        url = "https://twitter.com/search?" #da continuare
        csvuri = "F:\\TwitterDataSet\\Query_1.csv"
        query_for_objects = "//div[@class = 'stream']/ol/li[starts-with(@class,'js-stream-item')]"
    elif args[1] == "3":  #Query_3 = gente DI como che parla di qualunque cosa nel periodo giugno-ottobre (ATTENTO: devi prendere recenti, non popolari)
        #attenzione, scaglionalo in base a periodi temporali, se no il tuo computer non regge ...
        uri = "TweetsTalkingOfComo_Query_3_12_ottobre_31_ottobre.html"
        url = "https://twitter.com/search?f=tweets&vertical=default&q=geocode%3A45.804968%2C9.090950%2C5km%20since%3A2016-10-12%20until%3A2016-11-1&src=typd"  # da continuare
        csvuri = "F:\\TwitterDataSet\\Query_3_temp_partial_part_2.csv"
        query_for_objects = "//div[@class = 'stream']/ol/li[starts-with(@class,'js-stream-item')]"
    elif args[1] == "4":  #Query_3 = gente DI como che parla di qualunque cosa nel periodo novembre-dicembre (ATTENTO: devi prendere recenti, non popolari)
        #attenzione, scaglionalo in base a periodi temporali, se no il tuo computer non regge ...
        uri = "TweetsTalkingOfComo_Query_3_1_novembre_31_dicembre.html"
        url = "https://twitter.com/search?f=tweets&q=geocode%3A45.804968%2C9.090950%2C5km%20since%3A2016-11-1%20until%3A2017-1-1&src=typd"  # da continuare
        csvuri = "F:\\TwitterDataSet\\Query_3_novembre_dicembre.csv"
        query_for_objects = "//div[@class = 'stream']/ol/li[starts-with(@class,'js-stream-item')]"
    elif args[1] == "5":  #Query_3 = gente DI como che parla di qualunque cosa nel periodo novembre-dicembre (ATTENTO: devi prendere recenti, non popolari)
        #attenzione, scaglionalo in base a periodi temporali, se no il tuo computer non regge ...
        uri = "TweetsTalkingOfComo_Query_3_1_gennaio_31_maggio.html"
        url = "https://twitter.com/search?f=tweets&q=geocode%3A45.804968%2C9.090950%2C5km%20since%3A2016-1-1%20until%3A2016-6-1&src=typd"  # da continuare
        csvuri = "F:\\TwitterDataSet\\Query_3_gennaio_maggio.csv"
        query_for_objects = "//div[@class = 'stream']/ol/li[starts-with(@class,'js-stream-item')]"
    elif args[1] == "6":  #Query_4 = scrape sui dati degli utenti
        uri = "TweetsTalkingOfComo_Query_1.html"
        url = "https://twitter.com/search?"  # da continuare
        csvuri = "F:\\TwitterDataSet\\Query_1.csv"
        query_for_objects = "//div[@class = 'stream']/ol/li[starts-with(@class,'js-stream-item')]"
    elif args[1] == "7":  # Query_5 = PRENDI TUTTI I TWEET A COMO NEL 2017
        uri = "TweetsTalkingOfComo_Query_2017.html"
        url = "https://twitter.com/search?f=tweets&q=geocode%3A45.804968%2C9.090950%2C5km%20since%3A2017-1-1%20until%3A2017-12-31&src=typd"  # da continuare
        csvuri = "F:\\TwitterDataSet\\Query_2017.csv"
        query_for_objects = "//div[@class = 'stream']/ol/li[starts-with(@class,'js-stream-item')]"


    tree = data_ingestion(online_mode, url, uri)
    extracted_dictionary_data = data_extraction(query_for_objects, tree, endpoint_index)
    write_data_to_csv(csvuri,extracted_dictionary_data)

#call "python ./WebScienceProject/true_scraper.py onlineMode queryCode"
if __name__=="__main__":
    main(sys.argv[1:])


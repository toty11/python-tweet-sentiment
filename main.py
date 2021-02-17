from threading import Thread
from textblob import TextBlob
import multiprocessing
import codecs

class tp4(Thread):
    def __init__(this, s, end, tab):
        Thread.__init__(this)
        this.s = s
        this.end = end
        this.ld = tab
        this.lr = []

    def run(this):
        j=0
        nbe = (this.end - this.s)
        tab = []
        for i in range(this.s,this.end):
            if i != 0:
                tab = this.ld[i].split(",")
                this.lr.append(this.ld[i][:len(this.ld[i])-2]+''+analyse_sentiment(tab[2])+'\n')   
                    
                
def analyse_sentiment(str):
    blob = TextBlob(str)
    return "%.3f,%.3f" % (blob.sentiment.polarity,blob.sentiment.subjectivity)     
    
def readData(str):
    f = open(str,"r",encoding="utf-8")
    liste = []
    for x in f:
        liste.append(x)
    return liste

def writeData(str, l):
    with codecs.open(str, 'a', 'utf-8') as outfile:
        for x in l:
            outfile.write(x)

def division(taille, nbcoeur):
    while nbcoeur > taille:
        nbcoeur = nbcoeur - 1
    t = []
    tailledivision = taille / nbcoeur
    reste = taille % tailledivision

    for i in range(nbcoeur):
        
        t.append([int(i * tailledivision), int((i + 1) * tailledivision)])

    if reste != 0:
        t[nbcoeur - 1][1] = t[nbcoeur - 1][1] + reste
    return t
        
str_i = "./tweet_data.csv"
str_o = "./tweet_export.csv"
liste = readData(str_i)
liste_r = []
#Entête fichier CSV
liste_r.append("created_at,tweet_id,tweet,likes,retweet_count,source,user_id,user_name,user_screen_name,user_description,user_join_date,user_followers_count,user_location,lat,long,city,country,continent,state,state_code,collected_at,ORIGIN,sexe,polarity,subjectivity\n")
nbproc = multiprocessing.cpu_count()
liste_tache = division(len(liste),nbproc)
threads = []

for i in range(0,nbproc):
    threads.append(tp4(liste_tache[i][0],liste_tache[i][1],liste))

for thread in threads:
    print("thread start")
    thread.start()

for thread in threads:
    print("thread join")
    thread.join()

for thread in threads:
    for l in thread.lr:
        liste_r.append(l)
    
writeData(str_o,liste_r)
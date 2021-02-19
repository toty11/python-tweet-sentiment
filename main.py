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
        this.nbCalculZero = 0

    def run(this):
        j=0
        nbe = (this.end - this.s)
        tab = []
        for i in range(this.s,this.end):
            if i != 0:
                tab = this.ld[i].split(",")
                #sentiment = analyse_sentiment(tab[2]).split(',')   
                this.lr.append(this.ld[i][:len(this.ld[i])-3]+''+analyse_sentiment(tab[0])+'\n')

                # if float(sentiment[0]) != 0.000 and float(sentiment[1]) != 0.000:   
                #     this.lr.append(this.ld[i][:len(this.ld[i])-2]+''+analyse_sentiment(tab[2])+'\n')   
                # else:                    
                #     this.nbCalculZero += 1 
                
def analyse_sentiment(str):
    blob = TextBlob(str)
    return "%.3f,%.3f," % (blob.sentiment.polarity,blob.sentiment.subjectivity)     
    
def readData(str):
    f = open(str,"r",encoding="utf-8")
    liste = []
    for x in f:
        liste.append(x)
    return liste

def writeData(str, l):
    with codecs.open(str, 'a+', 'utf-8') as outfile:
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
        
str_i = "./echantillion_tweet_csv.csv"
str_o = "./tweet_export.csv"
liste = readData(str_i)
liste_r = []
#Entête fichier CSV
liste_r.append("tweet,likes,retweet_count,state,state_code,ORIGIN,polarity,subjectivity\n")
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

nbSentimentZero = 0
for thread in threads:
    print(thread.nbCalculZero)
    nbSentimentZero += thread.nbCalculZero
    for l in thread.lr:
        liste_r.append(l)
    
print(nbSentimentZero)
writeData(str_o,liste_r)
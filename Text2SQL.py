import time
import os
import MySQLdb

ID=0
XX=-1


def READER(message):
    rcv=message.find('To:')
    endrcv=message[rcv:].find("\n")
    Rcvr=(message[rcv+len('To:'):rcv+endrcv].replace("\r","").replace(" ",""))       #Receiver
    snd=message.find('From:')
    endsnd=message[snd:].find("\n")
    Sender=(message[snd+len('From:'):snd+endsnd].replace("\r","").replace(" ",""))       #Sender
    #print(Rcvr,Sender)
    sbjt=message.find('Subject:')
    endsbjt=message[sbjt:].find("\n")
    Subject=(message[sbjt+len('Subject:'):sbjt+endsbjt].replace("\r","").replace(" ",""))       #Subject
    date=message.find('Date:')
    enddate=message[date:].find("\n")
    Date=(message[date+len('Date:'):date+enddate].replace("\r","").replace(" ",""))       #Date
    
    actptr=message.find('X-FileName:') #Actual Text Pointer
    actstr=message[actptr:].find("\n")  #Actual Text Start
    endtext=message[actptr+actstr:].find("--Original Message--")
    endNest=message[actptr+actstr:].find("\n>")
            
    if endtext == -1 and endNest == -1:
        Text=message[actptr+actstr:]
    elif endNest == -1:
        Text=message[actptr+actstr:actptr+actstr+endtext]
    else:
        Text=message[actptr+actstr:actptr+actstr+endNest]    
    return([Sender,Rcvr,Date,Subject,Text])

Emails=[]
directories = '.'
for x in os.walk('.'):
    for D in x[2]:
        if D[-1]=='.':
            Emails.append(x[0]+'/'+D)
            
X=0

j=1
print(len(Emails))
print(len(max(Emails)))
while j<len(Emails):
    db=MySQLdb.connect(host='',user='',passwd='',db='enron') #Add HostName, UserName,Password
    cursor=db.cursor()
    for D in Emails:
        if j%500==0:
            print(j)
        if j>X:
            try:
                f= open(D,'r')
                message=f.read()
                RTn=READER(message)
                SQL="INSERT into enron.Corpus (Path,Sender,Receiver,Date,Subject,Text) Values (%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE Text=Text"
                cursor.execute(SQL,(D,RTn[0],RTn[1],RTn[2],RTn[3],RTn[4]))
                db.commit()
            except:
                break
        j=j+1                                                    
    X=j
    cursor.close()
    db.close()    

               
            
                
import socket
import tweepy

HOST='localhost'
PORT=9009

s=socket.socket()
s.bind((HOST,PORT))
print(f'aguardando conexao na porta:{PORT}')

s.listen(5)
connection,adress = s.accept()
print(f'Rcebendo solicitacao de {adress}')

token ='AAAAAAAAAAAAAAAAAAAAAEbsiwEAAAAAyKIUYMPeFeUB%2Bz5xoRaegZ%2B0aYI%3D1U8sVpp31K5J38hhmfuYWpXmM5KBF5yl01isAhbDluidg1dhKJ'
keyword='Presidente do Brasil'

class GetTweets(tweepy.StreamingClient):
    def on_tweet(self, tweet):
        print(tweet.text)
        print('='*50)
        connection.send(tweet.text.encode('latin1','ignore'))

printer = GetTweets(token)
printer.add_rules(tweepy.StreamRule(keyword))        
printer.filter()

connection.close()


import random

from kafka import KafkaProducer

s_nouns = ["A dude", "My mom", "The king", "Some guy", "A cat with rabies", "A sloth", "Your homie",
           "This cool guy my gardener met yesterday", "Superman"]
p_nouns = ["These dudes", "Both of my moms", "All the kings of the world", "Some guys", "All of a cattery's cats",
           "The multitude of sloths living under your bed", "Your homies", "Like, these, like, all these people",
           "Supermen"]
s_verbs = ["eats", "kicks", "gives", "treats", "meets with", "creates", "hacks", "configures", "spies on", "retards",
           "meows on", "flees from", "tries to automate", "explodes"]
p_verbs = ["eat", "kick", "give", "treat", "meet with", "create", "hack", "configure", "spy on", "retard", "meow on",
           "flee from", "try to automate", "explode"]
infinitives = ["to make a pie.", "for no apparent reason.", "because the sky is green.", "for a disease.",
               "to be able to make toast explode.", "to know more about archeology."]


def sing_sen_maker():
    '''Makes a random sentence from the different parts of speech. Uses a SINGULAR subject'''
    sen = random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(
        p_nouns).lower(), random.choice(infinitives)
    return ' '.join(sen) # convert tuple to string

if __name__ == "__main__":
    topic = 'wordcount'
    brokers = 'localhost:9092'

    producer = KafkaProducer(bootstrap_servers=[brokers])

    for item in range(0, 100):
        value = sing_sen_maker()
        print(value)
        producer.send(topic, value)
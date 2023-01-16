import pandas as pd
import random
import datetime

def generate_dummy_info():
    dict_pr={}
    list_products=['iphone11','iphone12','iphone13','iphoneSE','IpadMax','IpadMini','laptop256','Macbook512','galaxy10','galaxy11','galaxy12','galaxy13','watch320','watch340','Nk320','Nk400','Nk500']
    for i in list_products:
        dict_pr[i]= (random.randint(5555555, 9999999))/100
    return dict_pr.items()



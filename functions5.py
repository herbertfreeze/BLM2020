import pandas as pd
import re
from itertools import combinations_with_replacement, combinations
from collections import Counter
import numpy as np
import shutil
import imagehash
from PIL import Image

def multicore_move(p):
    shutil.move(*p)

def get_phash(f):
    try:
        hashfunc = imagehash.phash
        phash = str( hashfunc(Image.open(f).convert("RGBA")) )
        return phash
    except:
        return Nones

def phash_dataframe(df):
    df["phash"] = df.file.apply(lambda x: get_phash(x))
    df.to_pickle("hashes/{}.pkl".format(df.name.iloc[1]))
    return df
    
def list_to_tuples(x):
    return list( combinations(sorted(x),2) )

def update_counter(x, hashtag_dict):
    hashtag_dict += Counter( list_to_tuples(x)  )
    
def get_hash_counter(df):
    hashtag_dict = Counter()
    df.hashtags.apply(update_counter, args=(hashtag_dict,))
    return hashtag_dict

def move_file(p):
    src, tgt = p
    shutil.move(src, tgt)

def get_hash_oneshot(df):
    df["tuple_combos"] = df.hashtags.apply(list_to_tuples)
    hashtups = [item for sublist in df.tuple_combos.values for item in sublist]
    return Counter(hashtups)

def update_comments_with_metadata(r):
    for d in r.answers:
        d.update({"shortcode": r.shortcode, 
                  "is_reply" : True,
                  "response_to_userid": r.comment_userid, 
                  "response_to_username": r.comment_username, 
                  "response_to_comment": r.id})
    return r.answers

## Comments Processing
def process_comments(f):
    try:
        A = pd.read_json(f)
        A["shortcode"] = f.split("F:/Data-Dumps/BLM/Comments\\")[-1].split("=")[0]
        A["comment_userid"] = A.owner.apply(lambda x: x["id"])
        A["comment_username"] = A.owner.apply(lambda x: x["username"])
        A["is_reply"] = False
        A["reply_to_userid"] = np.nan
        A["reply_to_username"] = np.nan
        A["reply_to_comment_id"] = np.nan
        ## Unpack responses
        try:
            B = A[ A.answers.apply(lambda x: len(x) > 1) ].copy()
            B["answers"] = B.apply(update_comments_with_metadata, axis = 1)
            B = pd.DataFrame(  [item for sublist in B.answers.values for item in sublist]  )
            B["comment_userid"] = B.owner.apply(lambda x: x["id"])
            B["comment_username"] = B.owner.apply(lambda x: x["username"])
        except:
            B=pd.DataFrame()
        A = pd.concat((A,B))
        del A["owner"]
        del A["answers"]
        return A
    except:
        return pd.DataFrame()
    
def process_comment_users(f):
    pass
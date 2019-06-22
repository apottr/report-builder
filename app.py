import boto3,re
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path

tmp = Path(__file__).parent.resolve() / "tmp"

def initialize():
    s3 = boto3.resource("s3")
    return s3.Bucket("collector-storage")

def pd_load_dataframe(file,key):
    df = pd.read_csv(str(tmp.parent / file))
    df["address"] = df[key].apply(lambda x: re.sub(r"[\/\?:\.\\&#]","_",x))
    return df

def pd_grab_selector(df,url):
    series = df.loc[lambda x: x['address'] == url]
    return str(series.selectors.values[0])

def loader(soup,sel):
    expr = lambda x: len(re.split(r"\s",x)) > 3
    filtered = filter(expr,[re.sub(r"\s{2,}","",i.text) for i in soup.select(sel)])
    #list comprehension: subsitute any 2 or more whitespace characters with 
    # an empty character, for all titles from the selector.
    #filter: exclude all titles less than 2 words long.
    deduped = set(filtered) #remove duplicate entries in result
    return "\n\n".join(deduped)

def get_html(body,selector):
    return loader(BeautifulSoup(body,'html.parser'),selector)

def get_xml(body):
    return loader(BeautifulSoup(body,'lxml'),"item > title")

def list_news_objects(resource):
    uuid = "0fe7c2ad-f12c-461e-ab93-a580072fe255"
    affiliate = pd_load_dataframe("aff-final.csv","website")
    for obj in resource.objects.filter(Prefix=f"news/{uuid}/2019/6"):
        bobj = obj.get()
        body = bobj["Body"]
        out = ""
        url = obj.key.split("/")[-1]
        if "patch" in url:
            out = get_xml(body)
        else:
            selector = pd_grab_selector(affiliate,url)
            out = get_html(body,selector)
        with open(tmp / f"{url}.txt","a+") as f:
            print(obj.key)
            f.write(obj.key)
            f.write(out)
        


if __name__ == "__main__":
    bucket = initialize()
    list_news_objects(bucket)
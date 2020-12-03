from location_analysis.api_function import *
from sys import platform
import pandas as pd
import gzip
import pickle
import requests

# Choosing platform
root = ""
if platform == "darwin":
    root = "/Users/jy"
elif platform == "win32":
    root = "D:"

path_government_office = file_directory

print("Reading {}전국공공시설개방정보표준데이터.csv".format(path_government_office))
df_info_government_office = pd.read_csv("{}전국공공시설개방정보표준데이터.csv".format(path_government_office), encoding="ms949")
df_info_government_office = df_info_government_office[df_info_government_office["제공기관명"].notnull()].reset_index(drop=True)
df_info_government_office["개방장소명_extract"] = df_info_government_office["개방장소명"].str.extract("(주민센터|구청)")
df_info_government_office = df_info_government_office[df_info_government_office["개방장소명_extract"].notnull()].reset_index(drop=True).drop(["개방장소명_extract"], axis=1)
df_info_government_office = df_info_government_office[['개방장소명', '소재지도로명주소', '관리기관명', '사용안내전화번호', '홈페이지주소', '위도', '경도']].copy()

df_export = pd.DataFrame(columns=['address_name', 'category_group_code', 'category_group_name',
                                  'category_name', 'distance', 'id', 'phone', 'place_name', 'place_url',
                                  'road_address_name', 'x', 'y'])
########################################################################################################################
for ind in df_info_government_office.index:
    print("관공서) Adding latitude and longitude for index {} ({})..".format(ind, df_info_government_office.shape[0]))
    name = df_info_government_office.loc[ind, "개방장소명"]

    url = "url"
    headers = {"authorization": "key"}
    r = requests.get(url, headers=headers, params={"query": "{}".format(name)}).json()

    if len(r.get("documents")) != 0:
        for l in r.get("documents"):
            df_export = df_export.append(pd.DataFrame(data=[l]), sort=False, ignore_index=True)
            del l
    del name, url, headers, r
########################################################################################################################
with gzip.open("{}df_government_office_with_kakao_api.pickle".format(path_government_office), "wb") as f:
    print("Exporting {}df_government_office_with_kakao_api.pickle".format(path_government_office))
    pickle.dump(df_export, f)
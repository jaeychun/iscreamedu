from location_analysis.api_function import *
from sys import platform
import pandas as pd
import gzip
import pickle
import requests
import numpy as np
from functools import reduce

# Choosing platform
root = ""
if platform == "darwin":
    root = "/Users/jy"
elif platform == "win32":
    root = "D:"

map_city_name = {"강원도": "강원",
                 "경기도": "경기",
                 "경상남도": "경남",
                 "경상북도": "경북",
                 "광주광역시": "광주",
                 "대구광역시": "대구",
                 "대전광역시": "대전",
                 "부산광역시": "부산",
                 "서울특별시": "서울",
                 "울산광역시": "울산",
                 "인천광역시": "인천",
                 "전라남도": "전남",
                 "전라북도": "전북",
                 "제주특별자치도": "제주",
                 "충청남도": "충남",
                 "충청북도": "충북"}

path_school = file_directory
year = 2019
file_name_elementary = ""
file_name_middle = ""
if year == 2019:
    file_name_elementary += "2019년도_학교기본정보_초등학교"
    file_name_middle += "2019년도_학교기본정보_중학교"
elif year == 2020:
    file_name_elementary += "학교기본정보(초)"
    file_name_middle += "학교기본정보(중)"

df_school_map_elementary = pd.DataFrame()
df_school_map_middle = pd.DataFrame()

if year == 2019:
    print("Reading and parsing {}{}.csv".format(path_school, file_name_elementary))
    temp_elementary = pd.read_csv("{}{}.csv".format(path_school, file_name_elementary))
    del file_name_elementary
    temp_elementary.columns = [x.replace(" \n", "") for x in temp_elementary.columns]
    df_school_map_elementary = df_school_map_elementary.append(temp_elementary, sort=False, ignore_index=True)
    del temp_elementary

    print("Reading and parsing {}{}.csv".format(path_school, file_name_middle))
    temp_middle = pd.read_csv("{}{}.csv".format(path_school, file_name_middle))
    del file_name_middle
    temp_middle.columns = [x.replace(" \n", "") for x in temp_middle.columns]
    df_school_map_middle = df_school_map_middle.append(temp_middle, sort=False, ignore_index=True)
    del temp_middle
elif year == 2020:
    print("Reading and parsing {}학교기본정보(초).xlsx".format(path_school))
    temp_elementary = pd.read_excel("{}학교기본정보(초).xlsx".format(path_school))
    temp_elementary.columns = [x.replace(" \n", "") for x in temp_elementary.columns]
    df_school_map_elementary = df_school_map_elementary.append(temp_elementary, sort=False, ignore_index=True)
    del temp_elementary

    print("Reading and parsing {}학교기본정보(중).xlsx".format(path_school))
    temp_middle = pd.read_excel("{}학교기본정보(중).xlsx".format(path_school))
    temp_middle.columns = [x.replace(" \n", "") for x in temp_middle.columns]
    df_school_map_middle = df_school_map_middle.append(temp_middle, sort=False, ignore_index=True)
    del temp_middle

for col in ["학교명_kakao_api", "학교도로명주소_kakao_api", "위도_kakao_api", "경도_kakao_api"]:
    df_school_map_elementary[col] = np.nan
    df_school_map_middle[col] = np.nan
    del col

sum = 1
for dfs in [df_school_map_elementary, df_school_map_middle]:
    print("Changing columns for dataframe {}..".format(sum))
    for col in ["주소내역", "학교도로명 주소"]:
        temp = dfs[col].str.split(" ", expand=True)
        temp[0] = temp[0].map(map_city_name)
        temp = temp.fillna("")
        for ind in temp.index:
            dfs.loc[ind, col] = reduce(lambda x, y: x+y, "{}".format(" ".join(temp.loc[ind, :].tolist()).strip()))
            del ind
        del temp, col
    sum += 1
    del dfs
del sum
########################################################################################################################
for ind in df_school_map_elementary.index:
    print("초등학교) Adding latitude and longitude for index {} ({})..".format(ind, df_school_map_elementary.shape[0]))
    school_name = df_school_map_elementary.loc[ind, "학교명"]
    address_name = df_school_map_elementary.loc[ind, "주소내역"]
    address_name = address_name.replace(" ", "")

    url = "url"
    headers = {"authorization": "key"}
    r = requests.get(url, headers=headers, params={"query": "{}".format(school_name)}).json()

    if len(r.get("documents")) != 0:
        for l in r.get("documents"):
            if school_name in l["place_name"].replace(" ", "") and address_name in l["address_name"].replace(" ", ""):
                df_school_map_elementary.loc[ind, "학교명_kakao_api"] = l["place_name"]
                df_school_map_elementary.loc[ind, "학교도로명주소_kakao_api"] = l["address_name"]
                df_school_map_elementary.loc[ind, "위도_kakao_api"] = l["y"]
                df_school_map_elementary.loc[ind, "경도_kakao_api"] = l["x"]
                del l
                break
    del school_name, address_name, url, headers, r

for ind in df_school_map_middle.index:
    print("중학교) Adding latitude and longitude for index {} ({})..".format(ind, df_school_map_middle.shape[0]))
    school_name = df_school_map_middle.loc[ind, "학교명"]
    address_name = df_school_map_middle.loc[ind, "주소내역"]
    address_name = address_name.replace(" ", "")

    url = "https://dapi.kakao.com/v2/local/search/keyword"
    headers = {"authorization": "KakaoAK 33408e7ae4583400966cb8b5163a5a82"}
    r = requests.get(url, headers=headers, params={"query": "{}".format(school_name)}).json()

    if len(r.get("documents")) != 0:
        for l in r.get("documents"):
            if school_name in l["place_name"].replace(" ", "") and address_name in l["address_name"].replace(" ", ""):
                df_school_map_middle.loc[ind, "학교명_kakao_api"] = l["place_name"]
                df_school_map_middle.loc[ind, "학교도로명주소_kakao_api"] = l["road_address_name"]
                df_school_map_middle.loc[ind, "위도_kakao_api"] = l["y"]
                df_school_map_middle.loc[ind, "경도_kakao_api"] = l["x"]
                del l
                break
    del school_name, url, headers, r
########################################################################################################################
if df_school_map_elementary[df_school_map_elementary["위도_kakao_api"].notnull()].shape[0] != df_school_map_elementary[df_school_map_elementary["경도_kakao_api"].notnull()].shape[0]:
    raise ValueError("초등학교) Need to check 위도, 경도..")
else:
    with gzip.open("{}df_elementary_school_with_kakao_api_{}.pickle".format(path_school, year), "wb") as f:
        print("Exporting {}df_elementary_school_with_kakao_api_{}.pickle".format(path_school, year))
        pickle.dump(df_school_map_elementary, f)

if df_school_map_middle[df_school_map_middle["위도_kakao_api"].notnull()].shape[0] != df_school_map_middle[df_school_map_middle["경도_kakao_api"].notnull()].shape[0]:
    raise ValueError("초등학교) Need to check 위도, 경도..")
else:
    with gzip.open("{}df_middle_school_with_kakao_api_{}.pickle".format(path_school, year), "wb") as f:
        print("Exporting {}df_middle_school_with_kakao_api_{}.pickle".format(path_school, year))
        pickle.dump(df_school_map_middle, f)

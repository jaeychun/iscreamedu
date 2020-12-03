from location_analysis.api_function import *
from sys import platform
import pandas as pd
import gzip
import pickle
from glob import glob
import requests
import numpy as np

# Choosing platform
root = ""
if platform == "darwin":
    root = "/Users/jy"
elif platform == "win32":
    root = "D:"

path_park = file_directory

lst_city_name = ["서울특별시", "인천광역시", "경기도", "충청남도", "충청북도", "대전광역시", "대구광역시"]
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

print("Reading and parsing {}전국도시공원정보표준데이터-20200508.xlsx".format(path_park))
df_info_park = pd.read_excel("{}전국도시공원정보표준데이터-20200508.xlsx".format(path_park))

if "관리번호" in df_info_park.loc[0, :].tolist():
    df_info_park.columns = df_info_park.loc[0, :].tolist()
    df_info_park = df_info_park.drop(0, axis=0).reset_index(drop=True)
df_info_park = df_info_park[["공원명", "공원구분", "위도", "경도", "제공기관명", "소재지도로명주소", "소재지지번주소"]]
df_info_park = df_info_park[df_info_park["제공기관명"] != "세종특별자치시"].reset_index(drop=True)
df_info_park.insert(0, "제공기관명_extract", df_info_park["제공기관명"].str.extract("({})".format("|".join(map_city_name.keys()))))

df_info_park_isnull = df_info_park[df_info_park["제공기관명_extract"].isnull()].copy()
df_info_park_isnull["제공기관명_extract"] = df_info_park_isnull["소재지도로명주소"].str.split(" ", expand=True)[0]
df_info_park_notnull = df_info_park[df_info_park["제공기관명_extract"].notnull()].copy()

df_info_park = pd.concat([df_info_park_isnull, df_info_park_notnull], axis=0).sort_index()
del df_info_park_isnull, df_info_park_notnull

for col in ["공원명_kakao_api", "주소_kakao_api", "위도__kakao_api", "경도_kakao_api"]:
    df_info_park[col] = np.nan
    del col

########################################################################################################################
for ind in df_info_park.index:
    print("Adding latitude and longitude for index {} ({})..".format(ind, df_info_park.shape[0]))
    # 소재지도로주소
    dorojuso = df_info_park.loc[ind, "소재지도로명주소"]
    # 소재지지번주소
    jibun = df_info_park.loc[ind, "소재지지번주소"]

    url = "url"
    headers = {"authorization": "key"}
    r = requests.get(url, headers=headers, params={"query": "{}".format(dorojuso)}).json()

    if len(r.get("documents")) != 0:
        for l in r.get("documents"):
            df_info_park.loc[ind, "공원명_kakao_api"] = l["place_name"]
            df_info_park.loc[ind, "주소_kakao_api"] = l["address_name"]
            df_info_park.loc[ind, "위도__kakao_api"] = l["y"]
            df_info_park.loc[ind, "경도_kakao_api"] = l["x"]
            del l
    else:
        if jibun is np.nan:
            continue
        else:
            r = requests.get(url, headers=headers, params={"query": "{}".format(jibun)}).json()
            for l in r.get("documents"):
                df_info_park.loc[ind, "공원명_kakao_api"] = l["place_name"]
                df_info_park.loc[ind, "주소_kakao_api"] = l["address_name"]
                df_info_park.loc[ind, "위도__kakao_api"] = l["y"]
                df_info_park.loc[ind, "경도_kakao_api"] = l["x"]
                del l
    del dorojuso, jibun, url, headers, r

if df_info_park[df_info_park["위도_kakao_api"].notnull()].shape[0] != df_info_park[df_info_park["경도_kakao_api"].notnull()].shape[0]:
    raise ValueError("Need to check 위도, 경도..")
else:
    with gzip.open("{}df_park_with_kakao_api.pickle".format(path_park), "wb") as f:
        print("Exporting {}df_park_with_kakao_api.pickle".format(path_park))
        pickle.dump(df_info_park, f)
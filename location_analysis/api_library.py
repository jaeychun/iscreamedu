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

path_library = file_directory

print("Reading and parsing {}전국도서관표준데이터.xls".format(path_library))
df_info_library = pd.read_excel("{}전국도서관표준데이터.xls".format(path_library))
print("Changing some coordinates..")
df_info_library.insert(0, "도서관명_no_space_lower", df_info_library["도서관명"].str.replace(" ", "").str.lower())
df_info_library = df_info_library.drop(df_info_library[(df_info_library["시도명"] == "서울특별시") & (df_info_library["시군구명"] == "관악구") &
                                                       (df_info_library["도서관명_no_space_lower"] == "조원작은도서관")].index, axis=0).reset_index(drop=True)

df_export = pd.DataFrame(columns=['place_name', 'address_name', 'road_address_name', 'y', 'x'])
########################################################################################################################
for ind in df_info_library.index:
    print("도서관) Adding latitude and longitude for index {} ({})..".format(ind, df_info_library.shape[0]))
    name_one = df_info_library.loc[ind, "도서관명_no_space_lower"]
    name_two = df_info_library.loc[ind, "도서관명"]

    url = "url"
    headers = {"authorization": "key"}
    r = requests.get(url, headers=headers, params={"query": "{}".format(name_one)}).json()

    if len(r.get("documents")) != 0:
        for l in r.get("documents"):
            df_export = df_export.append(
                pd.DataFrame(data=[l])[["place_name", "address_name", "road_address_name", "y", "x"]],
                sort=False, ignore_index=True)
            del l
    else:
        r = requests.get(url, headers=headers, params={"query": "{}".format(name_two)}).json()
        for l in r.get("documents"):
            df_export = df_export.append(
                pd.DataFrame(data=[l])[["place_name", "address_name", "road_address_name", "y", "x"]],
                sort=False, ignore_index=True)
            del l
    del name_one, name_two, url, headers, r
########################################################################################################################
with gzip.open("{}df_library_with_kakao_api.pickle".format(path_library), "wb") as f:
    print("Exporting {}df_library_with_kakao_api.pickle".format(path_library))
    pickle.dump(df_export, f)
def export_pickle(file_name=None, df=None):
    import gzip
    import pickle

    with gzip.open(file_name, "wb") as f:
        print("Exporting {}".format(file_name))
        pickle.dump(df, f)
        del f


def import_pickle(file_name=None):
    import gzip
    import pickle

    with gzip.open(file_name, "rb") as f:
        print("Reading {}".format(file_name))
        df = pickle.load(f)
        del f

    return df


def korea_region():
    import pandas as pd
    import numpy as np
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory
    df = pd.read_excel(path_import).rename(columns={"시도": "SIDO", "시군구": "SIGUNGU"})
    df.loc[df[df["SIGUNGU"] == "세종특별자치시"].index.tolist(), "SIDO"] = "세종특별자치시"
    df_g = df.groupby(["SIDO"])["SIGUNGU"].count().reset_index().drop(["SIGUNGU"], axis=1).rename(columns={"시도": "SIDO"})

    df_g["SIDO_length"] = df_g["SIDO"].apply(lambda x: len(x))
    df_g["SIDO_1"] = np.nan
    df_g.loc[df_g[df_g["SIDO_length"] == 3].index.tolist(), "SIDO_1"] = df_g.loc[df_g[df_g["SIDO_length"] == 3].index.tolist(), "SIDO"].apply(lambda x: x[:2])
    df_g.loc[df_g[df_g["SIDO_length"] == 4].index.tolist(), "SIDO_1"] = df_g.loc[df_g[df_g["SIDO_length"] == 4].index.tolist(), "SIDO"].apply(lambda x: x[0] + x[2])
    df_g.loc[df_g[df_g["SIDO_length"] == 5].index.tolist(), "SIDO_1"] = df_g.loc[df_g[df_g["SIDO_length"] == 5].index.tolist(), "SIDO"].apply(lambda x: x[:2])
    df_g.loc[df_g[df_g["SIDO_length"] == 7].index.tolist(), "SIDO_1"] = df_g.loc[df_g[df_g["SIDO_length"] == 7].index.tolist(), "SIDO"].apply(lambda x: x[:2])

    df_g["SIDO_2"] = np.nan
    df_g.loc[df_g[df_g["SIDO_length"] == 5].index.tolist(), "SIDO_2"] = df_g.loc[df_g[df_g["SIDO_length"] == 5].index.tolist(), "SIDO"].apply(lambda x: x[:2] + x[-1])
    df_g.loc[df_g[df_g["SIDO_length"] == 7].index.tolist(), "SIDO_2"] = df_g.loc[df_g[df_g["SIDO_length"] == 7].index.tolist(), "SIDO"].apply(lambda x: x[:2] + x[-1])

    df_g.drop(["SIDO_length"], axis=1, inplace=True)

    df = pd.merge(df, df_g, on=["SIDO"], how="inner")
    del df_g

    return df


def total_t_itf_user(dbname="homelearnfriendsmall"):
    from glob import glob
    import gzip
    import pickle
    import pandas as pd
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory

    print("Reading recent every file for df_t_itf_user..")
    df_t_itf_user = pd.DataFrame()
    for file in [x for x in sorted(glob(path_import)) if "df_t_itf_user" in x]:
        with gzip.open(file, "rb") as f:
            df_t_itf_user = df_t_itf_user.append(pickle.load(f), sort=False, ignore_index=True)
    df_t_itf_user.dropna(axis=1, how="all", inplace=True)
    df_t_itf_user.drop_duplicates(["USERNO"], keep="last", inplace=True)

    if df_t_itf_user["USERNO"].nunique() != df_t_itf_user.shape[0]:
        raise ValueError("USERNOs in df_t_itf_user are not unique..")

    print("Checking if 'USERKIND', 'HOMERUNID', 'HOMERUNNO' are unique for unique USERNOs..")
    check = df_t_itf_user.groupby(["USERNO"]).agg({"USERKIND": pd.Series.nunique,
                                                   "HOMERUNID": pd.Series.nunique,
                                                   "HOMERUNNO": pd.Series.nunique}).reset_index()
    for col in check.columns.tolist()[1:]:
        if check[col].sum() != df_t_itf_user.shape[0]:
            raise ValueError("{} is not unique for unique USERNOs in df_t_itf_user..")
        del col
    print("'USERKIND', 'HOMERUNID', 'HOMERUNNO' are unique for unique USERNOs..")
    del check

    if df_t_itf_user["USERNO"].nunique() != df_t_itf_user.shape[0]:
        raise ValueError("USERNOs in df_t_itf_user are not unique..")

    return df_t_itf_user


def recent_t_member(dbname="homelearnfriendsmall"):
    from glob import glob
    import gzip
    import pickle
    import pandas as pd
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory

    df_t_itf_user = total_t_itf_user()

    file = [x for x in sorted(glob(path_import)) if "df_t_member" in x][-1]
    print("(recent df_t_member file for df_t_member..)\nReading {}".format(file))
    with gzip.open(file, "rb") as f:
        df_t_member = pickle.load(f)
    for col in df_t_member.columns:
        if df_t_member[df_t_member[col].isnull()].shape[0] == df_t_member.shape[0]:
            df_t_member.drop([col], axis=1, inplace=True)
            continue
        if df_t_member[col].nunique(dropna=False) == 1 and df_t_member[col].unique()[0] == "":
            df_t_member.drop([col], axis=1, inplace=True)
            continue
        if df_t_member[col].nunique(dropna=False) == 2 and "" in df_t_member[col].unique().tolist() and None in df_t_member[col].unique().tolist():
            df_t_member.drop([col], axis=1, inplace=True)

    df_t_member["LEVELIDX"] = df_t_member["LEVELIDX"].map(
        {0: "탈퇴회원", 11: "일반회원", 13: "홈런회원", 126: "임직원"})
    if df_t_member[df_t_member["LEVELIDX"].isnull()].shape[0] != 0:
        raise ValueError("탈퇴회원, 일반 회원 국내, 홈런회워, 일반 회원 국외, 임직원 are not the only values in [LEVELIDX] column..")
    df_t_member["LEVEL_IDX_BEFORE"] = df_t_member["LEVEL_IDX_BEFORE"].map(
        {0: "탈퇴회원", 11: "일반회원", 13: "홈런회원", 126: "임직원"})
    df_t_member["GENDER"] = df_t_member["GENDER"].map({"100": "남", "200": "여"})
    df_t_member["GENDER"].fillna("미제공", inplace=True)
    if df_t_member[df_t_member["GENDER"].isnull()].shape[0] != 0:
        raise ValueError("남, 여, 미제공 are not the only values in [GENDER] column..")
    df_t_member["STATE"] = df_t_member["STATE"].map({"100": "정상", "900": "탈퇴"})
    if df_t_member[df_t_member["STATE"].isnull()].shape[0] != 0:
        raise ValueError("정상 and 탈퇴 are not the only values in [STATE] (회원 상태) column..")

    df_t_member["GUBUN"] = df_t_member["GUBUN"].map({"K": "만 14세 미만", "N": "일반회원", "S": "소셜회원"})
    if df_t_member[df_t_member["GUBUN"].isnull()].shape[0] != 0:
        raise ValueError("만 14세 미만 and 만 14세 이상 are not the only values in [GUBUN] column..")
    df_t_member["SUBS_MEDIA"] = df_t_member["SUBS_MEDIA"].map({"O": "오즈오즈", "H": "홈런프렌즈", "P": "학부모"})
    if df_t_member[df_t_member["SUBS_MEDIA"].isnull()].shape[0] != 0:
        raise ValueError("오즈오즈 and 홈런프렌즈 and 학부모 are not the only values in [SUBS_MEDIA] (가입 매체) column..")

    print("Checking if 'NAME', 'GENDER', 'STATE', 'GUBUN', 'SUBS_MEDIA' are unique for unique USERNOs..")
    check = df_t_member.groupby(["USERNO"]).agg({"NAME": pd.Series.nunique,
                                                 "GENDER": pd.Series.nunique,
                                                 "BIRTHDATE": pd.Series.nunique,
                                                 "ISLUNAR": pd.Series.nunique,
                                                 "STATE": pd.Series.nunique,
                                                 "GUBUN": pd.Series.nunique,
                                                 "SUBS_MEDIA": pd.Series.nunique}).reset_index()

    lst_check_final = check.columns.tolist()[1:]
    lst_check = []
    for col in check.columns.tolist()[1:]:
        if check[col].sum() != check.shape[0]:
            lst_check.append(col)
        del col

    for val in lst_check:
        if val not in ['BIRTHDATE', 'ISLUNAR']:
            raise ValueError("Need to check data fir fir df_t_member..")
        elif val in ['BIRTHDATE', 'ISLUNAR']:
            lst_check_final.remove(val)
        del val
    del lst_check

    for col in lst_check_final:
        if check[col].sum() != df_t_member.shape[0]:
            raise ValueError("{} is not unique for unique USERNOs in df_t_member..".format(col))
        del col
    print("'NAME', 'GENDER', 'STATE', 'GUBUN', 'SUBS_MEDIA' are unique for unique USERNOs..")
    del check

    col_drop = ["MEMBERTYPE", "DUPINFO", "ISLUNAR", "TEL", "MOBILE", "ISSMS", "EMAIL", "ISMAILING", "POST", "ADDRDETAIL",
                "CITY", "STATEORREGION", "COUNTRY", "CONNINFO", "SUBS_MEDIA", "LEVELMANUAL", "RECV_AD_MODDATE",
                "MEMBER_TYPE_BEFORE", "ISMARKETING", "ISADDRMOD"]
    df_t_member.drop(col_drop, axis=1, inplace=True)
    del col_drop

    df_korea_region_sido = korea_region()[["SIDO_1"]].drop_duplicates(keep="first", ignore_index=True)
    df_korea_region_sigungu = korea_region()[["SIGUNGU"]].drop_duplicates(keep="first", ignore_index=True)
    df_address = df_t_member[["ADDR"]].copy().drop_duplicates(keep="first", ignore_index=True)
    df_address = df_address[(df_address["ADDR"].notnull()) & (df_address["ADDR"] != "")].copy()
    df_address["SIDO"] = df_address["ADDR"].str.extract("({})".format("|".join(df_korea_region_sido["SIDO_1"].unique().tolist())))
    df_address["SIGUNGU"] = df_address["ADDR"].str.extract("({})".format("|".join(df_korea_region_sigungu["SIGUNGU"].unique().tolist())))
    df_address = df_address[df_address["SIDO"].notnull()].reset_index(drop=True)
    del df_korea_region_sido, df_korea_region_sigungu
    rows = df_t_member.shape[0]
    df_t_member = pd.merge(df_t_member, df_address, on=["ADDR"], how="left")
    if rows != df_t_member.shape[0]:
        raise ValueError("Need to check ADDR column..")
    else:
        del rows, df_address
        df_t_member = df_t_member[['USERNO', 'NAME', 'GENDER', 'BIRTHDATE', 'ADDR', 'SIDO', 'SIGUNGU', 'LEVELIDX', 'STATE', 'GUBUN', 'LEVEL_IDX_BEFORE']]

    df_t_member["LEVELIDXBEFOREAFTER"] = df_t_member["LEVEL_IDX_BEFORE"] + " > " + df_t_member["LEVELIDX"]
    df_t_member["LEVELIDX_GUBUN"] = df_t_member["LEVELIDX"] + "_" + df_t_member["GUBUN"]

    df_t_itf_user = df_t_itf_user[["USERNO", "HOMERUNID"]]
    df_t_member = pd.merge(df_t_member, df_t_itf_user, on=["USERNO"], how="left")
    del df_t_itf_user
    df_t_member = df_t_member[['USERNO', 'HOMERUNID', 'NAME', 'GENDER', 'BIRTHDATE', 'LEVELIDX_GUBUN',
                               'LEVEL_IDX_BEFORE', 'LEVELIDXBEFOREAFTER', 'STATE', 'SIDO', 'SIGUNGU']]

    return df_t_member


def recent_t_member_with_age(date=None):
    if date is None:
        raise ValueError("Need to specify date in order to calculate age..")
    import numpy as np

    recent_t_member_with_age = recent_t_member()
    print("Creating AGE_BIN and AGE columns..")
    recent_t_member_with_age["BIRTHDATE_LEN"] = recent_t_member_with_age["BIRTHDATE"].str.len()
    ind_nobirthdate = recent_t_member_with_age[recent_t_member_with_age["BIRTHDATE_LEN"] != 10].index.tolist()
    ind_birthdate = recent_t_member_with_age[recent_t_member_with_age["BIRTHDATE_LEN"] == 10].index.tolist()
    for col in ["AGE_BIN", "AGE", "AGE_BIN_ENG"]:
        recent_t_member_with_age[col] = np.nan
        string = ""
        if col != "AGE_BIN_ENG":
            string += "미제공"
        else:
            string += "unavailable"
        recent_t_member_with_age.loc[ind_nobirthdate, col] = string
        del col, string
    if recent_t_member_with_age.shape[0] != len(ind_birthdate) + len(ind_nobirthdate):
        raise ValueError("YYYY-MM-DD and '' are not the only values in BIRTHDATE column..")
    recent_t_member_with_age.drop(["BIRTHDATE_LEN"], axis=1, inplace=True)

    recent_t_member_with_age.loc[ind_birthdate, "AGE_BIN"] = int(date.split("-")[0]) - recent_t_member_with_age.loc[
        ind_birthdate, "BIRTHDATE"].apply(lambda x: int(x.split("-")[0])) + 1
    recent_t_member_with_age.loc[ind_birthdate, "AGE"] = recent_t_member_with_age.loc[ind_birthdate, "AGE_BIN"].astype(str) + " 세"
    recent_t_member_with_age.loc[ind_birthdate, "AGE_BIN"] = recent_t_member_with_age.loc[ind_birthdate, "AGE_BIN"] // 10 * 10
    recent_t_member_with_age.loc[ind_birthdate, "AGE_BIN_ENG"] = recent_t_member_with_age.loc[ind_birthdate, "AGE_BIN"].astype(str) + "s"
    recent_t_member_with_age.loc[ind_birthdate, "AGE_BIN"] = recent_t_member_with_age.loc[ind_birthdate, "AGE_BIN"].astype(str) + "대"
    del ind_birthdate, ind_nobirthdate

    ind_ten_yr_lower = recent_t_member_with_age[recent_t_member_with_age["AGE_BIN"] == "0대"].index.tolist()
    recent_t_member_with_age.loc[ind_ten_yr_lower, "AGE_BIN"] = "10대 미만"
    recent_t_member_with_age.loc[ind_ten_yr_lower, "AGE_BIN_ENG"] = "10under"
    del ind_ten_yr_lower

    recent_t_member_with_age = recent_t_member_with_age[['USERNO', 'HOMERUNID', 'NAME', 'GENDER', 'BIRTHDATE',
                                                         'AGE', 'AGE_BIN', 'AGE_BIN_ENG', 'LEVELIDX_GUBUN', 'LEVEL_IDX_BEFORE',
                                                         'LEVELIDXBEFOREAFTER', 'STATE', 'SIDO', 'SIGUNGU']]

    if recent_t_member_with_age["USERNO"].nunique() != recent_t_member_with_age.shape[0]:
        raise ValueError("USERNO is not unique in recent_t_member_with_age..")

    return recent_t_member_with_age


def recent_t_wish(dbname=None):
    from glob import glob
    import gzip
    import pickle
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory

    file = [x for x in sorted(glob(path_import)) if "df_t_wish" in x][-1]
    print("(recent df_t_wish file for df_t_wish..)\nReading {}".format(file))
    with gzip.open(file, "rb") as f:
        df_t_wish = pickle.load(f)
        df_t_wish["WISH_KIND"] = df_t_wish["WISH_KIND"].map({"W": "관심상품", "L": "좋아요"})
        del f, file
    if df_t_wish["IDX"].nunique() != df_t_wish.shape[0]:
        raise ValueError("IDX in df_t_wish is not unique..")
    if df_t_wish[df_t_wish["WISH_KIND"].isnull()].shape[0] != 0:
        raise ValueError("WISH_KIND columns in df_t_wish do not only contain 'W'(관심상품) and 'L'(좋아요).\n{}".format(
            df_t_wish["WISH_KIND"].unique()))
    df_t_wish.rename(columns={"REGDATE": "REGDATE_WISH"}, inplace=True)

    df_t_wish = df_t_wish.drop_duplicates(["USERNO", "GOODSNO", "CATEIDX"], keep=False, ignore_index=True)

    return df_t_wish


def recent_t_cart_order(dbname=None):
    from glob import glob
    import gzip
    import pickle
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory

    file = [x for x in sorted(glob(path_import)) if "df_t_cart_order" in x][-1]
    print("(recent df_t_cart_order file for df_t_cart..)\nReading {}".format(file))
    with gzip.open(file, "rb") as f:
        df_t_cart_order = pickle.load(f)
        df_t_cart_order = df_t_cart_order[["CARTIDX", "ORIGINALPRICE", "PRICE"]]
        del f, file

    return df_t_cart_order


def recent_t_order_claim(dbname=None):
    from glob import glob
    import gzip
    import pickle
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory

    print("Reading a updated file for df_t_order_claim..")
    with gzip.open([x for x in sorted(glob(path_import)) if "df_t_order_claim" in x][-1], "rb") as f:
        df_t_order_claim = pickle.load(f)
        del f
    df_t_order_claim["USERID"] = df_t_order_claim["USERID"].str.lower()

    return df_t_order_claim


def recent_t_order_info(dbname=None):
    from glob import glob
    import gzip
    import pickle
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory

    print("Reading a updated file for df_t_order_info..")
    with gzip.open([x for x in sorted(glob(path_import)) if "df_t_order_info" in x][-1], "rb") as f:
        df_t_order_info = pickle.load(f)
        del f

    return df_t_order_info


def recent_t_order_option(dbname=None):
    from glob import glob
    import gzip
    import pickle
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory

    print("Reading a updated file for df_t_order_option..")
    file_nm_goods = [x for x in sorted(glob("{}".format(path_import))) if "df_t_order_option" in x][-1]
    with gzip.open(file_nm_goods, "rb") as f:
        df_t_order_option = pickle.load(f)
        del f, file_nm_goods

    df_t_order_option["OPTIONNO > OPTIONNAME > OPTIONITEM"] = df_t_order_option["OPTIONNO"].astype(str) + " > " + df_t_order_option["OPTIONNAME"] + " > " + df_t_order_option["OPTIONITEM"]
    df_t_order_option = df_t_order_option.groupby(["ORDERGOODSIDX", "OPTIONNO > OPTIONNAME > OPTIONITEM"])["IDX"].count().reset_index().drop(
        ["IDX"], axis=1)

    return df_t_order_option


def recent_t_order_delivery(dbname=None):
    from glob import glob
    import gzip
    import pickle
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory

    print("Reading a updated file for df_t_order_info..")
    with gzip.open([x for x in sorted(glob(path_import)) if "df_t_order_delivery" in x][-1], "rb") as f:
        df_t_order_info = pickle.load(f)
        del f

    return df_t_order_info


def recent_t_goods(dbname=None):
    from glob import glob
    import gzip
    import pickle
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory

    print("Reading a updated file for df_goods..")
    file_nm_goods = [x for x in sorted(glob("{}".format(path_import))) if "df_t_goods" in x and "df_t_goods_linked" not in x and "df_t_goods_log" not in x][-1]
    with gzip.open(file_nm_goods, "rb") as f:
        df_goods = pickle.load(f)
        del f, file_nm_goods

    df_goods = df_goods[["NO", "GOODSKIND", "GOODSNAME", "GOODS_EXPANSION_CD"]].rename(columns={"NO": "GOODSNO"})
    df_goods["GOODSKIND"] = df_goods["GOODSKIND"].map({"100": "일반상품", "200": "소모품"})
    if dbname == "conestore":
        df_goods["GOODS_EXPANSION_CD"] = df_goods["GOODS_EXPANSION_CD"].map({"N": "일반상품", "D": "예치금", "M": "모바일 상품권", "T": "체험학습"})
    elif dbname == "homelearnfriendsmall":
        df_goods["GOODS_EXPANSION_CD"] = df_goods["GOODS_EXPANSION_CD"].map({"N": "일반상품", "O": "O", "T": "체험학습"})
    if df_goods[df_goods["GOODSKIND"].isnull()].shape[0] != 0:
        raise ValueError('"There are null GOODSKIND after mapping {"100": "일반상품", "200": "소모품"}..')
    if dbname == "conestore":
        if df_goods[df_goods["GOODS_EXPANSION_CD"].isnull()].shape[0] != 0:
            raise ValueError('"There are null GOODS_EXPANSION_CD after mapping {"N": "일반상품", "D": "예치금", "M": "모바일 상품권", "T": "체험학습"}..')
    elif dbname == "homelearnfriendsmall":
        df_goods["GOODS_EXPANSION_CD"].fillna("NULL", inplace=True)
        if df_goods[df_goods["GOODS_EXPANSION_CD"].isnull()].shape[0] != 0:
            raise ValueError('"There are null GOODS_EXPANSION_CD after mapping {"N": "일반상품", "O": "O", "T": "체험학습"} and "NULL"..')

    return df_goods


def recent_t_goods_linked(dbname=None):
    import pandas as pd
    from glob import glob
    import gzip
    import pickle
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory

    print("Reading a updated file for df_goods..")
    file_nm_goods = [x for x in sorted(glob("{}".format(path_import))) if "df_t_goods_linked" in x][-1]
    with gzip.open(file_nm_goods, "rb") as f:
        df_t_goods_linked = pickle.load(f)
        del f, file_nm_goods
    temp_linkedgoodsno = df_t_goods_linked.groupby(["GOODSNO"])["LINKEDGOODSNO"].apply(list).reset_index()
    temp_sort = df_t_goods_linked.groupby(["GOODSNO"])["SORT"].apply(list).reset_index()
    temp = pd.merge(temp_linkedgoodsno, temp_sort, on=["GOODSNO"], how="outer")
    del temp_linkedgoodsno, temp_sort

    if temp.shape[0] != df_t_goods_linked["GOODSNO"].nunique(dropna=False):
        raise ValueError("Need to check unique GOODSNO in t_goods_linked..")
    else:
        df_t_goods_linked = temp.copy()
        del temp

    return df_t_goods_linked


def recent_t_order_goods(dbname=None):
    from glob import glob
    import pandas as pd
    import gzip
    import pickle
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory

    print("Reading a updated file for df_recent_t_order_goods..")
    with gzip.open([x for x in sorted(glob(path_import)) if "df_t_order_goods_2" in x][-1], "rb") as f:
        df_t_order_goods = pickle.load(f)
        del f

    if dbname == "conestore":
        df_t_order_goods = df_t_order_goods[['IDX', 'ORDERIDX', 'ORDERDELIVERYIDX', 'GOODSNO', 'CATEIDX', 'GOODSCODE', 'GOODSNAME']]
    if dbname == "homelearnfriendsmall":
        df_t_order_goods = df_t_order_goods[['IDX', 'ORDERIDX', 'ORDERDELIVERYIDX', 'DEALERNO', 'GOODSNO', 'CATEIDX', 'GOODSCODE', 'GOODSNAME']]

    return df_t_order_goods


def recent_t_order_goods_option_text(dbname=None):
    from glob import glob
    import gzip
    import pickle
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory

    print("Reading a updated file for df_t_order_goods_option_text..")
    file_nm_goods = [x for x in sorted(glob("{}".format(path_import))) if "df_t_order_goods_option_text" in x][-1]
    with gzip.open(file_nm_goods, "rb") as f:
        df_t_order_goods_option_text = pickle.load(f)
        del f, file_nm_goods

    return df_t_order_goods_option_text


def read_table(table_name=None, host=None, dbname=None, character_set=None, filter_column=None, start_date=None, end_date=None):
    import pymysql as mysql
    import pandas as pd
    host = host
    # user = username
    # password = password
    # port = port_number
    db = mysql.connect(host=host, user=user, passwd=password, port=port_number, charset=character_set, db=dbname)
    cursor = db.cursor()
    sql = ""
    if filter_column is None:
        sql += 'select * from {};'.format(table_name)
    else:
        sql += 'select * from {} WHERE DATE({}) BETWEEN "{}" AND "{}";'.format(table_name, filter_column, start_date, end_date)
    cursor.execute(sql)
    data = cursor.fetchall()
    col = [x[0] for x in cursor.description]
    df = pd.DataFrame(data=data, columns=col)
    del host, dbname, user, password, db, cursor, sql, data, col
    return df


def export_monthly_table(start_date=None, end_date=None, path_export=None, host=None, dbname=None, table_name=[], filter_column=None):
    import gzip
    import pickle
    from datetime import datetime

    for tab_nm in table_name:
        if filter_column is None:
            f_nm = "{}{}/df_{}_{}.pickle".format(path_export, dbname, tab_nm, datetime.today().strftime("%Y-%m-%d_%H_%M_%S"))
        else:
            f_nm = "{}{}/df_{}_{}_{}.pickle".format(path_export, dbname, tab_nm, start_date, end_date)
        df = read_table(table_name=tab_nm, host=host, dbname=dbname, character_set="utf8",
                        filter_column=filter_column, start_date=start_date, end_date=end_date)
        print("Exporting {}\n".format(f_nm))
        with gzip.open(f_nm, "wb") as f:
            pickle.dump(df, f)
            del f
        del f_nm, df, tab_nm


def conestore_monthly_t_user(table_name=None, date=None, dbname=None):
    from glob import glob
    import gzip
    import pickle
    import pandas as pd
    from datetime import datetime
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory
    # path_export = file_directory

    df_t_user = pd.DataFrame()

    print("Creating monthly_t_user for {}..".format(date))
    for file in sorted(glob(path_import)):
        if table_name in file and "log" not in file and "total" not in file and date in file:
            with gzip.open(file, "rb") as f:
                df_t_user = df_t_user.append(pickle.load(f), ignore_index=True, sort=False)
                del f
        del file

    df_t_user["USERKIND"] = df_t_user["USERKIND"].map({0: "0", 1: "관리자", 2: "부운영자", 8: "판매자", 16+1: "회원"})
    if df_t_user[df_t_user["USERKIND"].isnull()].shape[0] != 0:
        raise ValueError('"관리자", "부운영자", "판매자", "회원" are not the only values in USERKIND column..')
    else:
        print("Dropping test id's..")
        df_t_user["USERID"] = df_t_user["USERID"].str.lower()
        df_t_user = df_t_user[~df_t_user["USERID"].str.contains("test|테스트")].reset_index(drop=True)

    f_nm = "{}df_t_user_{}.pickle".format(path_export, date)
    print("Exporting {}\n".format(f_nm))
    with gzip.open(f_nm, "wb") as f:
        pickle.dump(df_t_user, f)
        del f
    del df_t_user


def homelearnfriendsmall_monthly_t_user(table_name=None, date=None, dbname=None):
    from glob import glob
    import gzip
    import pickle
    import pandas as pd
    import numpy as np
    from datetime import datetime
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory
    # path_export = file_directory

    df_t_user = pd.DataFrame()
    print("Creating monthly_t_user for {}..".format(date))
    for file in sorted(glob(path_import)):
        if table_name in file and "log" not in file and "total" not in file and date in file:
            with gzip.open(file, "rb") as f:
                df_t_user = df_t_user.append(pickle.load(f), ignore_index=True, sort=False)
                del f
        del file

    df_t_user["USERKIND"] = df_t_user["USERKIND"].map({0: "비회원", 1: "관리자", 2: "부운영자", 8: "판매자", 16: "회원"})
    if df_t_user[df_t_user["USERKIND"].isnull()].shape[0] != 0:
        raise ValueError('"관리자", "부운영자", "판매자", "회원" are not the only values in USERKIND column..')
    else:
        print("Dropping test id's..")
        df_t_user["USERID"] = df_t_user["USERID"].str.lower()
        df_t_user = df_t_user[~df_t_user["USERID"].str.contains("test|테스트")].reset_index(drop=True)
        df_t_user.insert(df_t_user.columns.tolist().index("USERKIND")+1, "USERKIND_CAT", np.nan)
    for col in ["SOCIAL_CODE", "SOCIAL_ID", "SOCIAL_NAME", "HICLASS_TYPE"]:
        lst_ind_temp = df_t_user[df_t_user[col] == ""].index.tolist()
        df_t_user.loc[lst_ind_temp, col] = np.nan
        del col

    lst_ind_hiclass = df_t_user[df_t_user["SOCIAL_CODE"] == "hiclass"].index.tolist()
    lst_ind_social = df_t_user[(df_t_user["SOCIAL_CODE"] != "") & (df_t_user["SOCIAL_CODE"] != "hiclass")].index.tolist()
    lst_ind_member = df_t_user[df_t_user["SOCIAL_CODE"] == ""].index.tolist()
    if df_t_user.shape[0] != len(lst_ind_hiclass) + len(lst_ind_social) + len(lst_ind_member):
        raise ValueError("df_t_user.shape[0] != len(lst_ind_hiclass) + len(lst_ind_social) + len(lst_ind_member)..")
    if len(set(lst_ind_social).intersection(set(lst_ind_member))) != 0:
        raise ValueError("Need to check social and notsocial indices..")
    if len(set(lst_ind_social).intersection(set(lst_ind_hiclass))) != 0:
        raise ValueError("Need to check social and hiclass indices..")
    if len(set(lst_ind_member).intersection(set(lst_ind_hiclass))) != 0:
        raise ValueError("Need to check notsocial and notsocial indices..")

    df_t_user.loc[lst_ind_social, "USERKIND_CAT"] = "소셜회원"
    df_t_user.loc[lst_ind_member, "USERKIND_CAT"] = "회원"
    df_t_user.loc[lst_ind_hiclass, "USERKIND_CAT"] = "하이클래스"
    df_t_user = df_t_user[["NO", "USERKIND", "USERKIND_CAT", "USERID", "REGDATE", "PW_MOD_DATE"]]

    f_nm = "{}df_t_user_{}.pickle".format(path_export, date)
    print("Exporting {}\n".format(f_nm))
    with gzip.open(f_nm, "wb") as f:
        pickle.dump(df_t_user, f)
        del f
    del df_t_user


def conestore_total_t_user(dbname=None):
    from glob import glob
    import gzip
    import pickle
    import pandas as pd
    from datetime import datetime
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    df_register_date = pd.DataFrame()
    print("Concatenating every data for df_t_user ({})..".format(dbname))
    # file_dir = file_directory
    for file in sorted(glob(file_dir)):
        with gzip.open(file, "rb") as f:
            df_register_date = df_register_date.append(pickle.load(f), ignore_index=True, sort=False)
            del f
        del file
    if df_register_date[df_register_date.duplicated(["NO", "USERID"], keep=False)].shape[0] != 0:
        raise ValueError("There are duplicated USERID in df_register_date..")
    df_register_date["USERID"] = df_register_date["USERID"].str.strip().str.lower()
    df_register_date = df_register_date[["NO", "USERKIND", "USERID", "REGDATE"]].rename(
        columns={"NO": "USERNO", "REGDATE": "JOINDATE"})

    return df_register_date


def homelearnfriendsmall_total_t_user(dbname=None):
    from glob import glob
    import gzip
    import pickle
    import pandas as pd
    from datetime import datetime
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    df_register_date = pd.DataFrame()
    print("Concatenating every data for df_t_user ({})..".format(dbname))
    # file_dir = file_directory
    for file in sorted(glob(file_dir)):
        with gzip.open(file, "rb") as f:
            temp = pickle.load(f)
            if temp.shape[0] == 0:
                continue
            df_register_date = df_register_date.append(temp, ignore_index=True, sort=False)
            del f, temp
        del file
    df_register_date = df_register_date.drop_duplicates(["NO", "USERID"], keep="first").copy().reset_index(drop=True)
    if df_register_date[df_register_date.duplicated(["NO", "USERID"], keep=False)].shape[0] != 0:
        raise ValueError("There are duplicated USERID in df_register_date..")
    df_register_date = df_register_date[["NO", "USERKIND", "USERKIND_CAT", "USERID", "REGDATE"]].rename(
        columns={"NO": "USERNO", "REGDATE": "JOINDATE"})

    return df_register_date


def conestore_monthly_active_user(table_name=None, date=None, dbname=None):
    from glob import glob
    import gzip
    import pickle
    import pandas as pd
    from datetime import datetime
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory
    # path_export = file_directory

    df_register_date = conestore_total_t_user(dbname=dbname)
    df_register_date["JOINDATE"] = df_register_date["JOINDATE"].apply(lambda x: str(x).split(" ")[0])

    print("Reading and appending df_t_user_log for {} in {}..".format(date, dbname))
    df_t_user_log = pd.DataFrame()
    for file in sorted(glob(path_import)):
        if table_name in file and date in file:
            with gzip.open(file, "rb") as f:
                df_t_user_log = df_t_user_log.append(pickle.load(f), ignore_index=True, sort=False)
                del f
        del file

    rows = df_t_user_log.shape[0]
    df_t_user_log = pd.merge(df_t_user_log, df_register_date, on=["USERNO"], how="left")  # merge with df_register_date["NO"] & df_t_user_log["USERNO"]

    if rows != df_t_user_log.shape[0]:
        raise ValueError("IDX columns has duplicates values..")
    else:
        df_t_user_log["USERKIND"].fillna("구분값 없음", inplace=True)
        lst_ind_no_member = df_t_user_log[df_t_user_log["USERKIND"] != "구분값 없음"].copy()
        lst_ind_no_member = lst_ind_no_member[(lst_ind_no_member["USERID"] == "") | (lst_ind_no_member["USERID"].isnull())].copy()
        if len(lst_ind_no_member) > 0:
            raise ValueError("There are 비회원 (null OR "" > USERID) in {}".format(dbname))
        else:
            del lst_ind_no_member
        df_t_user_log = df_t_user_log[(df_t_user_log["USERKIND"] == "회원") | (df_t_user_log["USERKIND"] == "비회원")].reset_index(drop=True)
        print("Dropping test id's..")
        lst_ind_test_id = df_t_user_log[df_t_user_log["USERID"].notnull()].copy()
        lst_ind_test_id["USERID"] = lst_ind_test_id["USERID"].str.lower()
        lst_ind_test_id = lst_ind_test_id[lst_ind_test_id["USERID"].str.contains("test|테스트")].index.tolist()
        df_t_user_log = df_t_user_log.drop(lst_ind_test_id, axis=0).reset_index(drop=True)
        del lst_ind_test_id

        df_t_user_log["REGDATE"] = df_t_user_log["REGDATE"].apply(lambda x: x.strftime("%Y-%m-%d"))
        df_t_user_log["weekNumber"] = df_t_user_log["REGDATE"].apply(
            lambda x: datetime(int(x.split("-")[0]), int(x.split("-")[1]), int(x.split("-")[2])).strftime("%V"))
        df_t_user_log["YYYYMM"] = df_t_user_log["REGDATE"].apply(lambda x: x[:-3])

        f_nm = "{}raw/{}_{}.pickle".format(path_export, table_name, date)
        with gzip.open(f_nm, "wb") as f:
            print("Exporting {}".format(f_nm))
            pickle.dump(df_t_user_log, f)
            del f_nm, f
        del rows, df_register_date

    print("Creating active user dataframe for {}..".format(date))
    df_t_user_log_daily = df_t_user_log.groupby(["REGDATE", "USERKIND"])["USERNO"].nunique().reset_index(name="activeUser").rename(
        columns={"REGDATE": "Date"})
    df_t_user_log_daily.insert(0, "div", "일별")
    df_t_user_log_monthly = df_t_user_log.groupby(["YYYYMM", "USERKIND"])["USERNO"].nunique().reset_index(name="activeUser").rename(
        columns={"YYYYMM": "Date"})
    df_t_user_log_monthly.insert(0, "div", "월별")

    df_t_user_log_summary = pd.concat([df_t_user_log_daily, df_t_user_log_monthly], axis=0,
                                      ignore_index=True)
    del df_t_user_log_daily, df_t_user_log_monthly

    print("Creating engagement rate by weekdays..")
    df_t_user_log_engagement_rate = df_t_user_log.copy()
    df_t_user_log_engagement_rate["weekday"] = df_t_user_log_engagement_rate["REGDATE"].apply(
        lambda x: datetime(year=int(x.split("-")[0]), month=int(x.split("-")[1]), day=int(x.split("-")[2])).strftime("%A"))
    df_t_user_log_engagement_rate = df_t_user_log_engagement_rate.groupby(["REGDATE", "weekday", "USERKIND"])["USERNO"].nunique().reset_index().rename(
        columns={"USERNO": "weekdayActiveUser"})
    df_t_user_log_engagement_rate["MAU"] = df_t_user_log["USERNO"].nunique()
    df_t_user_log_engagement_rate["weekdayEngagementRate"] = df_t_user_log_engagement_rate["weekdayActiveUser"] / df_t_user_log_engagement_rate["MAU"]
    df_t_user_log_engagement_rate["weekday"] = df_t_user_log_engagement_rate["weekday"].map(
        {"Monday": "월요일", "Tuesday": "화요일", "Wednesday": "수요일", "Thursday": "목요일", "Friday": "금요일",
         "Saturday": "토요일", "Sunday": "일요일"})

    print("Creating retention rate dataframe..")
    df_t_user_log_null_join_date = df_t_user_log[df_t_user_log["JOINDATE"].isnull()].copy().reset_index(drop=True)
    df_t_user_log = df_t_user_log[df_t_user_log["JOINDATE"].notnull()].reset_index(drop=True)
    df_t_user_log = df_t_user_log[df_t_user_log["JOINDATE"].str.contains(date)].reset_index(drop=True)
    df_t_user_log["JOINDATE_TO_REGDATE"] = pd.to_datetime(df_t_user_log["REGDATE"]) - pd.to_datetime(df_t_user_log["JOINDATE"])
    df_t_user_log["JOINDATE_TO_REGDATE"] = df_t_user_log["JOINDATE_TO_REGDATE"].apply(lambda x: str(str(x).split(" day")[0]) + " 일차")

    df_t_user_log_retention_rate = df_t_user_log.groupby(["USERKIND", "JOINDATE", "JOINDATE_TO_REGDATE"])["USERNO"].nunique().reset_index()
    df_t_user_log_retention_rate.sort_values(by=["JOINDATE", "JOINDATE_TO_REGDATE"], ascending=[True, True], inplace=True, ignore_index=True)

    if df_t_user_log_null_join_date.shape[0] != 0:
        if df_t_user_log_null_join_date["USERNO"].nunique() != 1 or df_t_user_log_null_join_date["USERNO"].unique()[0] != 1 or \
            df_t_user_log_null_join_date["USERKIND"].nunique() != 1 or df_t_user_log_null_join_date["USERKIND"].unique()[0] != "구분값 없음":
            raise ValueError("Need to check df_t_user_log_null_join_date..")

        f_nm = "{}df_t_user_log_null_join_date_{}.xlsx".format(path_export, date)
        print("Exporting {}".format(f_nm))
        df_t_user_log_null_join_date.to_excel(f_nm, index=False)
        del f_nm, df_t_user_log_null_join_date
    else:
        del df_t_user_log_null_join_date

    f_nm = "{}df_retention_rate_{}.pickle".format(path_export, date)
    print("Exporting {}".format(f_nm))
    with gzip.open(f_nm, "wb") as f:
        pickle.dump(df_t_user_log_retention_rate, f)
        del f, f_nm
    del df_t_user_log_retention_rate

    f_nm = "{}df_active_user_{}.pickle".format(path_export, date)
    print("Exporting {}".format(f_nm))
    with gzip.open(f_nm, "wb") as f:
        pickle.dump(df_t_user_log_summary, f)
        del f, f_nm
    del df_t_user_log_summary

    f_nm = "{}df_weekday_engagement_rate_{}.pickle".format(path_export, date)
    print("Exporting {}\n".format(f_nm))
    with gzip.open(f_nm, "wb") as f:
        pickle.dump(df_t_user_log_engagement_rate, f)
        del f, f_nm
    del df_t_user_log_engagement_rate

    del df_t_user_log


def homelearnfriendsmall_monthly_active_user(table_name=None, date=None, dbname=None):
    from glob import glob
    import gzip
    import pickle
    import pandas as pd
    import numpy as np
    from datetime import datetime
    from functools import reduce
    import math
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory
    # path_export = file_directory

    df_t_member = recent_t_member_with_age(date=date)
    df_register_date = homelearnfriendsmall_total_t_user(dbname=dbname)
    df_register_date["JOINDATE"] = df_register_date["JOINDATE"].apply(lambda x: str(x).split(" ")[0])

    print("Reading and appending df_t_user_log for {} in {}..".format(date, dbname))
    df_t_user_log = pd.DataFrame()
    for file in sorted(glob(path_import)):
        if table_name in file and date in file:
            with gzip.open(file, "rb") as f:
                df_t_user_log = df_t_user_log.append(pickle.load(f), ignore_index=True, sort=False)
                del f
        del file

    rows = df_t_user_log.shape[0]
    df_t_user_log = pd.merge(df_t_user_log, df_register_date, on=["USERNO"], how="left")  # merge with df_register_date["NO"] & df_t_user_log["USERNO"]

    if rows != df_t_user_log.shape[0]:
        raise ValueError("IDX columns has duplicates values..")
    else:
        df_t_user_log["USERKIND"].fillna("구분값 없음", inplace=True)
        df_t_user_log = df_t_user_log[(df_t_user_log["USERKIND"] == "회원") | (df_t_user_log["USERKIND"] == "비회원")].reset_index(drop=True)
        if "비회원" in df_t_user_log["USERKIND"].unique():
            raise ValueError("There is 비회원 in df_t_user_log..")
        print("Dropping test id's(테스트 포함된 USERID) in column USERID..")
        lst_ind_test_id = df_t_user_log[df_t_user_log["USERID"].notnull()].copy()
        lst_ind_test_id["USERID"] = lst_ind_test_id["USERID"].str.lower()
        lst_ind_test_id = lst_ind_test_id[lst_ind_test_id["USERID"].str.contains("테스트")].index.tolist()
        df_t_user_log = df_t_user_log.drop(lst_ind_test_id, axis=0).reset_index(drop=True)
        del lst_ind_test_id

        df_t_user_log["REGDATE"] = df_t_user_log["REGDATE"].apply(lambda x: x.strftime("%Y-%m-%d"))
        df_t_user_log["weekNumber"] = df_t_user_log["REGDATE"].apply(
            lambda x: datetime(int(x.split("-")[0]), int(x.split("-")[1]), int(x.split("-")[2])).strftime("%V"))
        df_t_user_log["YYYYMM"] = df_t_user_log["REGDATE"].apply(lambda x: x[:-3])
        # homelearnfriendsmall 에서 JOINDATE > REGDATE 인것은 비회원 로 간주되며, 비회원에 대한 active user 구하기 어려우므로 제외
        df_t_user_log = df_t_user_log[df_t_user_log["JOINDATE"] <= df_t_user_log["REGDATE"]].copy()

        unique_userno = df_t_user_log["USERNO"].nunique()
        check = df_t_user_log.groupby(["USERNO"])["USERID"].nunique().reset_index()
        if unique_userno == check["USERID"].sum():
            del unique_userno, check
        else:
            raise ValueError("USERID is not unique per USERNO..")

        rows = df_t_user_log.shape[0]
        df_t_user_log = pd.merge(df_t_user_log, df_t_member, on=["USERNO"], how="left")
        if df_t_user_log[df_t_user_log["NAME"].isnull()].shape[0] != 0:
            raise ValueError("There is some null values in NAME column after merging df_t_user_log with df_t_member..")
        if rows != df_t_user_log.shape[0]:
            raise ValueError("Need to check df_t_user_log after merging with df_t_member..")
        else:
            del rows, df_t_member
            df_t_user_log["NAME_lower_nospace"] = df_t_user_log["NAME"].str.lower().str.replace(" ", "")
            print("Dropping test id's in column NAME..")
            df_t_user_log = df_t_user_log[~df_t_user_log["NAME_lower_nospace"].str.contains("test|테스트")].reset_index(drop=True).drop(
                ["NAME_lower_nospace"], axis=1)

        for col in ["LEVELIDX_GUBUN", "USERKIND_CAT", "STATE"]:
            if df_t_user_log[df_t_user_log[col].isnull()].shape[0] != 0:
                raise ValueError("Column {} in df_t_user_log has null values..")
            del col

        check = df_t_user_log.groupby(["USERNO"]).agg({"USERID": pd.Series.nunique,
                                                       "GENDER": pd.Series.nunique,
                                                       "LEVELIDX_GUBUN": pd.Series.nunique,
                                                       "USERKIND_CAT": pd.Series.nunique}).reset_index()
        unique_userno = df_t_user_log["USERNO"].nunique()
        for col in check.columns.tolist()[1:]:
            if check[col].sum() != unique_userno:
                raise ValueError("Column {} is not unique..")
            del col
        del unique_userno, check

        df_t_user_log = df_t_user_log[df_t_user_log["STATE"] == "정상"].reset_index(drop=True)
        df_t_user_log = df_t_user_log[['IDX', 'USERNO', 'IP', 'REGDATE', 'USERKIND', 'USERKIND_CAT',
                                       'LEVELIDX_GUBUN', 'LEVEL_IDX_BEFORE', 'LEVELIDXBEFOREAFTER',
                                       'USERID', 'JOINDATE', 'weekNumber', 'YYYYMM', 'NAME', 'GENDER',
                                       'BIRTHDATE', 'AGE', 'AGE_BIN', 'AGE_BIN_ENG', 'STATE']]
        del df_register_date

        df_t_user_log.insert(df_t_user_log.columns.tolist().index("LEVEL_IDX_BEFORE"), "IDX_CHANGED", np.nan)
        ind_levelidxbeforeafter = df_t_user_log[df_t_user_log["LEVELIDXBEFOREAFTER"].notnull()].index.tolist()
        df_t_user_log.loc[ind_levelidxbeforeafter, "IDX_CHANGED"] = "Y"
        df_t_user_log["IDX_CHANGED"].fillna("N", inplace=True)
        del ind_levelidxbeforeafter

        for col in df_t_user_log.columns:
            if df_t_user_log[df_t_user_log[col].isnull()].shape[0] != 0:
                df_t_user_log[col].fillna("", inplace=True)
            del col

        f_nm = "{}raw/{}_{}.pickle".format(path_export, table_name, date)
        with gzip.open(f_nm, "wb") as f:
            print("Exporting {}".format(f_nm))
            pickle.dump(df_t_user_log, f)
            del f_nm, f

    df_t_user_log["USERKIND_CAT"] = df_t_user_log["USERKIND_CAT"].map({"회원": "member", "소셜회원": "socialmember", "하이클래스": "hiclass"})
    if df_t_user_log[df_t_user_log["USERKIND_CAT"].isnull()].shape[0] != 0:
        raise ValueError("Need to check USERKIND_CAT after mapping..")
    df_t_user_log["USERKIND_CAT_AGE_BIN_ENG"] = df_t_user_log["USERKIND_CAT"] + "_" + df_t_user_log["AGE_BIN_ENG"]

    print("Creating active user dataframe for {}..".format(date))
    df_t_user_log_daily_total = df_t_user_log.groupby(["REGDATE"])["USERNO"].nunique().reset_index(name="activeUser_total").rename(
        columns={"REGDATE": "Date"})
    df_t_user_log_daily_total.insert(0, "div", "일별")

    df_t_user_log_daily_socialcode = df_t_user_log.groupby(["REGDATE", "USERKIND_CAT_AGE_BIN_ENG"])["USERNO"].nunique().reset_index(name="activeUser").rename(
        columns={"REGDATE": "Date"})
    df_t_user_log_daily_socialcode.insert(0, "div", "일별")

    for val in sorted(df_t_user_log_daily_socialcode["USERKIND_CAT_AGE_BIN_ENG"].unique().tolist()):
        temp = df_t_user_log_daily_socialcode[df_t_user_log_daily_socialcode["USERKIND_CAT_AGE_BIN_ENG"] == val].copy().reset_index(drop=True)
        temp = temp.rename(columns={"activeUser": "activeUser_{}".format(val)}).drop(["USERKIND_CAT_AGE_BIN_ENG"], axis=1)
        df_t_user_log_daily_total = pd.merge(df_t_user_log_daily_total, temp, on=["div", "Date"], how="outer")
        df_t_user_log_daily_total["activeUser_{}".format(val)] = df_t_user_log_daily_total["activeUser_{}".format(val)].fillna(0).astype(int)
        del temp
    del df_t_user_log_daily_socialcode

    sum = 0
    for col in df_t_user_log_daily_total.columns.tolist()[df_t_user_log_daily_total.columns.tolist().index("activeUser_total")+1:]:
        sum += df_t_user_log_daily_total[col].sum()
        del col
    if sum != df_t_user_log_daily_total["activeUser_total"].sum():
        raise ValueError("Need to check df_t_user_log_daily for {}..".format(dbname))
    else:
        del sum

    df_t_user_log_monthly_total = df_t_user_log.groupby(["YYYYMM"])["USERNO"].nunique().reset_index(
        name="activeUser_total").rename(columns={"REGDATE": "Date"})

    temp = df_t_user_log.groupby(["USERNO"])["USERKIND_CAT_AGE_BIN_ENG"].nunique().reset_index()
    temp = temp[temp["USERKIND_CAT_AGE_BIN_ENG"] > 1].copy()["USERNO"].unique().tolist()
    if len(temp) != 0:
        df_t_user_log_monthly_total.loc[0, "activeUser_total"] += len(temp)
    del temp
    df_t_user_log_monthly_total.insert(0, "div", "월별")

    df_t_user_log_monthly_userkind = df_t_user_log.groupby(["YYYYMM", "USERKIND_CAT_AGE_BIN_ENG"])["USERNO"].nunique().reset_index(name="activeUser").rename(
        columns={"REGDATE": "Date"})
    df_t_user_log_monthly_userkind.insert(0, "div", "월별")

    for val in sorted(df_t_user_log_monthly_userkind["USERKIND_CAT_AGE_BIN_ENG"].unique().tolist()):
        temp = df_t_user_log_monthly_userkind[df_t_user_log_monthly_userkind["USERKIND_CAT_AGE_BIN_ENG"] == val].copy().reset_index(drop=True)
        temp = temp.rename(columns={"activeUser": "activeUser_{}".format(val)}).drop(["USERKIND_CAT_AGE_BIN_ENG"],axis=1)
        df_t_user_log_monthly_total = pd.merge(df_t_user_log_monthly_total, temp, on=["div", "YYYYMM"], how="outer")
        df_t_user_log_monthly_total["activeUser_{}".format(val)] = df_t_user_log_monthly_total["activeUser_{}".format(val)].fillna(0).astype(int)
        del temp
    del df_t_user_log_monthly_userkind

    sum = 0
    for col in df_t_user_log_monthly_total.columns.tolist()[df_t_user_log_monthly_total.columns.tolist().index("activeUser_total") + 1:]:
        sum += df_t_user_log_monthly_total[col].sum()
        del col
    if sum != df_t_user_log_monthly_total["activeUser_total"].sum():
        raise ValueError("Need to check df_t_user_log_monthly_total for {}..".format(dbname))
    else:
        del sum
        df_t_user_log_monthly_total.rename(columns={"YYYYMM": "Date"}, inplace=True)

    df_t_user_log_summary = pd.concat([df_t_user_log_daily_total, df_t_user_log_monthly_total], axis=0, ignore_index=True)
    del df_t_user_log_daily_total, df_t_user_log_monthly_total

    print("Creating engagement rate by weekdays..")
    print("1. Total..")
    df_t_user_log_engagement_rate = df_t_user_log.copy()
    df_t_user_log_engagement_rate["weekday"] = df_t_user_log_engagement_rate["REGDATE"].apply(
        lambda x: datetime(year=int(x.split("-")[0]), month=int(x.split("-")[1]), day=int(x.split("-")[2])).strftime("%A"))

    df_t_user_log_engagement_rate_total = df_t_user_log_engagement_rate.groupby(["YYYYMM", "REGDATE", "weekday"])[
        "USERNO"].nunique().reset_index().rename(columns={"USERNO": "weekdayActiveUser_total"})
    df_t_user_log_engagement_rate_total["MAU"] = df_t_user_log["USERNO"].nunique()
    df_t_user_log_engagement_rate_total["weekdayEngagementRate_total"] = df_t_user_log_engagement_rate_total["weekdayActiveUser_total"] / df_t_user_log_engagement_rate_total["MAU"]
    df_t_user_log_engagement_rate_total["weekday"] = df_t_user_log_engagement_rate_total["weekday"].map(
        {"Monday": "월요일", "Tuesday": "화요일", "Wednesday": "수요일", "Thursday": "목요일", "Friday": "금요일",
         "Saturday": "토요일", "Sunday": "일요일"})
    df_t_user_log_engagement_rate_total = df_t_user_log_engagement_rate_total[["YYYYMM", "REGDATE", "weekday", "MAU", "weekdayActiveUser_total", "weekdayEngagementRate_total"]]

    print("2. USERKIND_CAT_AGE_BIN_ENG..")
    df_t_user_log_engagement_rate_userkind = df_t_user_log_engagement_rate.groupby(["YYYYMM", "REGDATE", "weekday", "USERKIND_CAT_AGE_BIN_ENG"])["USERNO"].nunique().reset_index().rename(
        columns={"USERNO": "weekdayActiveUser"})
    df_t_user_log_engagement_rate_userkind["weekday"] = df_t_user_log_engagement_rate_userkind["weekday"].map(
        {"Monday": "월요일", "Tuesday": "화요일", "Wednesday": "수요일", "Thursday": "목요일", "Friday": "금요일",
         "Saturday": "토요일", "Sunday": "일요일"})
    for val in sorted(df_t_user_log_engagement_rate_userkind["USERKIND_CAT_AGE_BIN_ENG"].unique().tolist()):
        temp = df_t_user_log_engagement_rate_userkind[df_t_user_log_engagement_rate_userkind["USERKIND_CAT_AGE_BIN_ENG"] == val].copy().reset_index(drop=True)
        temp = temp.rename(columns={"weekdayActiveUser": "weekdayActiveUser_{}".format(val)}).drop(["USERKIND_CAT_AGE_BIN_ENG"], axis=1)
        df_t_user_log_engagement_rate_total = pd.merge(df_t_user_log_engagement_rate_total, temp, on=["YYYYMM", "REGDATE", "weekday"], how="outer")
        df_t_user_log_engagement_rate_total["weekdayActiveUser_{}".format(val)] = df_t_user_log_engagement_rate_total["weekdayActiveUser_{}".format(val)].fillna(0).astype(int)
        df_t_user_log_engagement_rate_total["weekdayEngagementRate_{}".format(val)] = df_t_user_log_engagement_rate_total["weekdayActiveUser_{}".format(val)] / df_t_user_log_engagement_rate_total["MAU"]
        del temp
    del df_t_user_log_engagement_rate_userkind

    df_t_user_log_engagement_rate = df_t_user_log_engagement_rate_total.copy()
    del df_t_user_log_engagement_rate_total

    print("Creating retention rate dataframe..")
    df_t_user_log_null_join_date = df_t_user_log[df_t_user_log["JOINDATE"].isnull()].copy().reset_index(drop=True)
    df_t_user_log = df_t_user_log[df_t_user_log["JOINDATE"].notnull()].reset_index(drop=True)
    df_t_user_log["JOINDATE_TO_REGDATE"] = pd.to_datetime(df_t_user_log["REGDATE"]) - pd.to_datetime(df_t_user_log["JOINDATE"])
    df_t_user_log["JOINDATE_TO_REGDATE"] = df_t_user_log["JOINDATE_TO_REGDATE"].apply(lambda x: int(str(x).split(" day")[0]))
    if df_t_user_log[df_t_user_log["JOINDATE_TO_REGDATE"] < 0].shape[0] != 0:
        raise ValueError("Need to check JOINDATE_TO_REGDATE on df_t_user_log..")

    df_t_user_log_retention_rate = df_t_user_log.copy()
    df_t_user_log_retention_rate["JOINDATE_TO_REGDATE"] = df_t_user_log_retention_rate["JOINDATE_TO_REGDATE"].astype(str) + " 일차"

    print("1. Total..")
    df_t_user_log_retention_rate_total = df_t_user_log_retention_rate.groupby(["JOINDATE", "JOINDATE_TO_REGDATE", "AGE_BIN", "USERKIND_CAT", "LEVELIDX_GUBUN"])[
        "USERNO"].nunique().reset_index().rename(columns={"USERNO": "USERNO_total"})
    df_t_user_log_retention_rate_total.sort_values(by=["JOINDATE", "JOINDATE_TO_REGDATE"], ascending=[True, True],
                                                   inplace=True, ignore_index=True)

    df_t_user_log_retention_rate_total = df_t_user_log_retention_rate_total[
        ['JOINDATE', 'JOINDATE_TO_REGDATE', 'AGE_BIN', 'USERKIND_CAT', 'LEVELIDX_GUBUN', 'USERNO_total']]
    df_t_user_log_retention_rate = df_t_user_log_retention_rate_total.copy()
    del df_t_user_log_retention_rate_total

    if df_t_user_log_null_join_date.shape[0] != 0:
        if df_t_user_log_null_join_date["USERNO"].nunique() != 1 or df_t_user_log_null_join_date["USERNO"].unique()[0] != 1 or \
            df_t_user_log_null_join_date["USERKIND"].nunique() != 1 or df_t_user_log_null_join_date["USERKIND"].unique()[0] != "구분값 없음":
            raise ValueError("Need to check df_t_user_log_null_join_date..")
        f_nm = "{}df_t_user_log_null_join_date_{}.xlsx".format(path_export, date)
        print("Exporting {}".format(f_nm))
        df_t_user_log_null_join_date.to_excel(f_nm, index=False)
        del f_nm, df_t_user_log_null_join_date
    else:
        del df_t_user_log_null_join_date

    f_nm = "{}df_retention_rate_{}.pickle".format(path_export, date)
    print("Exporting {}".format(f_nm))
    with gzip.open(f_nm, "wb") as f:
        pickle.dump(df_t_user_log_retention_rate, f)
        del f, f_nm
    del df_t_user_log_retention_rate

    f_nm = "{}df_active_user_{}.pickle".format(path_export, date)
    print("Exporting {}".format(f_nm))
    with gzip.open(f_nm, "wb") as f:
        pickle.dump(df_t_user_log_summary, f)
        del f, f_nm
    del df_t_user_log_summary

    f_nm = "{}df_weekday_engagement_rate_{}.pickle".format(path_export, date)
    print("Exporting {}\n".format(f_nm))
    with gzip.open(f_nm, "wb") as f:
        pickle.dump(df_t_user_log_engagement_rate, f)
        del f, f_nm
    del df_t_user_log_engagement_rate

    del df_t_user_log


def conestore_total_engagement_rate(dbname=None):
    from glob import glob
    import gzip
    import pickle
    import pandas as pd
    from datetime import datetime
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory

    print("Reading and appending every df_t_user_log in ../{}/active_user/raw/ folder (for total engagement rate)..".format(dbname))
    df_t_user_log_total_au = pd.DataFrame()
    df_t_user_log = pd.DataFrame()
    for file in sorted(glob(path_import)):
        with gzip.open(file, "rb") as f:
            temp = pickle.load(f)
            temp.insert(0, "DIV", "전체")
            for val in temp["USERKIND"].unique():
                if val not in ["회원", "비회원"]:
                    raise ValueError("There are other values than 회원, 비회원 in {}".format(dbname))

            temp_total_au = temp.drop_duplicates(["DIV", "USERNO", "USERKIND", "YYYYMM"], keep="first")
            temp_total_au = temp_total_au[["DIV", "USERNO", "USERKIND", "YYYYMM"]]
            df_t_user_log_total_au = df_t_user_log_total_au.append(temp_total_au, sort=False, ignore_index=True)
            del temp_total_au

            temp.drop_duplicates(["DIV", "USERNO", "USERKIND", "REGDATE"], keep="first", inplace=True)
            temp = temp[["DIV", "YYYYMM", "REGDATE", "USERKIND", "USERNO"]]
            temp = temp.groupby(["DIV", "YYYYMM", "REGDATE", "USERKIND"])["USERNO"].nunique().reset_index().rename(
                columns={"USERNO": "weekdayActiveUser"})
            temp.insert(temp.columns.tolist().index("REGDATE")+1, "weekday",
                        temp["REGDATE"].apply(lambda x: datetime(year=int(x.split("-")[0]), month=int(x.split("-")[1]),
                                                                 day=int(x.split("-")[2])).strftime("%A")))
            temp["weekday"] = temp["weekday"].map(
                {"Monday": "월요일", "Tuesday": "화요일", "Wednesday": "수요일", "Thursday": "목요일", "Friday": "금요일",
                 "Saturday": "토요일", "Sunday": "일요일"})
            df_t_user_log = df_t_user_log.append(temp, ignore_index=True, sort=False)
            del f, temp
        del file

    df_t_user_log_total_au = df_t_user_log_total_au.groupby(["DIV", "YYYYMM", "USERKIND"])["USERNO"].nunique().reset_index().rename(
        columns={"USERNO": "TotalAU"})

    df_t_user_log = pd.merge(df_t_user_log, df_t_user_log_total_au, on=["DIV", "YYYYMM", "USERKIND"], how="outer")
    del df_t_user_log_total_au
    df_t_user_log["weekdayEngagementRate"] = df_t_user_log["weekdayActiveUser"] / df_t_user_log["TotalAU"]

    return df_t_user_log


def homelearnfriendsmall_total_engagement_rate(dbname=None):
    from glob import glob
    import gzip
    import pickle
    import pandas as pd
    from datetime import datetime
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory

    print("Reading and appending every df_t_user_log in ../active_user/raw/ folder (for total engagement rate)..")
    df_t_user_log = pd.DataFrame()
    for file in sorted(glob(path_import)):
        if "df_weekday_engagement" in file:
            print(file)
            with gzip.open(file, "rb") as f:
                temp = pickle.load(f)
                if "YYYYMM" not in temp.columns:
                    temp.insert(0, "YYYYMM", temp["REGDATE"].apply(lambda x: x[:-3]))
                temp.insert(0, "DIV", "전체")
                temp.rename(columns={"MAU": "TotalAU_total"}, inplace=True)
                df_t_user_log = df_t_user_log.append(temp, ignore_index=True, sort=False)
                del f, temp
        del file

    for col in df_t_user_log.columns:
        if df_t_user_log[df_t_user_log[col].isnull()].shape[0] != 0:
            if "ActiveUser" in col:
                df_t_user_log[col] = df_t_user_log[col].fillna(0).astype(int)
            if "EngagementRate" in col:
                df_t_user_log[col] = df_t_user_log[col].fillna(0)
        del col

    return df_t_user_log


def conestore_monthly_cart(table_name=None, date=None, dbname=None):
    from glob import glob
    import gzip
    import pickle
    import pandas as pd
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory
    # path_export = file_directory
    # path_t_user_log_raw = file_directory

    df_t_wish = recent_t_wish(dbname=dbname)

    df_t_cart_order = recent_t_cart_order(dbname=dbname)

    if df_t_cart_order["CARTIDX"].nunique() != df_t_cart_order.shape[0]:
        raise ValueError("CARTIDX in df_t_cart_order is not unique..")
    else:
        df_t_wish.drop(["SITEID", "IDX"], axis=1, inplace=True)

    print("Reading and appending df_t_cart for {}..".format(date))
    df_t_cart = pd.DataFrame()
    for file in sorted(glob(path_import)):
        if table_name in file and date in file and "df_t_cart_order" not in file:
            with gzip.open(file, "rb") as f:
                df_t_cart = df_t_cart.append(pickle.load(f), ignore_index=True, sort=False)
                del f
        del file
    df_t_cart.drop(["SITEID"], axis=1, inplace=True)
    df_t_cart.rename(columns={"IDX": "CARTIDX"}, inplace=True)
    df_t_cart["SESSID"] = df_t_cart["SESSID"].str.lower()
    df_t_cart = df_t_cart[~df_t_cart["SESSID"].str.contains("test|테스트")].reset_index(drop=True)

    print("Merging df_t_cart ({}) and df_t_cart_order..".format(date))
    df_t_cart = pd.merge(df_t_cart, df_t_cart_order, on=["CARTIDX"], how="left")
    del df_t_cart_order

    row = df_t_cart.shape[0]
    df_t_cart = pd.merge(df_t_cart, df_t_wish, on=["USERNO", "GOODSNO", "CATEIDX"], how="left")
    if row != df_t_cart.shape[0]:
        raise ValueError("Need to check df_t_wish data after merging with df_t_cart..")
    else:
        del df_t_wish

    df_t_user_log = pd.DataFrame()
    print("Reading ../active_user/raw/df_t_user_log_{}..".format(date))
    f_nm = "{}df_t_user_log_{}.pickle".format(path_t_user_log_raw, date)
    with gzip.open(f_nm, "rb") as f:
        df_t_user_log = df_t_user_log.append(pickle.load(f), sort=False, ignore_index=True)
        del f_nm, f
    df_t_user_log = df_t_user_log.groupby(["USERNO", "USERKIND"])["IDX"].count().reset_index().drop(["IDX"], axis=1)

    df_t_cart = pd.merge(df_t_cart, df_t_user_log, on=["USERNO"], how="left")

    ind_null_user_kind = df_t_cart[df_t_cart["USERKIND"].isnull()].index.tolist()
    if len(ind_null_user_kind) > 0:
        if df_t_cart.loc[ind_null_user_kind, "USERNO"].nunique() != 1 or df_t_cart.loc[ind_null_user_kind, "USERNO"].unique()[0] != 0:
            raise ValueError("0 is not the only USERNO with empty USERKIND..")
        else:
            df_t_cart.loc[ind_null_user_kind, "USERKIND"] = "0"
    else:
        del ind_null_user_kind

    if df_t_cart[df_t_cart["USERKIND"].isnull()].shape[0] != 0:
        raise ValueError("There are null values in USERKIND after merging df_cart with df_t_user_log..")
    else:
        del df_t_user_log

    f_nm = "{}df_t_cart_{}.pickle".format(path_export, date)
    print("Exporting {}\n".format(f_nm))
    with gzip.open(f_nm, "wb") as f:
        pickle.dump(df_t_cart, f)
        del f, f_nm
    del df_t_cart


def conestore_monthly_paid_user(table_name=None, date=None, dbname=None):
    from glob import glob
    import gzip
    import pickle
    import pandas as pd
    import numpy as np
    from datetime import datetime
    from functools import reduce
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory
    # path_export = file_directory

    df_register_date = conestore_total_t_user(dbname=dbname)

    df_t_order_goods = recent_t_order_goods(dbname=dbname)
    df_t_order_delivery = recent_t_order_delivery(dbname=dbname)

    df_t_order_claim = recent_t_order_claim(dbname=dbname)
    df_t_order_claim = df_t_order_claim[["ORDERIDX", "CLAIMTYPE", "USERNO", "USERID", "REASON", "REASONDETAIL", "REFUNDSTATE", "REGDATE"]].rename(columns={"ORDERIDX": "IDX", "REGDATE": "CANCELDATE"})
    df_t_order_claim["CLAIMTYPE"] = df_t_order_claim["CLAIMTYPE"].map({"100": "취소 (클레임)", "200": "교환 (클레임)", "300": "반품 (클레임)"})
    if df_t_order_claim[df_t_order_claim["CLAIMTYPE"].isnull()].shape[0] != 0:
        raise ValueError("취소, 교환, 반품 are not the only values in CLAIMTYPE columns in df_t_order_claim..")

    df_region = korea_region()

    df_sido_map = {}
    for col in ["SIDO_1", "SIDO_2"]:
        temp = df_region[["SIDO", col, "SIGUNGU"]].copy()
        temp = temp.groupby(["SIDO", col])["SIGUNGU"].count().reset_index().drop(["SIGUNGU"], axis=1)
        df_sido_map.update(dict(zip(temp[col], temp["SIDO"])))
        del temp, col
    temp = df_region["SIDO"].unique().tolist()
    df_sido_map.update(dict(zip(temp, temp)))
    del temp

    df_not = df_region[["SIDO", "SIGUNGU"]].copy()
    df_one = df_region[["SIDO_1", "SIGUNGU"]].copy().rename(columns={"SIDO_1": "SIDO"})
    df_two = df_region[["SIDO_2", "SIGUNGU"]].copy().rename(columns={"SIDO_2": "SIDO"}).dropna()
    df_region_concat = pd.concat([df_not, df_one, df_two], axis=0, ignore_index=True)
    df_region_concat["SIDOSIGUNGU"] = df_region_concat["SIDO"] + df_region_concat["SIGUNGU"]
    del df_not, df_one, df_two

    df_t_order_info = recent_t_order_info(dbname=dbname)
    for col in df_t_order_info.columns:
        if df_t_order_info[col].nunique() == 1 and df_t_order_info[col].unique()[0] == "":
            df_t_order_info.drop(col, axis=1, inplace=True)
        del col
    df_t_order_info = df_t_order_info[["ORDERIDX", "ORDADDR", "DEMAND"]].rename(
        columns={"ORDERIDX": "IDX"})
    df_t_order_info["ORDADDR_NOSPACE"] = df_t_order_info["ORDADDR"].str.replace(" ", "")
    df_t_order_info["ORDADDR_EXTRACT"] = df_t_order_info["ORDADDR_NOSPACE"].str.extract(
        "({})".format("|".join(df_region_concat["SIDOSIGUNGU"].unique().tolist())))

    df_t_order_info_isnull = df_t_order_info[(df_t_order_info["ORDADDR_EXTRACT"].isnull()) & (df_t_order_info["ORDADDR_NOSPACE"] != "")].copy()
    df_t_order_info_isnull["SIDO"] = np.nan
    df_t_order_info_isnull["SIGUNGU"] = np.nan

    df_t_order_info_isnull["SIDO"] = df_t_order_info_isnull["ORDADDR_NOSPACE"].str.extract("({})".format("|".join(df_region["SIDO"].unique().tolist())))
    df_t_order_info_isnull = df_t_order_info_isnull[df_t_order_info_isnull["SIDO"].notnull()].copy()

    for val in df_t_order_info_isnull["SIDO"].unique():
        temp = df_t_order_info_isnull[df_t_order_info_isnull["SIDO"] == val].copy()
        temp_region = df_region_concat[df_region_concat["SIDO"] == val]["SIGUNGU"].unique().tolist()
        temp["SIGUNGU"] = temp["ORDADDR_NOSPACE"].str.extract("({})".format("|".join(temp_region)))
        lst_ind = temp.index.tolist()
        df_t_order_info_isnull.loc[lst_ind, "SIGUNGU"] = temp.loc[lst_ind, "SIGUNGU"]
        del val, temp, temp_region, lst_ind
    lst_ind = df_t_order_info_isnull[(df_t_order_info_isnull["SIDO"] == "세종특별자치시") | (df_t_order_info_isnull["SIDO"] == "세종시") | (df_t_order_info_isnull["SIDO"] == "세종")].index.tolist()
    df_t_order_info_isnull.loc[lst_ind, "SIGUNGU"] = np.nan
    del lst_ind
    df_t_order_info_isnull["SIGUNGU"].fillna("", inplace=True)
    df_t_order_info_isnull["ORDADDR_EXTRACT"] = df_t_order_info_isnull["SIDO"] + df_t_order_info_isnull["SIGUNGU"]

    df_t_order_info.loc[df_t_order_info_isnull.index.tolist(), "ORDADDR_EXTRACT"] = df_t_order_info_isnull.loc[df_t_order_info_isnull.index.tolist(), "ORDADDR_EXTRACT"]
    del df_t_order_info_isnull
    df_t_order_info.rename(columns={"ORDADDR_EXTRACT": "SIDOSIGUNGU"}, inplace=True)

    df_t_order_info = pd.merge(df_t_order_info, df_region_concat, on="SIDOSIGUNGU", how="left")
    del df_region_concat

    lst_ind = df_t_order_info[(df_t_order_info["SIDOSIGUNGU"].notnull()) & (df_t_order_info["SIDO"].isnull())].index.tolist()
    df_t_order_info.loc[lst_ind, "SIDO"] = df_t_order_info.loc[lst_ind, "SIDOSIGUNGU"]
    df_t_order_info.drop(["SIDOSIGUNGU", "ORDADDR_NOSPACE"], axis=1, inplace=True)
    df_t_order_info["SIDO"] = df_t_order_info["SIDO"].map(df_sido_map)
    del df_region, df_sido_map

    print("Reading and appending df_t_order for {}..".format(date))
    df_t_order = pd.DataFrame()
    for file in sorted(glob(path_import)):
        if table_name in file and date in file and "df_t_order_goods" not in file and "df_t_order_goods_delivery_req" not in file and "t_order_delivery" not in file and "t_order_claim" not in file and "df_t_order_info" not in file and "t_cart_order" not in file and "t_order_pay" not in file and "t_order_option" not in file and "t_order_goods_option_text" not in file:
            with gzip.open(file, "rb") as f:
                df_t_order = df_t_order.append(pickle.load(f), ignore_index=True, sort=False)
                del f
        del file

    df_t_order = df_t_order[['IDX', 'FLATFORM', 'ORDNO', 'SESSID', 'USERNO', 'USERID', 'MEMBERLEVELNAME', 'SETTLEPRICE',
                             'ISFINISH', 'ISCONFIRM', 'CONFIRMDATE', "ISDELIVERYFINISH", "DELIVERYFINISHDATE", "ISCANCEL", 'CANCELDATE',
                             'REGDATE', 'TOTALORDERPRICE']]
    df_t_order["SESSID"] = df_t_order["SESSID"].str.strip().str.lower()
    df_t_order["USERID"] = df_t_order["USERID"].str.strip().str.lower()
    df_t_order = df_t_order[~df_t_order["SESSID"].str.contains("테스트|test")].reset_index(drop=True)
    df_t_order = df_t_order[~df_t_order["USERID"].str.contains("테스트|test")].reset_index(drop=True)

    unique_sessid = df_t_order["SESSID"].nunique()
    unique_userno = df_t_order["USERNO"].nunique()
    unique_userid = df_t_order["USERID"].nunique()

    if unique_sessid != unique_userno:
        raise ValueError("unique_sessid ({}) != unique_userno ({})".format(unique_sessid, unique_userno))
    if unique_sessid != unique_userid:
        raise ValueError("unique_sessid ({}) != unique_userid ({})".format(unique_sessid, unique_userid))
    if unique_userno != unique_userid:
        raise ValueError("unique_userno ({}) != unique_userid ({})".format(unique_userno, unique_userid))
    del unique_sessid, unique_userno, unique_userid

    df_t_order["regdate"] = df_t_order["REGDATE"].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
    df_t_order["REGDATE"] = df_t_order["REGDATE"].apply(lambda x: x.strftime("%Y-%m-%d"))
    df_t_order["weekNumber"] = df_t_order["REGDATE"].apply(
        lambda x: datetime(int(x.split("-")[0]), int(x.split("-")[1]), int(x.split("-")[2])).strftime("%V"))
    df_t_order["YYYYMM"] = df_t_order["REGDATE"].apply(lambda x: x[:-3])

    rows = df_t_order.shape[0]
    df_t_order = pd.merge(df_t_order, df_t_order_info, on=["IDX"], how="left")
    if rows != df_t_order.shape[0]:
        raise ValueError("IDX in df_t_order_info is no unique..")
    else:
        del df_t_order_info, rows
    df_t_order["DIV_ADDR"] = np.nan
    df_t_order.loc[df_t_order["SIDO"].notnull().tolist(), "DIV_ADDR"] = "확인된 주소"
    df_t_order.loc[df_t_order["SIDO"].isnull().tolist(), "DIV_ADDR"] = "미확인된 주소"

    rows = df_t_order.shape[0]
    df_t_order = pd.merge(df_t_order, df_t_order_claim, on=["IDX", "USERNO", "USERID", "CANCELDATE"], how="left")
    if df_t_order.shape[0] != rows:
        raise ValueError("Rows do not match after merging df_t_order with df_t_order_claim..\nbefore merging: {}\nafter merging: {}".format(rows, df_t_order.shape[0]))
    else:
        del df_t_order_claim, rows

    lst_claim_col = ["CLAIMTYPE", "REASON", "REASONDETAIL", "REFUNDSTATE"]
    for col in lst_claim_col:
        df_t_order[col].fillna("NULL", inplace=True)
        del col
    for col in ["SIDO", "SIGUNGU"]:
        df_t_order[col].fillna("확인 불가", inplace=True)
        del col

    row = df_t_order.shape[0]
    df_t_order = pd.merge(df_t_order, df_register_date, on=["USERNO", "USERID"], how="left")
    if row != df_t_order.shape[0]:
        raise ValueError("USERID do not have unique JOINDATE..")
    else:
        del row, df_register_date
    df_t_order["JOINDATE"] = df_t_order["JOINDATE"].astype(str).apply(lambda x: x.split(" ")[0])
    df_t_order["JOINDATE"].fillna("JOINDATE UNAVAILABLE", inplace=True)

    df_t_order["CONFIRMSTATUS"] = df_t_order["ISFINISH"] + df_t_order["ISCONFIRM"]

    df_t_order["CONFIRMSTATUS"] = df_t_order["CONFIRMSTATUS"].map({"TT": "구매 완료"})  # 구매 == 주문 및 입금 완료
    if df_t_order[df_t_order["CONFIRMSTATUS"].isnull()].shape[0] != 0:
        raise ValueError("There are other values than '주문 및 입금 완료' in [ISFINISH] and [ISCONFIRM]")

    df_t_order["CANCELSTATUS"] = df_t_order["ISDELIVERYFINISH"] + df_t_order["ISCANCEL"]
    df_t_order["CANCELSTATUS"] = df_t_order["CANCELSTATUS"].map({"TT": "배송 후 주문 취소",
                                                                 "TF": "구매 확정",
                                                                 "FT": "배송 전 주문 취소",
                                                                 "FF": "배송 중"})
    if df_t_order[df_t_order["CANCELSTATUS"].isnull()].shape[0] != 0:
        raise ValueError("There are null values in CANCELSTATUS column..")

    df_t_order_region = df_t_order.groupby(["REGDATE", "USERKIND", "DIV_ADDR", "SIDO", "SIGUNGU", "CLAIMTYPE", "REASON",
                                            "REASONDETAIL", "CONFIRMSTATUS", "CANCELSTATUS"])["ORDNO"].count().reset_index().rename(
        columns={"ORDNO": "COUNT"})
    if df_t_order_region["COUNT"].sum() != df_t_order.shape[0]:
        raise ValueError("Need to check data for ORDNO count..")

    print("Creating 결제후 취소 건수 dataframe..")
    lst_filter = ["구매 완료"]
    df_t_order_cancellation = df_t_order[df_t_order["CONFIRMSTATUS"].str.contains("|".join(lst_filter))].reset_index(drop=True)
    df_t_order_cancellation["payment"] = 1
    df_t_order_cancellation["cancelPayment"] = np.nan
    df_t_order_cancellation.loc[df_t_order_cancellation[df_t_order_cancellation["ISCANCEL"] == "T"].index, "cancelPayment"] = 1
    df_t_order_cancellation["cancelPayment"] = df_t_order_cancellation["cancelPayment"].fillna(0).astype(int)

    df_t_order_cancellation_daily = df_t_order_cancellation.groupby(["REGDATE", "USERKIND"]).agg({"payment": "sum", "cancelPayment": "sum"}).reset_index().rename(columns={"REGDATE": "Date"})
    df_t_order_cancellation_daily.insert(0, "div", "일별")
    df_t_order_cancellation_monthly = df_t_order_cancellation.groupby(["YYYYMM", "USERKIND"]).agg({"payment": "sum", "cancelPayment": "sum"}).reset_index().rename(columns={"YYYYMM": "Date"})
    df_t_order_cancellation_monthly.insert(0, "div", "월별")

    df_t_order_cancellation_g = pd.concat([df_t_order_cancellation_daily, df_t_order_cancellation_monthly], axis=0, ignore_index=True)
    del df_t_order_cancellation_daily, df_t_order_cancellation_monthly
    df_t_order_cancellation_g["cancellationRatio"] = df_t_order_cancellation_g["cancelPayment"] / df_t_order_cancellation_g["payment"]

    print("Creating 결제후 취소 건수 리스트 dataframe..")
    df_t_order_cancellation_list = df_t_order[df_t_order["CONFIRMSTATUS"].str.contains("|".join(lst_filter))].reset_index(drop=True)
    df_t_order_cancellation_list["payment"] = 1
    df_t_order_cancellation_list["cancelPayment"] = np.nan
    df_t_order_cancellation_list.loc[df_t_order_cancellation_list[df_t_order_cancellation_list["ISCANCEL"] == "T"].index, "cancelPayment"] = 1
    df_t_order_cancellation_list["cancelPayment"] = df_t_order_cancellation_list["cancelPayment"].fillna(0).astype(int)

    df_t_order_cancellation_list_daily = df_t_order_cancellation_list.groupby(["REGDATE", "USERKIND", "CONFIRMSTATUS", "CANCELSTATUS"]).agg(
        {"payment": "sum", "cancelPayment": "sum"}).reset_index().rename(
        columns={"REGDATE": "Date", "payment": "paymentCount", "cancelPayment": "cancelPaymentCount"})
    df_t_order_cancellation_list_daily.insert(0, "div", "일별")
    df_t_order_cancellation_list_monthly = df_t_order_cancellation_list.groupby(["YYYYMM", "USERKIND", "CONFIRMSTATUS", "CANCELSTATUS"]).agg(
        {"payment": "sum", "cancelPayment": "sum"}).reset_index().rename(
        columns={"YYYYMM": "Date", "payment": "paymentCount", "cancelPayment": "cancelPaymentCount"})
    df_t_order_cancellation_list_monthly.insert(0, "div", "월별")

    df_t_order_cancellation_list_g = pd.concat([df_t_order_cancellation_list_daily, df_t_order_cancellation_list_monthly], axis=0, ignore_index=True)
    del df_t_order_cancellation_list, df_t_order_cancellation_list_daily, df_t_order_cancellation_list_monthly

    for col in ["payment", "cancelPayment"]:
        if df_t_order_cancellation_list_g["{}Count".format(col)].sum() != df_t_order_cancellation_g[col].sum():
            raise ValueError("{} does not match for df_t_order_cancellation_g[{}].sum() and df_t_order_cancellation_list_g[{}].sum()".format(
                col, col, col))
        del col

    print("Creating total sequence..")
    df_t_order_cancellation_order = df_t_order_cancellation[["SIDO", "SIGUNGU", "USERNO", "ORDNO", "CANCELSTATUS", "regdate", "CLAIMTYPE", "REASON", "REASONDETAIL"]].copy().rename(columns={"regdate": "DATE"})
    df_t_order_cancellation_confirm = df_t_order_cancellation[["SIDO", "SIGUNGU", "USERNO", "ORDNO", "CANCELSTATUS", "CONFIRMDATE", "CLAIMTYPE", "REASON", "REASONDETAIL"]].copy().rename(columns={"CONFIRMDATE": "DATE"})
    df_t_order_cancellation_delivery = df_t_order_cancellation[["SIDO", "SIGUNGU", "USERNO", "ORDNO", "CANCELSTATUS", "DELIVERYFINISHDATE", "CLAIMTYPE", "REASON", "REASONDETAIL"]].copy().rename(columns={"DELIVERYFINISHDATE": "DATE"})
    df_t_order_cancellation_cancel = df_t_order_cancellation[["SIDO", "SIGUNGU", "USERNO", "ORDNO", "CANCELSTATUS", "CANCELDATE", "CLAIMTYPE", "REASON", "REASONDETAIL"]].copy().rename(columns={"CANCELDATE": "DATE"})

    df_t_order_cancellation_order["DATE"] = pd.to_datetime(df_t_order_cancellation_order["DATE"])
    df_t_order_cancellation_confirm["DATE"] = pd.to_datetime(df_t_order_cancellation_confirm["DATE"])
    df_t_order_cancellation_delivery["DATE"] = pd.to_datetime(df_t_order_cancellation_delivery["DATE"])
    df_t_order_cancellation_cancel["DATE"] = pd.to_datetime(df_t_order_cancellation_cancel["DATE"])

    df_t_order_cancellation_order = df_t_order_cancellation_order[df_t_order_cancellation_order["DATE"].notnull()].copy()
    df_t_order_cancellation_confirm = df_t_order_cancellation_confirm[df_t_order_cancellation_confirm["DATE"].notnull()].copy()
    df_t_order_cancellation_delivery = df_t_order_cancellation_delivery[df_t_order_cancellation_delivery["DATE"].notnull()].copy()
    df_t_order_cancellation_cancel = df_t_order_cancellation_cancel[df_t_order_cancellation_cancel["DATE"].notnull()].copy()

    df_t_order_cancellation_order["STATUS"] = "1.주문일자"
    df_t_order_cancellation_confirm["STATUS"] = "2.입금확인일자"
    df_t_order_cancellation_delivery["STATUS"] = "3.배송완료일자"
    df_t_order_cancellation_cancel["STATUS"] = "4.주문취소일자"

    df_t_order_cancellation_seq = pd.concat([df_t_order_cancellation_order, df_t_order_cancellation_confirm, df_t_order_cancellation_delivery, df_t_order_cancellation_cancel],
                                            axis=0).sort_values(by=["DATE", "STATUS"], ascending=["True", "True"], ignore_index=True)
    del df_t_order_cancellation, df_t_order_cancellation_order, df_t_order_cancellation_confirm, df_t_order_cancellation_delivery, df_t_order_cancellation_cancel

    df_t_order = df_t_order[(df_t_order["CONFIRMSTATUS"] == "구매 완료") & (df_t_order["CANCELSTATUS"] == "구매 확정")].reset_index(drop=True)

    for col in ["CONFIRMSTATUS", "CANCELSTATUS"]:
        v = ""
        if col == "CONFIRMSTATUS":
            v += "구매 완료"
        elif col == "CANCELSTATUS":
            v += "구매 확정"
        else:
            raise ValueError("No value defined..")
        if df_t_order[col].nunique() != 1 or df_t_order[col].unique().tolist()[0] != v:
            raise ValueError("Need to check data for {} column since ISFINISH==T and ISCONFIRM==T and ISDELIVERYFINISH==T and ISCANCEL==F..".format(col))
        del col, v

    print("Creating 매출 dataframe..")
    df_t_order_price_daily = df_t_order.groupby(["REGDATE", "USERKIND"])["TOTALORDERPRICE"].sum().reset_index(name="totalOrderPrice").rename(columns={"REGDATE": "Date"})
    df_t_order_price_daily.insert(0, "div", "일별")
    df_t_order_price_monthly = df_t_order.groupby(["YYYYMM", "USERKIND"])["TOTALORDERPRICE"].sum().reset_index(name="totalOrderPrice").rename(columns={"YYYYMM": "Date"})
    df_t_order_price_monthly.insert(0, "div", "월별")

    df_t_order_price_g = pd.concat([df_t_order_price_daily, df_t_order_price_monthly],
                                   axis=0, ignore_index=True)
    del df_t_order_price_daily, df_t_order_price_monthly

    df_t_order_payment_confirm = df_t_order.copy()
    df_t_order_payment_confirm["paymentConfirm"] = 1

    df_t_order_payment_confirm_daily = df_t_order_payment_confirm.groupby(["REGDATE", "USERKIND"]).agg({"paymentConfirm": "sum"}).reset_index().rename(columns={"REGDATE": "Date"})
    df_t_order_payment_confirm_daily.insert(0, "div", "일별")
    df_t_order_payment_confirm_monthly = df_t_order_payment_confirm.groupby(["YYYYMM", "USERKIND"]).agg({"paymentConfirm": "sum"}).reset_index().rename(columns={"YYYYMM": "Date"})
    df_t_order_payment_confirm_monthly.insert(0, "div", "월별")

    df_t_order_payment_confirm_g = pd.concat([df_t_order_payment_confirm_daily, df_t_order_payment_confirm_monthly], axis=0, ignore_index=True)
    del df_t_order_payment_confirm, df_t_order_payment_confirm_daily, df_t_order_payment_confirm_monthly

    print("Creating 구매자수 dataframe..")
    df_t_order_paid_user_daily = df_t_order.groupby(["REGDATE", "USERKIND"])["USERNO"].nunique().reset_index(name="paidUser").rename(columns={"REGDATE": "Date"})
    df_t_order_paid_user_daily.insert(0, "div", "일별")
    df_t_order_paid_user_monthly = df_t_order.groupby(["YYYYMM", "USERKIND"])["USERNO"].nunique().reset_index(name="paidUser").rename(columns={"YYYYMM": "Date"})
    df_t_order_paid_user_monthly.insert(0, "div", "월별")

    df_t_order_paid_user_g = pd.concat([df_t_order_paid_user_daily, df_t_order_paid_user_monthly],
                                       axis=0, ignore_index=True)
    del df_t_order_paid_user_daily, df_t_order_paid_user_monthly

    print("Creating 재구매율 dataframe..")
    df_t_order_repurchase_daily = df_t_order.groupby(["USERNO", "USERKIND"])["REGDATE"].apply(set).reset_index().rename(columns={"REGDATE": "REGDATES"})
    df_t_order_repurchase_daily["REGDATES"] = df_t_order_repurchase_daily["REGDATES"].apply(lambda x: sorted(list(x)))
    df_t_order_repurchase_daily["purchaseDays"] = df_t_order_repurchase_daily["REGDATES"].apply(lambda x: len(x))
    df_t_order_repurchase_daily["repurchase"] = np.nan
    df_t_order_repurchase_daily.loc[df_t_order_repurchase_daily[df_t_order_repurchase_daily["purchaseDays"] == 1].index, "repurchase"] = "N"
    df_t_order_repurchase_daily.loc[df_t_order_repurchase_daily[df_t_order_repurchase_daily["purchaseDays"] > 1].index, "repurchase"] = "Y"
    df_t_order_repurchase_daily["firstREGDATE"] = df_t_order_repurchase_daily["REGDATES"].apply(lambda x: x[0])
    df_t_order_repurchase_daily = df_t_order_repurchase_daily.groupby(["firstREGDATE", "USERKIND", "repurchase"])[
        "USERNO"].nunique().reset_index(name="numberOfUsers").rename(columns={"repurchase": "재구매 여부", "firstREGDATE": "Date"})
    df_t_order_repurchase_daily.insert(0, "div", "일별")

    df_t_order_repurchase_daily_repurchase_y = df_t_order_repurchase_daily[df_t_order_repurchase_daily["재구매 여부"] == "Y"].copy().reset_index(drop=True).rename(
        columns={"numberOfUsers": "numberOfUsers_repurchaseY"}).drop(["재구매 여부"], axis=1)
    df_t_order_repurchase_daily_repurchase_n = df_t_order_repurchase_daily[df_t_order_repurchase_daily["재구매 여부"] == "N"].copy().reset_index(drop=True).rename(
        columns={"numberOfUsers": "numberOfUsers_repurchaseN"}).drop(["재구매 여부"], axis=1)
    del df_t_order_repurchase_daily
    df_t_order_repurchase_daily = pd.merge(df_t_order_repurchase_daily_repurchase_y, df_t_order_repurchase_daily_repurchase_n, on=["div", "Date", "USERKIND"], how="outer")
    del df_t_order_repurchase_daily_repurchase_y, df_t_order_repurchase_daily_repurchase_n

    df_t_order_repurchase_daily.sort_values(["div", "Date"], ignore_index=True, inplace=True)
    df_t_order_repurchase_daily["numberOfUsers_repurchaseY"].fillna(0, inplace=True)
    df_t_order_repurchase_daily["numberOfUsers_repurchaseN"].fillna(0, inplace=True)
    df_t_order_repurchase_daily[["numberOfUsers_repurchaseY", "numberOfUsers_repurchaseN"]] = df_t_order_repurchase_daily[["numberOfUsers_repurchaseY", "numberOfUsers_repurchaseN"]].astype(int)
    df_t_order_repurchase_daily.rename(columns={"numberOfUsers_repurchaseY": "재구매_Y", "numberOfUsers_repurchaseN": "재구매_N"}, inplace=True)
    df_t_order_repurchase_daily["재구매_총"] = df_t_order_repurchase_daily["재구매_Y"] + df_t_order_repurchase_daily["재구매_N"]
    df_t_order_repurchase_daily["재구매율"] = df_t_order_repurchase_daily["재구매_Y"] / df_t_order_repurchase_daily["재구매_총"]

    print("Creating 첫 구매 후 두 번째 구매까지 걸리는 시간 dataframe..")
    df_t_order_repurchase_first_to_second_purchase_daily = df_t_order.groupby(["SESSID", "USERNO", "USERID", "USERKIND"])["regdate"].apply(set).reset_index().rename(columns={"regdate": "regdates"})
    df_t_order_repurchase_first_to_second_purchase_daily["regdates"] = df_t_order_repurchase_first_to_second_purchase_daily["regdates"].apply(lambda x: sorted(list(x)))
    df_t_order_repurchase_first_to_second_purchase_daily["regdatesLst"] = df_t_order_repurchase_first_to_second_purchase_daily["regdates"].apply(lambda x: [dt.split(" ")[0] for dt in x])
    df_t_order_repurchase_first_to_second_purchase_daily["regdatesLstUnique"] = df_t_order_repurchase_first_to_second_purchase_daily["regdatesLst"].apply(set)
    df_t_order_repurchase_first_to_second_purchase_daily["regdatesLstUnique"] = df_t_order_repurchase_first_to_second_purchase_daily["regdatesLstUnique"].apply(lambda x: sorted(list(x)))
    df_t_order_repurchase_first_to_second_purchase_daily["purchaseDays"] = df_t_order_repurchase_first_to_second_purchase_daily["regdatesLstUnique"].apply(len)

    purchase_date_greater_than_one = df_t_order_repurchase_first_to_second_purchase_daily[df_t_order_repurchase_first_to_second_purchase_daily["purchaseDays"] > 1].reset_index(drop=True)

    purchase_date_greater_than_one["firstRegdate"] = purchase_date_greater_than_one["regdatesLstUnique"].apply(lambda x: x[0])
    purchase_date_greater_than_one["secondRegdate"] = purchase_date_greater_than_one["regdatesLstUnique"].apply(lambda x: x[1])

    purchase_date_greater_than_one["firstRegdatesTime"] = np.nan
    purchase_date_greater_than_one["secondRegdatesTime"] = np.nan
    for ind in purchase_date_greater_than_one.index:
        first_regdate_index = purchase_date_greater_than_one.loc[ind, "firstRegdate"]
        second_regdate_index = purchase_date_greater_than_one.loc[ind, "secondRegdate"]

        first_regdate_index = purchase_date_greater_than_one.loc[ind, "regdatesLst"].index(first_regdate_index)
        second_regdate_index = purchase_date_greater_than_one.loc[ind, "regdatesLst"].index(second_regdate_index)

        purchase_date_greater_than_one.loc[ind, "firstRegdatesTime"] = purchase_date_greater_than_one.loc[ind, "regdates"][first_regdate_index]
        purchase_date_greater_than_one.loc[ind, "secondRegdatesTime"] = purchase_date_greater_than_one.loc[ind, "regdates"][second_regdate_index]
        del ind, first_regdate_index, second_regdate_index

    for col in ["firstRegdatesTime", "secondRegdatesTime"]:
        purchase_date_greater_than_one[col] = pd.to_datetime(purchase_date_greater_than_one[col])
        del col
    purchase_date_greater_than_one.drop(["regdates", "regdatesLst", "regdatesLstUnique", "purchaseDays", "firstRegdate", "secondRegdate"], axis=1, inplace=True)

    purchase_date_equal_to_one = df_t_order_repurchase_first_to_second_purchase_daily[df_t_order_repurchase_first_to_second_purchase_daily["purchaseDays"] == 1].reset_index(drop=True)
    purchase_date_equal_to_one["firstRegdatesTime"] = purchase_date_equal_to_one["regdates"].apply(lambda x: x[0])
    purchase_date_equal_to_one["secondRegdatesTime"] = np.nan
    purchase_date_equal_to_one.drop(["regdates", "regdatesLst", "regdatesLstUnique", "purchaseDays"], axis=1, inplace=True)

    if df_t_order_repurchase_first_to_second_purchase_daily.shape[0] != purchase_date_greater_than_one.shape[0] + purchase_date_equal_to_one.shape[0]:
        raise ValueError("1. Need to check data..")
    else:
        df_t_order_repurchase_first_to_second_purchase_daily = pd.concat([purchase_date_greater_than_one, purchase_date_equal_to_one], axis=0, ignore_index=True)
        del purchase_date_greater_than_one, purchase_date_equal_to_one

    print("Merging all the dataframes above..")
    df_t_order_g = reduce(lambda left, right: pd.merge(left, right, on=["div", "Date", "USERKIND"], how="outer"),
                        [df_t_order_cancellation_g, df_t_order_payment_confirm_g, df_t_order_price_g, df_t_order_paid_user_g, df_t_order_repurchase_daily])
    del df_t_order_cancellation_g, df_t_order_payment_confirm_g, df_t_order_price_g, df_t_order_paid_user_g, df_t_order_repurchase_daily

    df_t_order_g.insert(df_t_order_g.columns.tolist().index("Date") + 1, "weekday", np.nan)
    df_t_order_g_dau = df_t_order_g[df_t_order_g["div"] == "일별"].copy()
    df_t_order_g_dau["weekday"] = df_t_order_g_dau["Date"].apply(lambda x: datetime(year=int(x.split("-")[0]), month=int(x.split("-")[1]), day=int(x.split("-")[2])).strftime("%A"))
    df_t_order_g_mau = df_t_order_g[df_t_order_g["div"] == "월별"].copy()
    df_t_order_g = pd.concat([df_t_order_g_dau, df_t_order_g_mau], axis=0, ignore_index=True)
    del df_t_order_g_dau, df_t_order_g_mau
    df_t_order_g.insert(df_t_order_g.columns.tolist().index("weekday") + 1, "weekdayDiv", df_t_order_g["weekday"].copy())
    df_t_order_g["weekdayDiv"] = df_t_order_g["weekdayDiv"].map(
        {"Monday": "주중", "Tuesday": "주중", "Wednesday": "주중", "Thursday": "주중", "Friday": "주중",
         "Saturday": "주말", "Sunday": "주말"})
    df_t_order_g["weekday"] = df_t_order_g["weekday"].map(
        {"Monday": "월요일", "Tuesday": "화요일", "Wednesday": "수요일", "Thursday": "목요일", "Friday": "금요일",
         "Saturday": "토요일", "Sunday": "일요일"})

    print("Creating 회원가입일별 구매 확정수 dataframe..")
    df_t_order_by_join_date = df_t_order.copy()
    df_t_order_by_join_date.insert(df_t_order_by_join_date.columns.tolist().index("JOINDATE")+1, "DIV_JOINDATE", np.nan)
    ind_join_date = [x.strftime("%Y-%m-%d") for x in pd.date_range(start="2020-03-06", end=datetime.today().strftime("%Y-%m-%d"), freq="D")]
    ind_join_date = "|".join(ind_join_date)
    ind_join_date = df_t_order_by_join_date[df_t_order_by_join_date["JOINDATE"].str.contains("{}".format(ind_join_date))].index.tolist()
    df_t_order_by_join_date.loc[ind_join_date, "DIV_JOINDATE"] = "2020-03-06 이후"
    df_t_order_by_join_date["DIV_JOINDATE"].fillna("2020-03-05 이전", inplace=True)
    del ind_join_date

    df_t_order_by_join_date["JOINDATE"] = df_t_order_by_join_date["JOINDATE"].apply(lambda x: x[:-3])
    df_t_order_by_join_date = df_t_order_by_join_date.groupby(["YYYYMM", "USERKIND", "JOINDATE", "DIV_JOINDATE"]).agg({"TOTALORDERPRICE": "sum", "USERNO": pd.Series.nunique}).reset_index().rename(
        columns={"YYYYMM": "paymentConfirmDate", "USERNO": "UNIQUEUSERNO"})
    df_t_order_by_join_date["REVENUEPERUNIQUEUSERNO"] = df_t_order_by_join_date["TOTALORDERPRICE"] / df_t_order_by_join_date["UNIQUEUSERNO"]

    f_nm_order_seq = "{}df_order_sequence_{}.pickle".format(path_export, date)
    print("Exporting {}".format(f_nm_order_seq))
    with gzip.open(f_nm_order_seq, "wb") as f:
        pickle.dump(df_t_order_cancellation_seq, f)
        del f
    del df_t_order_cancellation_seq, f_nm_order_seq

    f_nm_cancellation_list = "{}df_cancellation_list_{}.pickle".format(path_export, date)
    print("Exporting {}".format(f_nm_cancellation_list))
    with gzip.open(f_nm_cancellation_list, "wb") as f:
        pickle.dump(df_t_order_cancellation_list_g, f)
        del f
    del df_t_order_cancellation_list_g, f_nm_cancellation_list

    f_nm_order_region = "{}df_order_region_{}.pickle".format(path_export, date)
    print("Exporting {}".format(f_nm_order_region))
    with gzip.open(f_nm_order_region, "wb") as f:
        pickle.dump(df_t_order_region, f)
        del f
    del df_t_order_region, f_nm_order_region

    f_nm_paid_user = "{}df_paid_user_{}.pickle".format(path_export, date)
    print("Exporting {}".format(f_nm_paid_user))
    with gzip.open(f_nm_paid_user, "wb") as f:
        pickle.dump(df_t_order_g, f)
        del f
    del df_t_order_g, f_nm_paid_user

    f_nm_first_to_second_purchase = "{}df_first_to_second_purchase_{}.pickle".format(path_export, date)
    print("Exporting {}".format(f_nm_first_to_second_purchase))
    with gzip.open(f_nm_first_to_second_purchase, "wb") as f:
        pickle.dump(df_t_order_repurchase_first_to_second_purchase_daily, f)
        del f
    del df_t_order_repurchase_first_to_second_purchase_daily, f_nm_first_to_second_purchase

    f_nm_by_join_date = "{}df_by_join_date_{}.pickle".format(path_export, date)
    print("Exporting {}\n".format(f_nm_by_join_date))
    with gzip.open(f_nm_by_join_date, "wb") as f:
        pickle.dump(df_t_order_by_join_date, f)
        del f
    del df_t_order_by_join_date, f_nm_by_join_date

    del df_t_order


def homelearnfriendsmall_monthly_paid_user_raw(table_name=None, date=None, dbname=None):
    from glob import glob
    import gzip
    import pickle
    import pandas as pd
    import numpy as np
    from datetime import datetime
    from functools import reduce
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory
    # path_export = file_directory

    df_recent_t_member_with_age = recent_t_member_with_age(date=date)
    df_register_date = homelearnfriendsmall_total_t_user(dbname=dbname)

    df_t_order_goods = recent_t_order_goods(dbname=dbname)
    df_t_order_goods = df_t_order_goods.drop(["IDX"], axis=1).rename(columns={"ORDERIDX": "IDX"})

    check_by_df_t_order_goods = df_t_order_goods.groupby(["IDX", "GOODSNAME"])["GOODSCODE"].count().reset_index().drop(["GOODSCODE"], axis=1)
    check_by_df_t_order_goods = check_by_df_t_order_goods.groupby("IDX")["GOODSNAME"].apply(list).reset_index()
    check_by_df_t_order_goods["count"] = check_by_df_t_order_goods["GOODSNAME"].apply(lambda x: len(x))
    check_by_df_t_order_goods["GOODSNAME"] = check_by_df_t_order_goods["GOODSNAME"].apply(lambda x: " 또는 ".join(x))

    df_t_order_delivery = recent_t_order_delivery(dbname=dbname)
    df_t_order_delivery.rename(columns={"IDX": "ORDERDELIVERYIDX"}, inplace=True)

    rows = df_t_order_goods.shape[0]
    df_t_order_goods = pd.merge(df_t_order_goods, df_t_order_delivery, on=["ORDERDELIVERYIDX", "DEALERNO", "GOODSNO"], how="left")
    if rows != df_t_order_goods.shape[0]:
        raise ValueError("Need to check df_t_order_goods after merging with df_t_order_delivery..")
    else:
        del rows, df_t_order_delivery

        df_t_order_goods_goodsname = df_t_order_goods.groupby(["IDX", "GOODSNAME"])["GOODSCODE"].count().reset_index().drop(["GOODSCODE"], axis=1)
        df_t_order_goods_goodsname = df_t_order_goods_goodsname.groupby("IDX")["GOODSNAME"].apply(list).reset_index()
        df_t_order_goods_goodsno = df_t_order_goods.groupby(["IDX", "GOODSNO"])["GOODSCODE"].count().reset_index().drop(["GOODSCODE"], axis=1)
        df_t_order_goods_goodsno = df_t_order_goods_goodsno.groupby("IDX")["GOODSNO"].apply(list).reset_index()

        df_t_order_goods = pd.merge(df_t_order_goods_goodsname, df_t_order_goods_goodsno, on=["IDX"], how="outer")
        del df_t_order_goods_goodsname, df_t_order_goods_goodsno
        df_t_order_goods["count_GOODSNAME"] = df_t_order_goods["GOODSNAME"].apply(lambda x: len(x))
        df_t_order_goods["GOODSNAME"] = df_t_order_goods["GOODSNAME"].apply(lambda x: " 또는 ".join(x))
        df_t_order_goods["count_GOODSNO"] = df_t_order_goods["GOODSNO"].apply(lambda x: len(x))
        df_t_order_goods["GOODSNO"] = df_t_order_goods["GOODSNO"].apply(lambda x: " 또는 ".join([str(i) for i in x]))
        if df_t_order_goods[df_t_order_goods["count_GOODSNAME"] != df_t_order_goods["count_GOODSNO"]].shape[0] != 0:
            raise ValueError("Need to check the length of lists for GOODSNAME and GOODSNO..")
        else:
            df_t_order_goods = df_t_order_goods.drop(["count_GOODSNO"], axis=1).rename(columns={"count_GOODSNAME": "count"})

        if df_t_order_goods[df_t_order_goods["GOODSNAME"].isnull()].shape[0] != 0:
            raise ValueError("Need to check unique values for df_t_order_goods and df_t_order_delivery..")
        else:
            df_t_order_goods.drop(["count"], axis=1, inplace=True)

    df_t_order_claim = recent_t_order_claim(dbname=dbname)
    df_t_order_claim = df_t_order_claim[["ORDERIDX", "CLAIMTYPE", "USERNO", "USERID", "REASON", "REASONDETAIL", "REFUNDSTATE", "REGDATE"]].rename(columns={"ORDERIDX": "IDX", "REGDATE": "CANCELDATE"})
    df_t_order_claim["CLAIMTYPE"] = df_t_order_claim["CLAIMTYPE"].map({"100": "취소 (클레임)", "200": "교환 (클레임)", "300": "반품 (클레임)"})
    if df_t_order_claim[df_t_order_claim["CLAIMTYPE"].isnull()].shape[0] != 0:
        raise ValueError("취소, 교환, 반품 are not the only values in CLAIMTYPE columns in df_t_order_claim..")

    df_region = korea_region()

    df_sido_map = {}
    for col in ["SIDO_1", "SIDO_2"]:
        temp = df_region[["SIDO", col, "SIGUNGU"]].copy()
        temp = temp.groupby(["SIDO", col])["SIGUNGU"].count().reset_index().drop(["SIGUNGU"], axis=1)
        df_sido_map.update(dict(zip(temp[col], temp["SIDO"])))
        del temp, col
    temp = df_region["SIDO"].unique().tolist()
    df_sido_map.update(dict(zip(temp, temp)))
    del temp

    df_not = df_region[["SIDO", "SIGUNGU"]].copy()
    df_one = df_region[["SIDO_1", "SIGUNGU"]].copy().rename(columns={"SIDO_1": "SIDO"})
    df_two = df_region[["SIDO_2", "SIGUNGU"]].copy().rename(columns={"SIDO_2": "SIDO"}).dropna()
    df_region_concat = pd.concat([df_not, df_one, df_two], axis=0, ignore_index=True)
    df_region_concat["SIDOSIGUNGU"] = df_region_concat["SIDO"] + df_region_concat["SIGUNGU"]
    del df_not, df_one, df_two

    df_t_order_info = recent_t_order_info(dbname=dbname)
    for col in df_t_order_info.columns:
        if df_t_order_info[col].nunique() == 1 and df_t_order_info[col].unique()[0] == "":
            df_t_order_info.drop(col, axis=1, inplace=True)
        del col
    df_t_order_info = df_t_order_info[["ORDERIDX", "ORDADDR", "DEMAND"]].rename(columns={"ORDERIDX": "IDX"})
    df_t_order_info["ORDADDR_NOSPACE"] = df_t_order_info["ORDADDR"].str.replace(" ", "")
    df_t_order_info["ORDADDR_EXTRACT"] = df_t_order_info["ORDADDR_NOSPACE"].str.extract(
        "({})".format("|".join(df_region_concat["SIDOSIGUNGU"].unique().tolist())))

    df_t_order_info_isnull = df_t_order_info[(df_t_order_info["ORDADDR_EXTRACT"].isnull()) & (df_t_order_info["ORDADDR_NOSPACE"] != "")].copy()
    df_t_order_info_isnull["SIDO"] = np.nan
    df_t_order_info_isnull["SIGUNGU"] = np.nan

    df_t_order_info_isnull["SIDO"] = df_t_order_info_isnull["ORDADDR_NOSPACE"].str.extract("({})".format("|".join(df_region["SIDO"].unique().tolist())))
    df_t_order_info_isnull = df_t_order_info_isnull[df_t_order_info_isnull["SIDO"].notnull()].copy()

    for val in df_t_order_info_isnull["SIDO"].unique():
        temp = df_t_order_info_isnull[df_t_order_info_isnull["SIDO"] == val].copy()
        temp_region = df_region_concat[df_region_concat["SIDO"] == val]["SIGUNGU"].unique().tolist()
        temp["SIGUNGU"] = temp["ORDADDR_NOSPACE"].str.extract("({})".format("|".join(temp_region)))
        lst_ind = temp.index.tolist()
        df_t_order_info_isnull.loc[lst_ind, "SIGUNGU"] = temp.loc[lst_ind, "SIGUNGU"]
        del val, temp, temp_region, lst_ind
    lst_ind = df_t_order_info_isnull[(df_t_order_info_isnull["SIDO"] == "세종특별자치시") | (df_t_order_info_isnull["SIDO"] == "세종시") | (df_t_order_info_isnull["SIDO"] == "세종")].index.tolist()
    df_t_order_info_isnull.loc[lst_ind, "SIGUNGU"] = np.nan
    del lst_ind
    df_t_order_info_isnull["SIGUNGU"].fillna("", inplace=True)
    df_t_order_info_isnull["ORDADDR_EXTRACT"] = df_t_order_info_isnull["SIDO"] + df_t_order_info_isnull["SIGUNGU"]

    df_t_order_info.loc[df_t_order_info_isnull.index.tolist(), "ORDADDR_EXTRACT"] = df_t_order_info_isnull.loc[df_t_order_info_isnull.index.tolist(), "ORDADDR_EXTRACT"]
    del df_t_order_info_isnull
    df_t_order_info.rename(columns={"ORDADDR_EXTRACT": "SIDOSIGUNGU"}, inplace=True)

    df_t_order_info = pd.merge(df_t_order_info, df_region_concat, on="SIDOSIGUNGU", how="left")
    del df_region_concat

    lst_ind = df_t_order_info[(df_t_order_info["SIDOSIGUNGU"].notnull()) & (df_t_order_info["SIDO"].isnull())].index.tolist()
    df_t_order_info.loc[lst_ind, "SIDO"] = df_t_order_info.loc[lst_ind, "SIDOSIGUNGU"]
    df_t_order_info.drop(["SIDOSIGUNGU", "ORDADDR_NOSPACE"], axis=1, inplace=True)
    df_t_order_info["SIDO"] = df_t_order_info["SIDO"].map(df_sido_map)
    del df_region, df_sido_map

    print("Reading and appending df_t_order for {}..".format(date))
    df_t_order = pd.DataFrame()
    for file in sorted(glob(path_import)):
        if table_name in file and date in file and "df_t_order_goods" not in file and "df_t_order_goods_delivery_req" not in file and "t_order_delivery" not in file and "t_order_claim" not in file and "df_t_order_info" not in file and "t_cart_order" not in file and "t_order_pay" not in file and "t_order_option" not in file and "t_order_goods_option_text" not in file:
            with gzip.open(file, "rb") as f:
                df_t_order = df_t_order.append(pickle.load(f), ignore_index=True, sort=False)
                del f
        del file

    check_by_df_t_order_goods = pd.merge(df_t_order, check_by_df_t_order_goods, on=["IDX"], how="left")

    rows = df_t_order.shape[0]
    df_t_order = pd.merge(df_t_order, df_t_order_goods, on=["IDX"], how="left")
    if rows != df_t_order.shape[0]:
        raise ValueError("Need to check df_t_order_goods (merging df_t_order_goods and df_t_order_delivery)..")
    else:
        del rows, df_t_order_goods
    if df_t_order[df_t_order["GOODSNAME"].isnull()].shape[0] != 0:
        raise ValueError("Need to check null GOODSNAME in df_t_order column..")

    check_by_df_t_order_goods_delivery = df_t_order["GOODSNAME"].tolist()
    check_by_df_t_order_goods = check_by_df_t_order_goods["GOODSNAME"].tolist()
    check_map = dict(zip(check_by_df_t_order_goods_delivery, check_by_df_t_order_goods))
    del check_by_df_t_order_goods_delivery, check_by_df_t_order_goods

    for k in check_map.keys():
        if k != check_map[k]:
            raise ValueError("GOODSNAME DO NOT MATCH..")
        del k
    del check_map

    df_t_order.drop(["SITEID"], axis=1, inplace=True)
    lst_ind_nomember = df_t_order[df_t_order["USERNO"] == 0].index.tolist()
    if df_t_order.loc[lst_ind_nomember, "USERID"].nunique(dropna=False) != 1 or df_t_order.loc[lst_ind_nomember, "USERID"].unique()[0] != "guest":
        raise ValueError("Check USERID for USERNO == 0..")
    if df_t_order.loc[lst_ind_nomember, "MEMBERLEVELIDX"].nunique(dropna=False) != 1 or df_t_order.loc[lst_ind_nomember, "MEMBERLEVELIDX"].unique()[0] != 0:
        raise ValueError("Check MEMBERLEVEIDX for USERNO == 0..")
    df_t_order.loc[lst_ind_nomember, "MEMBERLEVELNAME"] = "비회원"
    if df_t_order[df_t_order["MEMBERLEVELNAME"].isnull()].shape[0] != 0:
        raise ValueError("Need to check MEMBERLEVELNAME after filling 비회원 where USERNO == 0..")
    del lst_ind_nomember

    col_check = [x for x in df_recent_t_member_with_age.columns.tolist() if "USERNO" not in x]
    for col in col_check:
        df_recent_t_member_with_age.rename(columns={col: "{}_T_MEMBER".format(col)}, inplace=True)
        del col

    rows = df_t_order.shape[0]
    df_t_order = pd.merge(df_t_order, df_recent_t_member_with_age, on=["USERNO"], how="left")
    if rows != df_t_order.shape[0]:
        raise ValueError("Need to check df_recent_t_member_with_age since the USERNO is not unique..")
    else:
        del rows, df_recent_t_member_with_age
    # 테이블명세서-홈런프렌즈쇼핑몰 > FLATFORM column 100: PC,  200: 모바일.. conestore 에서 400 은 무엇인가?
    df_t_order["FLATFORM"] = df_t_order["FLATFORM"].map({"100": "PC", "200": "MO"})
    if df_t_order[df_t_order["FLATFORM"].isnull()].shape[0] != 0:
        raise ValueError("100 and 200 are not the only value in FLATFORM column in df_t_order in {}".format(dbname))

    df_t_order = df_t_order[['IDX', 'FLATFORM', 'ORDNO', 'SESSID', 'USERNO', 'USERID', 'MEMBERLEVELNAME', 'SETTLEPRICE',
                             'ISFINISH', 'ISCONFIRM', 'CONFIRMDATE', "ISDELIVERYFINISH", "DELIVERYFINISHDATE", "ISCANCEL", 'CANCELDATE',
                             'REGDATE', 'TOTALORDERPRICE', 'GOODSNAME', 'GOODSNO'] + ["{}_T_MEMBER".format(x) for x in col_check]]

    lst_ind_nomember = df_t_order[df_t_order["USERNO"] == 0].index.tolist()
    df_t_order.loc[lst_ind_nomember, "NAME_T_MEMBER"] = "비회원"
    if df_t_order[df_t_order["NAME_T_MEMBER"].isnull()].shape[0] != 0:
        raise ValueError("There are null values in NAME_T_MEMBER columns after adding '비회원' where USERNO == 0..")
    else:
        for col in ["NAME_T_MEMBER", "SESSID", "USERID"]:
            df_t_order = df_t_order[~df_t_order[col].str.lower().str.replace(" ", "").str.contains("test|테스트")].copy()
        unique_userno = df_t_order.groupby(["USERNO"]).agg({"USERID": pd.Series.nunique, "NAME_T_MEMBER": pd.Series.nunique}).reset_index()
        for col in ["USERID", "NAME_T_MEMBER"]:
            if unique_userno["USERNO"].nunique() != unique_userno[col].sum():
                unique_userno_temp = unique_userno[unique_userno["USERID"] > 1].copy()
                for userno in unique_userno_temp["USERNO"].unique().tolist():
                    lst_ind_userno = df_t_order[df_t_order["USERNO"] == userno].index.tolist()
                    ind_userno_unique_sessid = df_t_order.loc[lst_ind_userno, "SESSID"].unique().tolist()
                    ind_userno_unique_sessid = [x for x in ind_userno_unique_sessid if len(x) > 1]
                    if len(ind_userno_unique_sessid) != 1:
                        raise ValueError("USERID does not have unique sessid..")
                    else:
                        ind_userno_unique_sessid = ind_userno_unique_sessid[0]
                    ind_userno_unique_userid = df_t_order.loc[lst_ind_userno, "USERID"].unique().tolist()
                    ind_userno_unique_userid = [x for x in ind_userno_unique_userid if len(x) > 1]
                    if len(ind_userno_unique_userid) != 1:
                        raise ValueError("USERID does not have unique userid..")
                    else:
                        ind_userno_unique_userid = ind_userno_unique_userid[0]

                    df_t_order.loc[lst_ind_userno, "SESSID"] = ind_userno_unique_sessid
                    df_t_order.loc[lst_ind_userno, "USERID"] = ind_userno_unique_userid
                    del lst_ind_userno, ind_userno_unique_sessid, ind_userno_unique_userid, userno

        unique_userno = df_t_order.groupby(["USERNO"]).agg({"USERID": pd.Series.nunique, "NAME_T_MEMBER": pd.Series.nunique,
                                                            "LEVELIDX_GUBUN_T_MEMBER": pd.Series.nunique}).reset_index()
        unique_userno.loc[unique_userno[unique_userno["USERNO"] == 0].index.tolist(), "LEVELIDX_GUBUN_T_MEMBER"] = 1
        for col in ["USERID", "NAME_T_MEMBER", "LEVELIDX_GUBUN_T_MEMBER"]:
            if unique_userno["USERNO"].nunique() != unique_userno[col].sum():
                raise ValueError("USERNO does not have unique {}s".format(col))
            del col
        del unique_userno

    df_t_order["regdate"] = df_t_order["REGDATE"].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
    df_t_order["REGDATE"] = df_t_order["REGDATE"].apply(lambda x: x.strftime("%Y-%m-%d"))
    df_t_order["weekNumber"] = df_t_order["REGDATE"].apply(
        lambda x: datetime(int(x.split("-")[0]), int(x.split("-")[1]), int(x.split("-")[2])).strftime("%V"))
    df_t_order["YYYYMM"] = df_t_order["REGDATE"].apply(lambda x: x[:-3])

    rows = df_t_order.shape[0]
    df_t_order = pd.merge(df_t_order, df_t_order_info, on=["IDX"], how="left")
    if rows != df_t_order.shape[0]:
        raise ValueError("IDX in df_t_order_info is no unique..")
    else:
        del df_t_order_info, rows
    df_t_order = df_t_order[~df_t_order["DEMAND"].str.lower().str.replace(" ", "").str.contains("test|테스트")].copy()
    df_t_order["DIV_ADDR"] = np.nan
    df_t_order.loc[df_t_order["SIDO"].notnull().tolist(), "DIV_ADDR"] = "확인된 주소"
    df_t_order.loc[df_t_order["SIDO"].isnull().tolist(), "DIV_ADDR"] = "미확인된 주소"

    rows = df_t_order.shape[0]
    ind_cancel_date_non = df_t_order[df_t_order["CANCELDATE"].isnull()].index.tolist()
    df_t_order.loc[ind_cancel_date_non, "CANCELDATE"] = np.nan
    del ind_cancel_date_non
    df_t_order["CANCELDATE"] = pd.to_datetime(df_t_order["CANCELDATE"])
    df_t_order = pd.merge(df_t_order, df_t_order_claim, on=["IDX", "USERNO", "USERID", "CANCELDATE"], how="left")
    if df_t_order.shape[0] != rows:
        raise ValueError("Rows do not match after merging df_t_order with df_t_order_claim..\nbefore merging: {}\nafter merging: {}".format(rows, df_t_order.shape[0]))
    else:
        del df_t_order_claim, rows

    lst_claim_col = ["CLAIMTYPE", "REASON", "REASONDETAIL", "REFUNDSTATE"]
    for col in lst_claim_col:
        df_t_order[col].fillna("NULL", inplace=True)
        del col
    for col in ["SIDO", "SIGUNGU"]:
        df_t_order[col].fillna("확인 불가", inplace=True)
        del col

    row = df_t_order.shape[0]
    if df_register_date["USERNO"].nunique(dropna=False) != df_register_date.shape[0]:
        raise ValueError("USERNO is not unique in df_register_date..")
    else:
        df_register_date.drop(["USERID"], axis=1, inplace=True)
    df_t_order = pd.merge(df_t_order, df_register_date, on=["USERNO"], how="left")
    if row != df_t_order.shape[0]:
        raise ValueError("Need to check df_register_date since USERNO are not unique after merging with df_t_order..")
    else:
        del row, df_register_date
        df_t_order = df_t_order[['FLATFORM', 'SESSID', 'USERNO', 'USERID', 'MEMBERLEVELNAME', 'USERKIND', 'USERKIND_CAT',
                                 'LEVELIDX_GUBUN_T_MEMBER', 'LEVEL_IDX_BEFORE_T_MEMBER', 'LEVELIDXBEFOREAFTER_T_MEMBER',
                                 'SETTLEPRICE', 'TOTALORDERPRICE', 'GOODSNAME', 'GOODSNO',
                                 'ISFINISH', 'ISCONFIRM', 'CONFIRMDATE', 'ISDELIVERYFINISH', 'DELIVERYFINISHDATE', 'ISCANCEL', 'CANCELDATE', 'REGDATE',
                                 'GENDER_T_MEMBER', 'AGE_BIN_T_MEMBER', 'AGE_BIN_ENG_T_MEMBER', 'STATE_T_MEMBER', 'SIDO_T_MEMBER',
                                 'SIGUNGU_T_MEMBER', 'regdate', 'weekNumber', 'YYYYMM', 'ORDADDR',
                                 'SIDO', 'SIGUNGU', 'DIV_ADDR', 'CLAIMTYPE', 'REASON',
                                 'REASONDETAIL', 'REFUNDSTATE', 'JOINDATE']]

    df_t_order["JOINDATE"] = df_t_order["JOINDATE"].astype(str).apply(lambda x: x.split(" ")[0])
    df_t_order["JOINDATE"] = df_t_order["JOINDATE"].str.replace("NaT", "JOINDATE UNAVAILABLE")
    df_t_order["MEMBERLEVELNAME"].value_counts(dropna=False)

    df_t_order_member = df_t_order[(df_t_order["MEMBERLEVELNAME"] != "비회원") & (df_t_order["USERKIND_CAT"] != "하이클래스")].copy()
    df_t_order_nomember = df_t_order[df_t_order["MEMBERLEVELNAME"] == "비회원"].copy()
    df_t_order_hiclassmember = df_t_order[(df_t_order["MEMBERLEVELNAME"] != "비회원") & (df_t_order["USERKIND_CAT"] == "하이클래스")].copy()

    rows_total = df_t_order.shape[0]
    rows_member = df_t_order_member.shape[0]
    rows_nomember = df_t_order_nomember.shape[0]
    rows_hiclassmember = df_t_order_hiclassmember.shape[0]
    if rows_total != rows_member + rows_nomember + rows_hiclassmember:
        raise ValueError("Need to check data..\ndf_t_order.shape[0]={}\ndf_t_order_member.shape[0]={}\nrows_nomember.shape[0]={}\nrows_hiclassmember.shape[0]={}".format(
            rows_total, rows_member, rows_nomember, rows_hiclassmember))
    else:
        del rows_total, rows_member, rows_nomember, rows_hiclassmember

    file_nm_total = "{}raw/total/df_t_order_{}.pickle".format(path_export, date)
    file_nm_member = "{}raw/member/df_t_order_member_{}.pickle".format(path_export, date)
    file_nm_nomember = "{}raw/nomember/df_t_order_nomember_{}.pickle".format(path_export, date)
    file_nm_hiclassmember = "{}raw/hiclassmember/df_t_order_hiclassmember_{}.pickle".format(path_export, date)

    export_pickle(file_nm_total, df_t_order)
    if df_t_order_member.shape[0] != 0:
        export_pickle(file_nm_member, df_t_order_member)
    if df_t_order_nomember.shape[0] != 0:
        export_pickle(file_nm_nomember, df_t_order_nomember)
    if df_t_order_hiclassmember.shape[0] != 0:
        export_pickle(file_nm_hiclassmember, df_t_order_hiclassmember)

    del df_t_order, df_t_order_member, df_t_order_nomember, df_t_order_hiclassmember, file_nm_member, file_nm_nomember, file_nm_hiclassmember


def homelearnfriendsmall_monthly_paid_user_total(date=None, dbname=None):
    from glob import glob
    import gzip
    import pickle
    import pandas as pd
    import numpy as np
    from datetime import datetime
    from functools import reduce
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory
    # path_export = file_directory

    df_t_recent_t_goods_linked = recent_t_goods_linked(dbname=dbname)

    file_nm_member = "{}raw/total/df_t_order_{}.pickle".format(path_import, date)
    if file_nm_member in glob("{}raw/total/*".format(path_import)):
        df_total = import_pickle(file_nm_member)
        del file_nm_member

        ind_nomember = df_total[df_total['USERNO'] == 0].index.tolist()
        ind_member = df_total[df_total['USERNO'] != 0].index.tolist()
        if df_total.shape[0] != len(ind_nomember) + len(ind_member):
            raise ValueError("Check the indices for 비회원 & (비회원이 아닌) 회원..")
        df_total.loc[ind_nomember, "USERNO"] = df_total.loc[ind_nomember, "SESSID"].copy()
        df_total.loc[ind_nomember, "MEMBERLEVELNAME"] = "nomember"
        df_total.loc[ind_member, "MEMBERLEVELNAME"] = "member"
        del ind_nomember

        df_total.drop(["FLATFORM", "USERKIND", "USERKIND_CAT", "LEVELIDXBEFOREAFTER_T_MEMBER", "LEVEL_IDX_BEFORE_T_MEMBER", "SESSID", "USERID", "LEVELIDX_GUBUN_T_MEMBER",
                       "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "STATE_T_MEMBER"], axis=1, inplace=True)

        df_total["CONFIRMSTATUS"] = df_total["ISFINISH"] + df_total["ISCONFIRM"]
        df_total["CONFIRMSTATUS"] = df_total["CONFIRMSTATUS"].map({"TT": "구매 완료", "FF": "미구매", "TF": "주문 완료 및 미입금"})
        if df_total[df_total["CONFIRMSTATUS"].isnull()].shape[0] != 0:
            raise ValueError("There are null values in CONFIRMSTATUS column in {}..".format(dbname))

        df_total["CANCELSTATUS"] = df_total["ISDELIVERYFINISH"] + df_total["ISCANCEL"]
        df_total["CANCELSTATUS"] = df_total["CANCELSTATUS"].map({"TT": "배송 후 주문 취소",
                                                                   "TF": "구매 확정",
                                                                   "FT": "배송 전 주문 취소",
                                                                   "FF": "배송 중"})
        if df_total[df_total["CANCELSTATUS"].isnull()].shape[0] != 0:
            raise ValueError("There are null values in CANCELSTATUS column in {}..".format(dbname))
        if df_total[df_total["CONFIRMSTATUS"] == "미구매"]["CANCELSTATUS"].nunique(dropna=False) == 1 and df_total[df_total["CONFIRMSTATUS"] == "미구매"]["CANCELSTATUS"].unique()[0] == "배송 중":
            df_total.loc[df_total[df_total["CONFIRMSTATUS"] == "미구매"].index.tolist(), "CANCELSTATUS"] = "미배송"
        else:
            raise ValueError("Need to check CONFIRMSTATUS == 미구매 for df_t_order in {}".format(dbname))

        col_agg = {"payment": "sum", "cancelPayment": "sum"}

        print("Creating 결제후 취소 건수 dataframe..")
        lst_filter = ["구매 완료", "주문 완료 및 미입금"]
        df_total_cancellation = df_total[df_total["CONFIRMSTATUS"].str.contains("|".join(lst_filter))].reset_index(drop=True)
        df_total_cancellation["payment"] = 1
        df_total_cancellation["cancelPayment"] = np.nan
        df_total_cancellation.loc[df_total_cancellation[df_total_cancellation["ISCANCEL"] == "T"].index, "cancelPayment"] = 1
        df_total_cancellation["cancelPayment"] = df_total_cancellation["cancelPayment"].fillna(0).astype(int)

        df_total_cancellation_total_daily = df_total_cancellation.groupby(["REGDATE"]).agg(col_agg).reset_index().rename(columns={"REGDATE": "Date"})
        df_total_cancellation_total_daily.insert(0, "div", "일별")
        df_total_cancellation_total_monthly = df_total_cancellation.groupby(["YYYYMM"]).agg(col_agg).reset_index().rename(columns={"YYYYMM": "Date"})
        df_total_cancellation_total_monthly.insert(0, "div", "월별")
        df_total_cancellation_g = pd.concat([df_total_cancellation_total_daily, df_total_cancellation_total_monthly], axis=0, ignore_index=True)
        del df_total_cancellation_total_daily, df_total_cancellation_total_monthly, df_total_cancellation
        df_total_cancellation_g["cancellationRatio"] = df_total_cancellation_g["cancelPayment"] / df_total_cancellation_g["payment"]

        df_total_export_raw = df_total.copy()
        df_total = df_total[(df_total["CONFIRMSTATUS"] == "구매 완료") & (df_total["CANCELSTATUS"] == "구매 확정")].reset_index(drop=True)

        for col in ["CONFIRMSTATUS", "CANCELSTATUS"]:
            v = ""
            if col == "CONFIRMSTATUS":
                v += "구매 완료"
            elif col == "CANCELSTATUS":
                v += "구매 확정"
            else:
                raise ValueError("No value defined..")
            if df_total[col].nunique() != 1 or df_total[col].unique().tolist()[0] != v:
                raise ValueError("Need to check data for {} column since ISFINISH==T and ISCONFIRM==T and ISDELIVERYFINISH==T and ISCANCEL==F..".format(col))
            del col, v

        print("Creating 매출 dataframe..")
        df_total_price_daily = df_total.groupby(["REGDATE"])["SETTLEPRICE"].sum().reset_index(name="settlePrice").rename(columns={"REGDATE": "Date"})
        df_total_price_daily.insert(0, "div", "일별")
        df_total_price_monthly = df_total.groupby(["YYYYMM"])["SETTLEPRICE"].sum().reset_index(name="settlePrice").rename(columns={"YYYYMM": "Date"})
        df_total_price_monthly.insert(0, "div", "월별")

        df_total_price_g = pd.concat([df_total_price_daily, df_total_price_monthly], axis=0, ignore_index=True)
        del df_total_price_daily, df_total_price_monthly

        df_total_payment_confirm = df_total.copy()
        df_total_payment_confirm["paymentConfirm"] = 1

        df_total_payment_confirm_daily = df_total_payment_confirm.groupby(["REGDATE"]).agg({"paymentConfirm": "sum"}).reset_index().rename(columns={"REGDATE": "Date"})
        df_total_payment_confirm_daily.insert(0, "div", "일별")
        df_total_payment_confirm_monthly = df_total_payment_confirm.groupby(["YYYYMM"]).agg({"paymentConfirm": "sum"}).reset_index().rename(columns={"YYYYMM": "Date"})
        df_total_payment_confirm_monthly.insert(0, "div", "월별")

        df_total_payment_confirm_g = pd.concat([df_total_payment_confirm_daily, df_total_payment_confirm_monthly], axis=0, ignore_index=True)
        del df_total_payment_confirm, df_total_payment_confirm_daily, df_total_payment_confirm_monthly

        print("Creating 구매자수 dataframe..")
        df_total_paid_user_daily = df_total.groupby(["REGDATE"])["USERNO"].nunique().reset_index(name="paidUser").rename(columns={"REGDATE": "Date", "paidUser": "paidUser_total"})
        df_total_paid_user_daily.insert(0, "div", "일별")

        temp_total_paid_user_memberlevelname_daily = df_total.groupby(["REGDATE", "MEMBERLEVELNAME"])["USERNO"].nunique().reset_index(name="paidUser").rename(columns={"REGDATE": "Date"})
        temp_total_paid_user_memberlevelname_daily.insert(0, "div", "일별")
        for col in ["paidUser_member", "paidUser_nomember"]:
            df_total_paid_user_daily[col] = np.nan
            del col
        for dt in temp_total_paid_user_memberlevelname_daily["Date"].unique():
            temp = temp_total_paid_user_memberlevelname_daily[temp_total_paid_user_memberlevelname_daily["Date"] == dt].copy()
            for val in temp["MEMBERLEVELNAME"].unique():
                result = temp.loc[(temp["Date"] == dt) & (temp["MEMBERLEVELNAME"] == val), "paidUser"].unique()[0]
                df_total_paid_user_daily.loc[(df_total_paid_user_daily["Date"] == dt),
                                             "paidUser_{}".format(val)] = result
                del val, result
            del dt, temp
        for col in ["paidUser_member", "paidUser_nomember"]:
            df_total_paid_user_daily[col] = df_total_paid_user_daily[col].fillna(0).astype(int)
            del col
        if df_total_paid_user_daily["paidUser_total"].sum() != df_total_paid_user_daily["paidUser_member"].sum() + df_total_paid_user_daily["paidUser_nomember"].sum():
            raise ValueError("daily) sum(paidUser_total) != sum(paidUser_member) + sum(paidUser_nomember)..")
        else:
            del temp_total_paid_user_memberlevelname_daily

        df_total_paid_user_monthly = df_total.groupby(["YYYYMM"])["USERNO"].nunique().reset_index(name="paidUser").rename(columns={"YYYYMM": "Date", "paidUser": "paidUser_total"})
        df_total_paid_user_monthly.insert(0, "div", "월별")

        temp_total_paid_user_memberlevelname_monthly = df_total.groupby(["YYYYMM", "MEMBERLEVELNAME"])["USERNO"].nunique().reset_index(name="paidUser").rename(columns={"YYYYMM": "Date"})
        temp_total_paid_user_memberlevelname_monthly.insert(0, "div", "월별")
        for col in ["paidUser_member", "paidUser_nomember"]:
            df_total_paid_user_monthly[col] = np.nan
            del col
        for dt in temp_total_paid_user_memberlevelname_monthly["Date"].unique():
            temp = temp_total_paid_user_memberlevelname_monthly[temp_total_paid_user_memberlevelname_monthly["Date"] == dt].copy()
            for val in temp["MEMBERLEVELNAME"].unique():
                result = temp.loc[(temp["Date"] == dt) & (temp["MEMBERLEVELNAME"] == val), "paidUser"].unique()[0]
                df_total_paid_user_monthly.loc[(df_total_paid_user_monthly["Date"] == dt),
                                               "paidUser_{}".format(val)] = result
                del val, result
            del dt, temp
        for col in ["paidUser_member", "paidUser_nomember"]:
            df_total_paid_user_monthly[col] = df_total_paid_user_monthly[col].fillna(0).astype(int)
            del col
        if df_total_paid_user_monthly["paidUser_total"].sum() != df_total_paid_user_monthly["paidUser_member"].sum() + df_total_paid_user_monthly["paidUser_nomember"].sum():
            raise ValueError("monthly) sum(paidUser_total) != sum(paidUser_member) + sum(paidUser_nomember)..")
        else:
            del temp_total_paid_user_memberlevelname_monthly

        df_total_paid_user_g = pd.concat([df_total_paid_user_daily, df_total_paid_user_monthly], axis=0, ignore_index=True)
        del df_total_paid_user_daily, df_total_paid_user_monthly

        print("Creating 재구매율 dataframe..")
        df_total_repurchase_daily = df_total.groupby(["USERNO", "MEMBERLEVELNAME"])["REGDATE"].apply(set).reset_index().rename(columns={"REGDATE": "REGDATES"})
        df_total_repurchase_daily = df_total_repurchase_daily[df_total_repurchase_daily["MEMBERLEVELNAME"] != "nomember"].reset_index(drop=True).drop(
            ["MEMBERLEVELNAME"], axis=1)
        df_total_repurchase_daily["REGDATES"] = df_total_repurchase_daily["REGDATES"].apply(lambda x: sorted(list(x)))
        df_total_repurchase_daily["purchaseDays"] = df_total_repurchase_daily["REGDATES"].apply(lambda x: len(x))
        df_total_repurchase_daily["repurchase"] = np.nan
        df_total_repurchase_daily.loc[df_total_repurchase_daily[df_total_repurchase_daily["purchaseDays"] == 1].index, "repurchase"] = "N"
        df_total_repurchase_daily.loc[df_total_repurchase_daily[df_total_repurchase_daily["purchaseDays"] > 1].index, "repurchase"] = "Y"
        df_total_repurchase_daily["firstREGDATE"] = df_total_repurchase_daily["REGDATES"].apply(lambda x: x[0])
        df_total_repurchase_daily = df_total_repurchase_daily.groupby(["firstREGDATE", "repurchase"])[
            "USERNO"].nunique().reset_index(name="numberOfUsers").rename(columns={"repurchase": "재구매 여부", "firstREGDATE": "Date"})
        df_total_repurchase_daily.insert(0, "div", "일별")

        df_total_repurchase_daily_repurchase_y = df_total_repurchase_daily[df_total_repurchase_daily["재구매 여부"] == "Y"].copy().reset_index(drop=True).rename(
            columns={"numberOfUsers": "numberOfUsers_repurchaseY"}).drop(["재구매 여부"], axis=1)
        df_total_repurchase_daily_repurchase_n = df_total_repurchase_daily[df_total_repurchase_daily["재구매 여부"] == "N"].copy().reset_index(drop=True).rename(
            columns={"numberOfUsers": "numberOfUsers_repurchaseN"}).drop(["재구매 여부"], axis=1)
        del df_total_repurchase_daily
        df_total_repurchase_daily = pd.merge(df_total_repurchase_daily_repurchase_y, df_total_repurchase_daily_repurchase_n, on=["div", "Date"], how="outer")
        del df_total_repurchase_daily_repurchase_y, df_total_repurchase_daily_repurchase_n

        df_total_repurchase_daily.sort_values(["div", "Date"], ignore_index=True, inplace=True)
        df_total_repurchase_daily["numberOfUsers_repurchaseY"].fillna(0, inplace=True)
        df_total_repurchase_daily["numberOfUsers_repurchaseN"].fillna(0, inplace=True)
        df_total_repurchase_daily[["numberOfUsers_repurchaseY", "numberOfUsers_repurchaseN"]] = df_total_repurchase_daily[["numberOfUsers_repurchaseY", "numberOfUsers_repurchaseN"]].astype(int)
        df_total_repurchase_daily.rename(columns={"numberOfUsers_repurchaseY": "재구매_Y", "numberOfUsers_repurchaseN": "재구매_N"}, inplace=True)
        df_total_repurchase_daily["재구매_총"] = df_total_repurchase_daily["재구매_Y"] + df_total_repurchase_daily["재구매_N"]
        df_total_repurchase_daily["재구매율"] = df_total_repurchase_daily["재구매_Y"] / df_total_repurchase_daily["재구매_총"]
        df_total_repurchase_daily["재구매율"] = df_total_repurchase_daily["재구매율"].fillna(0)

        print("Merging all the dataframes above..")
        df_total_g = reduce(lambda left, right: pd.merge(left, right, on=["div", "Date"], how="outer"),
                            [df_total_cancellation_g, df_total_payment_confirm_g, df_total_price_g, df_total_paid_user_g, df_total_repurchase_daily])
        del df_total_cancellation_g, df_total_payment_confirm_g, df_total_price_g, df_total_paid_user_g, df_total_repurchase_daily
        for col in ['paymentConfirm', 'settlePrice', 'paidUser_total', 'paidUser_member', 'paidUser_nomember', '재구매_Y', '재구매_N', '재구매_총', '재구매율']:
            ind_null = df_total_g[(df_total_g[col].isnull()) & (df_total_g["div"] == "일별")].index.tolist()
            df_total_g.loc[ind_null, col] = 0
            if col not in ['재구매_Y', '재구매_N', '재구매_총', '재구매율']:
                df_total_g[col] = df_total_g[col].astype(int)
            del col, ind_null

        df_total_g.insert(df_total_g.columns.tolist().index("Date") + 1, "weekday", np.nan)
        df_total_g_dau = df_total_g[df_total_g["div"] == "일별"].copy()
        df_total_g_dau["weekday"] = df_total_g_dau["Date"].apply(lambda x: datetime(year=int(x.split("-")[0]), month=int(x.split("-")[1]), day=int(x.split("-")[2])).strftime("%A"))
        df_total_g_mau = df_total_g[df_total_g["div"] == "월별"].copy()
        df_total_g = pd.concat([df_total_g_dau, df_total_g_mau], axis=0, ignore_index=True)
        del df_total_g_dau, df_total_g_mau
        df_total_g.insert(df_total_g.columns.tolist().index("weekday") + 1, "weekdayDiv", df_total_g["weekday"].copy())
        df_total_g["weekdayDiv"] = df_total_g["weekdayDiv"].map(
            {"Monday": "주중", "Tuesday": "주중", "Wednesday": "주중", "Thursday": "주중", "Friday": "주중",
             "Saturday": "주말", "Sunday": "주말"})
        df_total_g["weekday"] = df_total_g["weekday"].map(
            {"Monday": "월요일", "Tuesday": "화요일", "Wednesday": "수요일", "Thursday": "목요일", "Friday": "금요일",
             "Saturday": "토요일", "Sunday": "일요일"})

        f_nm_paid_user = "{}df_total_paid_user_{}.pickle".format(path_export, date)
        print("Exporting {}".format(f_nm_paid_user))
        with gzip.open(f_nm_paid_user, "wb") as f:
            pickle.dump(df_total_g, f)
            del f
        del df_total_g, f_nm_paid_user

        f_nm_analysis_db = "{}analysis_db/total/df_paid_user_member_analysis_db_{}.pickle".format(path_export, date)
        print("Exporting {}\n".format(f_nm_analysis_db))
        with gzip.open(f_nm_analysis_db, "wb") as f:
            pickle.dump(df_total_export_raw, f)
            del f
        del df_total_export_raw, f_nm_analysis_db

        del df_total

    else:
        print("There is no {}..".format(file_nm_member))


def homelearnfriendsmall_monthly_paid_user_member(date=None, dbname=None):
    from glob import glob
    import gzip
    import pickle
    import pandas as pd
    import numpy as np
    from datetime import datetime
    from functools import reduce
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory
    # path_export = file_directory

    df_t_recent_t_goods_linked = recent_t_goods_linked(dbname=dbname)

    file_nm_member = "{}raw/member/df_t_order_member_{}.pickle".format(path_import, date)
    if file_nm_member in glob("{}raw/member/*".format(path_import)):
        df_member = import_pickle(file_nm_member)
        del file_nm_member

        for col in ["GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "STATE_T_MEMBER"]:
            if df_member[df_member[col].isnull()].shape[0] != 0:
                raise ValueError("There are null values in {} column in df_member (dbname={})".format(col, dbname))
            del col
        df_member.drop(["STATE_T_MEMBER"], axis=1, inplace=True)
        for col in ["MEMBERLEVELNAME", "LEVELIDX_GUBUN_T_MEMBER"]:
            if df_member[df_member[col].isnull()].shape[0] != 0:
                raise ValueError("Need to check {} columns since there are null values..")
            del col
        df_member.insert(df_member.columns.tolist().index("LEVELIDX_GUBUN_T_MEMBER"), "LEVELIDX_GUBUN_T_MEMBER_FLATFORM",
                         df_member["LEVELIDX_GUBUN_T_MEMBER"] + "_" + df_member["FLATFORM"])
        df_member.drop(["USERKIND", "USERKIND_CAT", "SESSID", "USERID", "MEMBERLEVELNAME", "LEVELIDX_GUBUN_T_MEMBER"], axis=1, inplace=True)

        df_member["CONFIRMSTATUS"] = df_member["ISFINISH"] + df_member["ISCONFIRM"]
        df_member["CONFIRMSTATUS"] = df_member["CONFIRMSTATUS"].map({"TT": "구매 완료", "FF": "미구매", "TF": "주문 완료 및 미입금"})
        if df_member[df_member["CONFIRMSTATUS"].isnull()].shape[0] != 0:
            raise ValueError("There are null values in CONFIRMSTATUS column in {}..".format(dbname))

        df_member["CANCELSTATUS"] = df_member["ISDELIVERYFINISH"] + df_member["ISCANCEL"]
        df_member["CANCELSTATUS"] = df_member["CANCELSTATUS"].map({"TT": "배송 후 주문 취소",
                                                                   "TF": "구매 확정",
                                                                   "FT": "배송 전 주문 취소",
                                                                   "FF": "배송 중"})
        if df_member[df_member["CANCELSTATUS"].isnull()].shape[0] != 0:
            raise ValueError("There are null values in CANCELSTATUS column in {}..".format(dbname))
        if df_member[df_member["CONFIRMSTATUS"] == "미구매"]["CANCELSTATUS"].nunique(dropna=False) == 1 and df_member[df_member["CONFIRMSTATUS"] == "미구매"]["CANCELSTATUS"].unique()[0] == "배송 중":
            df_member.loc[df_member[df_member["CONFIRMSTATUS"] == "미구매"].index.tolist(), "CANCELSTATUS"] = "미배송"
        else:
            raise ValueError("Need to check CONFIRMSTATUS == 미구매 for df_t_order in {}".format(dbname))

        col_g_daily = ["REGDATE", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "DIV_ADDR", "SIDO", "SIGUNGU", "CLAIMTYPE", "REASON",
                       "REASONDETAIL", "CONFIRMSTATUS", "CANCELSTATUS", "GOODSNAME", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER"]
        col_g_monthly = ["YYYYMM", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "DIV_ADDR", "SIDO", "SIGUNGU", "CLAIMTYPE",
                         "REASON", "REASONDETAIL", "CONFIRMSTATUS", "CANCELSTATUS", "GOODSNAME", "GENDER_T_MEMBER",
                         "AGE_BIN_T_MEMBER"]
        col_agg = {"payment": "sum", "cancelPayment": "sum"}

        df_member_region = df_member.groupby(col_g_daily)["USERNO"].count().reset_index().rename(
            columns={"USERNO": "COUNT"})
        if df_member_region["COUNT"].sum() != df_member.shape[0]:
            raise ValueError("Need to check data for USERNO count..")

        print("Creating 결제후 취소 건수 dataframe..")
        lst_filter = ["구매 완료", "주문 완료 및 미입금"]
        df_member_cancellation = df_member[df_member["CONFIRMSTATUS"].str.contains("|".join(lst_filter))].reset_index(drop=True)
        df_member_cancellation["payment"] = 1
        df_member_cancellation["cancelPayment"] = np.nan
        df_member_cancellation.loc[df_member_cancellation[df_member_cancellation["ISCANCEL"] == "T"].index, "cancelPayment"] = 1
        df_member_cancellation["cancelPayment"] = df_member_cancellation["cancelPayment"].fillna(0).astype(int)

        df_member_cancellation_total_daily = df_member_cancellation.groupby(["REGDATE", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"]).agg(col_agg).reset_index().rename(columns={"REGDATE": "Date"})
        df_member_cancellation_total_daily.insert(0, "div", "일별")
        df_member_cancellation_total_monthly = df_member_cancellation.groupby(["YYYYMM", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"]).agg(col_agg).reset_index().rename(columns={"YYYYMM": "Date"})
        df_member_cancellation_total_monthly.insert(0, "div", "월별")
        df_member_cancellation_g = pd.concat([df_member_cancellation_total_daily, df_member_cancellation_total_monthly], axis=0, ignore_index=True)
        del df_member_cancellation_total_daily, df_member_cancellation_total_monthly

        print("Creating 결제후 취소 건수 리스트 dataframe..")
        df_member_cancellation_daily = df_member_cancellation.groupby(col_g_daily).agg(col_agg).reset_index().rename(
            columns={"REGDATE": "Date", "payment": "paymentCount", "cancelPayment": "cancelPaymentCount"})
        df_member_cancellation_daily.insert(0, "div", "일별")

        df_member_cancellation_monthly = df_member_cancellation.groupby(col_g_monthly).agg(col_agg).reset_index().rename(
            columns={"YYYYMM": "Date", "payment": "paymentCount", "cancelPayment": "cancelPaymentCount"})
        df_member_cancellation_monthly.insert(0, "div", "월별")

        df_member_cancellation_list_g = pd.concat([df_member_cancellation_daily, df_member_cancellation_monthly], axis=0, ignore_index=True)
        del df_member_cancellation_daily, df_member_cancellation_monthly

        for col in ["payment", "cancelPayment"]:
            if df_member_cancellation_list_g["{}Count".format(col)].sum() != df_member_cancellation_g[col].sum():
                raise ValueError("{} does not match for df_t_order_cancellation_list_g[{}].sum() and df_member_cancellation_g[{}].sum()".format(
                    col, col, col))
            del col

        print("Creating total sequence..")
        df_member_cancellation_order = df_member_cancellation[
            ["DIV_ADDR", "SIDO", "SIGUNGU", "USERNO", "FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER", "GOODSNAME", "CANCELSTATUS", "regdate", "CLAIMTYPE", "REASON", "REASONDETAIL"]].copy().rename(columns={"regdate": "DATE"})
        df_member_cancellation_confirm = df_member_cancellation[
            ["DIV_ADDR", "SIDO", "SIGUNGU", "USERNO", "FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER", "GOODSNAME", "CANCELSTATUS", "CONFIRMDATE", "CLAIMTYPE", "REASON", "REASONDETAIL"]].copy().rename(columns={"CONFIRMDATE": "DATE"})
        df_member_cancellation_delivery = df_member_cancellation[
            ["DIV_ADDR", "SIDO", "SIGUNGU", "USERNO", "FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER", "GOODSNAME", "CANCELSTATUS", "DELIVERYFINISHDATE", "CLAIMTYPE", "REASON", "REASONDETAIL"]].copy().rename(columns={"DELIVERYFINISHDATE": "DATE"})
        df_member_cancellation_cancel = df_member_cancellation[
            ["DIV_ADDR", "SIDO", "SIGUNGU", "USERNO", "FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER", "GOODSNAME", "CANCELSTATUS", "CANCELDATE", "CLAIMTYPE", "REASON", "REASONDETAIL"]].copy().rename(columns={"CANCELDATE": "DATE"})

        df_member_cancellation_order = df_member_cancellation_order[df_member_cancellation_order["DATE"].notnull()].copy()
        df_member_cancellation_confirm = df_member_cancellation_confirm[df_member_cancellation_confirm["DATE"].notnull()].copy()
        df_member_cancellation_delivery = df_member_cancellation_delivery[df_member_cancellation_delivery["DATE"].notnull()].copy()
        df_member_cancellation_cancel = df_member_cancellation_cancel[df_member_cancellation_cancel["DATE"].notnull()].copy()

        df_member_cancellation_order["DATE"] = pd.to_datetime(df_member_cancellation_order["DATE"])
        df_member_cancellation_confirm["DATE"] = pd.to_datetime(df_member_cancellation_confirm["DATE"])
        df_member_cancellation_delivery["DATE"] = pd.to_datetime(df_member_cancellation_delivery["DATE"])
        df_member_cancellation_cancel["DATE"] = pd.to_datetime(df_member_cancellation_cancel["DATE"])

        df_member_cancellation_order["STATUS"] = "1.주문일자"
        df_member_cancellation_confirm["STATUS"] = "2.입금확인일자"
        df_member_cancellation_delivery["STATUS"] = "3.배송완료일자"
        df_member_cancellation_cancel["STATUS"] = "4.주문취소일자"

        df_member_cancellation_seq = pd.concat([df_member_cancellation_order, df_member_cancellation_confirm, df_member_cancellation_delivery, df_member_cancellation_cancel],
                                               axis=0).sort_values(by=["DATE", "STATUS"], ascending=["True", "True"], ignore_index=True)
        del df_member_cancellation, df_member_cancellation_order, df_member_cancellation_confirm, df_member_cancellation_delivery, df_member_cancellation_cancel
        df_member_export_raw = df_member.copy()
        df_member = df_member[(df_member["CONFIRMSTATUS"] == "구매 완료") & (df_member["CANCELSTATUS"] == "구매 확정")].reset_index(drop=True)

        for col in ["CONFIRMSTATUS", "CANCELSTATUS"]:
            v = ""
            if col == "CONFIRMSTATUS":
                v += "구매 완료"
            elif col == "CANCELSTATUS":
                v += "구매 확정"
            else:
                raise ValueError("No value defined..")
            if df_member[col].nunique() != 1 or df_member[col].unique().tolist()[0] != v:
                raise ValueError("Need to check data for {} column since ISFINISH==T and ISCONFIRM==T and ISDELIVERYFINISH==T and ISCANCEL==F..".format(col))
            del col, v

        print("Creating 매출 dataframe..")
        df_member_price_daily = df_member.groupby(["REGDATE", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"])["SETTLEPRICE"].sum().reset_index(name="settlePrice").rename(columns={"REGDATE": "Date"})
        df_member_price_daily.insert(0, "div", "일별")
        df_member_price_monthly = df_member.groupby(["YYYYMM", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"])["SETTLEPRICE"].sum().reset_index(name="settlePrice").rename(columns={"YYYYMM": "Date"})
        df_member_price_monthly.insert(0, "div", "월별")

        df_member_price_g = pd.concat([df_member_price_daily, df_member_price_monthly], axis=0, ignore_index=True)
        del df_member_price_daily, df_member_price_monthly

        df_member_payment_confirm = df_member.copy()
        df_member_payment_confirm["paymentConfirm"] = 1

        df_member_payment_confirm_daily = df_member_payment_confirm.groupby(["REGDATE", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"]).agg({"paymentConfirm": "sum"}).reset_index().rename(columns={"REGDATE": "Date"})
        df_member_payment_confirm_daily.insert(0, "div", "일별")
        df_member_payment_confirm_monthly = df_member_payment_confirm.groupby(["YYYYMM", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"]).agg({"paymentConfirm": "sum"}).reset_index().rename(columns={"YYYYMM": "Date"})
        df_member_payment_confirm_monthly.insert(0, "div", "월별")

        df_member_payment_confirm_g = pd.concat([df_member_payment_confirm_daily, df_member_payment_confirm_monthly], axis=0, ignore_index=True)
        del df_member_payment_confirm, df_member_payment_confirm_daily, df_member_payment_confirm_monthly

        print("Creating 구매자수 dataframe..")
        df_member_paid_user_daily = df_member.groupby(["REGDATE", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"])["USERNO"].nunique().reset_index(name="paidUser").rename(columns={"REGDATE": "Date"})
        df_member_paid_user_daily.insert(0, "div", "일별")
        df_member_paid_user_monthly = df_member.groupby(["YYYYMM", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"])["USERNO"].nunique().reset_index(name="paidUser").rename(columns={"YYYYMM": "Date"})
        df_member_paid_user_monthly.insert(0, "div", "월별")

        df_member_paid_user_g = pd.concat([df_member_paid_user_daily, df_member_paid_user_monthly], axis=0, ignore_index=True)
        del df_member_paid_user_daily, df_member_paid_user_monthly

        print("Creating 재구매율 dataframe..")
        df_member_repurchase_daily = df_member.groupby(["USERNO", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"])["REGDATE"].apply(set).reset_index().rename(columns={"REGDATE": "REGDATES"})
        df_member_repurchase_daily["REGDATES"] = df_member_repurchase_daily["REGDATES"].apply(lambda x: sorted(list(x)))
        df_member_repurchase_daily["purchaseDays"] = df_member_repurchase_daily["REGDATES"].apply(lambda x: len(x))
        df_member_repurchase_daily["repurchase"] = np.nan
        df_member_repurchase_daily.loc[df_member_repurchase_daily[df_member_repurchase_daily["purchaseDays"] == 1].index, "repurchase"] = "N"
        df_member_repurchase_daily.loc[df_member_repurchase_daily[df_member_repurchase_daily["purchaseDays"] > 1].index, "repurchase"] = "Y"
        df_member_repurchase_daily["firstREGDATE"] = df_member_repurchase_daily["REGDATES"].apply(lambda x: x[0])
        df_member_repurchase_daily = df_member_repurchase_daily.groupby(["firstREGDATE", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "repurchase", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"])[
            "USERNO"].nunique().reset_index(name="numberOfUsers").rename(columns={"repurchase": "재구매 여부", "firstREGDATE": "Date"})
        df_member_repurchase_daily.insert(0, "div", "일별")

        df_member_repurchase_daily_repurchase_y = df_member_repurchase_daily[df_member_repurchase_daily["재구매 여부"] == "Y"].copy().reset_index(drop=True).rename(
            columns={"numberOfUsers": "numberOfUsers_repurchaseY"}).drop(["재구매 여부"], axis=1)
        df_member_repurchase_daily_repurchase_n = df_member_repurchase_daily[df_member_repurchase_daily["재구매 여부"] == "N"].copy().reset_index(drop=True).rename(
            columns={"numberOfUsers": "numberOfUsers_repurchaseN"}).drop(["재구매 여부"], axis=1)
        del df_member_repurchase_daily
        df_member_repurchase_daily = pd.merge(df_member_repurchase_daily_repurchase_y, df_member_repurchase_daily_repurchase_n, on=["div", "Date", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"], how="outer")
        del df_member_repurchase_daily_repurchase_y, df_member_repurchase_daily_repurchase_n

        df_member_repurchase_daily.sort_values(["div", "Date"], ignore_index=True, inplace=True)
        df_member_repurchase_daily["numberOfUsers_repurchaseY"].fillna(0, inplace=True)
        df_member_repurchase_daily["numberOfUsers_repurchaseN"].fillna(0, inplace=True)
        df_member_repurchase_daily[["numberOfUsers_repurchaseY", "numberOfUsers_repurchaseN"]] = df_member_repurchase_daily[["numberOfUsers_repurchaseY", "numberOfUsers_repurchaseN"]].astype(int)
        df_member_repurchase_daily.rename(columns={"numberOfUsers_repurchaseY": "재구매_Y", "numberOfUsers_repurchaseN": "재구매_N"}, inplace=True)

        print("Creating 첫 구매 후 두 번째 구매까지 걸리는 시간 dataframe..")
        df_member_repurchase_first_to_second_purchase_daily = df_member.groupby(["USERNO", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"])["regdate"].apply(set).reset_index().rename(columns={"regdate": "regdates"})
        df_member_repurchase_first_to_second_purchase_daily["regdates"] = df_member_repurchase_first_to_second_purchase_daily["regdates"].apply(lambda x: sorted(list(x)))
        df_member_repurchase_first_to_second_purchase_daily["regdatesLst"] = df_member_repurchase_first_to_second_purchase_daily["regdates"].apply(lambda x: [dt.split(" ")[0] for dt in x])
        df_member_repurchase_first_to_second_purchase_daily["regdatesLstUnique"] = df_member_repurchase_first_to_second_purchase_daily["regdatesLst"].apply(set)
        df_member_repurchase_first_to_second_purchase_daily["regdatesLstUnique"] = df_member_repurchase_first_to_second_purchase_daily["regdatesLstUnique"].apply(lambda x: sorted(list(x)))
        df_member_repurchase_first_to_second_purchase_daily["purchaseDays"] = df_member_repurchase_first_to_second_purchase_daily["regdatesLstUnique"].apply(len)

        purchase_date_greater_than_one = df_member_repurchase_first_to_second_purchase_daily[df_member_repurchase_first_to_second_purchase_daily["purchaseDays"] > 1].reset_index(drop=True)

        purchase_date_greater_than_one["firstRegdate"] = purchase_date_greater_than_one["regdatesLstUnique"].apply(lambda x: x[0])
        purchase_date_greater_than_one["secondRegdate"] = purchase_date_greater_than_one["regdatesLstUnique"].apply(lambda x: x[1])

        purchase_date_greater_than_one["firstRegdatesTime"] = np.nan
        purchase_date_greater_than_one["secondRegdatesTime"] = np.nan
        for ind in purchase_date_greater_than_one.index:
            first_regdate_index = purchase_date_greater_than_one.loc[ind, "firstRegdate"]
            second_regdate_index = purchase_date_greater_than_one.loc[ind, "secondRegdate"]

            first_regdate_index = purchase_date_greater_than_one.loc[ind, "regdatesLst"].index(first_regdate_index)
            second_regdate_index = purchase_date_greater_than_one.loc[ind, "regdatesLst"].index(second_regdate_index)

            purchase_date_greater_than_one.loc[ind, "firstRegdatesTime"] = purchase_date_greater_than_one.loc[ind, "regdates"][first_regdate_index]
            purchase_date_greater_than_one.loc[ind, "secondRegdatesTime"] = purchase_date_greater_than_one.loc[ind, "regdates"][second_regdate_index]
            del ind, first_regdate_index, second_regdate_index

        for col in ["firstRegdatesTime", "secondRegdatesTime"]:
            purchase_date_greater_than_one[col] = pd.to_datetime(purchase_date_greater_than_one[col])
            del col
        purchase_date_greater_than_one.drop(["regdates", "regdatesLst", "regdatesLstUnique", "purchaseDays", "firstRegdate", "secondRegdate"], axis=1, inplace=True)

        purchase_date_equal_to_one = df_member_repurchase_first_to_second_purchase_daily[df_member_repurchase_first_to_second_purchase_daily["purchaseDays"] == 1].reset_index(drop=True)
        purchase_date_equal_to_one["firstRegdatesTime"] = purchase_date_equal_to_one["regdates"].apply(lambda x: x[0])
        purchase_date_equal_to_one["secondRegdatesTime"] = np.nan
        purchase_date_equal_to_one.drop(["regdates", "regdatesLst", "regdatesLstUnique", "purchaseDays"], axis=1, inplace=True)

        if df_member_repurchase_first_to_second_purchase_daily.shape[0] != purchase_date_greater_than_one.shape[0] + purchase_date_equal_to_one.shape[0]:
            raise ValueError("1. Need to check data..")
        else:
            df_member_repurchase_first_to_second_purchase_daily = pd.concat([purchase_date_greater_than_one, purchase_date_equal_to_one], axis=0, ignore_index=True)
            del purchase_date_greater_than_one, purchase_date_equal_to_one

        print("Merging all the dataframes above..")
        df_member_g = reduce(lambda left, right: pd.merge(left, right, on=["div", "Date", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"], how="outer"),
                            [df_member_cancellation_g, df_member_payment_confirm_g, df_member_price_g, df_member_paid_user_g, df_member_repurchase_daily])
        del df_member_cancellation_g, df_member_payment_confirm_g, df_member_price_g, df_member_paid_user_g, df_member_repurchase_daily

        df_member_g.insert(df_member_g.columns.tolist().index("Date") + 1, "weekday", np.nan)
        df_member_g_dau = df_member_g[df_member_g["div"] == "일별"].copy()
        df_member_g_dau["weekday"] = df_member_g_dau["Date"].apply(lambda x: datetime(year=int(x.split("-")[0]), month=int(x.split("-")[1]), day=int(x.split("-")[2])).strftime("%A"))
        df_member_g_mau = df_member_g[df_member_g["div"] == "월별"].copy()
        df_member_g = pd.concat([df_member_g_dau, df_member_g_mau], axis=0, ignore_index=True)
        del df_member_g_dau, df_member_g_mau
        df_member_g.insert(df_member_g.columns.tolist().index("weekday") + 1, "weekdayDiv", df_member_g["weekday"].copy())
        df_member_g["weekdayDiv"] = df_member_g["weekdayDiv"].map(
            {"Monday": "주중", "Tuesday": "주중", "Wednesday": "주중", "Thursday": "주중", "Friday": "주중",
             "Saturday": "주말", "Sunday": "주말"})
        df_member_g["weekday"] = df_member_g["weekday"].map(
            {"Monday": "월요일", "Tuesday": "화요일", "Wednesday": "수요일", "Thursday": "목요일", "Friday": "금요일",
             "Saturday": "토요일", "Sunday": "일요일"})
        for col in ["paymentConfirm", "settlePrice", "paidUser", "재구매_Y", "재구매_N"]:
            df_member_g[col] = df_member_g[col].fillna(0).astype(int)
            del col

        print("Creating 회원가입일별 구매 확정수 dataframe..")
        df_member_by_join_date = df_member.copy()
        df_member_by_join_date.insert(df_member_by_join_date.columns.tolist().index("JOINDATE")+1, "DIV_JOINDATE", np.nan)
        ind_join_date = [x.strftime("%Y-%m-%d") for x in pd.date_range(start="2020-03-06", end=datetime.today().strftime("%Y-%m-%d"), freq="D")]
        ind_join_date = "|".join(ind_join_date)
        ind_join_date = df_member_by_join_date[df_member_by_join_date["JOINDATE"].str.contains("{}".format(ind_join_date))].index.tolist()
        df_member_by_join_date.loc[ind_join_date, "DIV_JOINDATE"] = "2020-03-06 이후"
        df_member_by_join_date["DIV_JOINDATE"].fillna("2020-03-05 이전", inplace=True)
        del ind_join_date

        df_member_by_join_date["JOINDATE"] = df_member_by_join_date["JOINDATE"].apply(lambda x: x[:-3])
        df_member_by_join_date = df_member_by_join_date.groupby(["YYYYMM", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "JOINDATE", "DIV_JOINDATE"]).agg({"SETTLEPRICE": "sum", "USERNO": pd.Series.nunique}).reset_index().rename(
            columns={"YYYYMM": "paymentConfirmDate", "USERNO": "UNIQUEUSERNO"})

        f_nm_order_seq = "{}df_member_order_sequence_{}.pickle".format(path_export, date)
        print("Exporting {}".format(f_nm_order_seq))
        with gzip.open(f_nm_order_seq, "wb") as f:
            pickle.dump(df_member_cancellation_seq, f)
            del f
        del df_member_cancellation_seq, f_nm_order_seq

        f_nm_cancellation_list = "{}df_member_cancellation_list_{}.pickle".format(path_export, date)
        print("Exporting {}".format(f_nm_cancellation_list))
        with gzip.open(f_nm_cancellation_list, "wb") as f:
            pickle.dump(df_member_cancellation_list_g, f)
            del f
        del df_member_cancellation_list_g, f_nm_cancellation_list

        f_nm_order_region = "{}df_member_order_region_{}.pickle".format(path_export, date)
        print("Exporting {}".format(f_nm_order_region))
        with gzip.open(f_nm_order_region, "wb") as f:
            pickle.dump(df_member_region, f)
            del f
        del df_member_region, f_nm_order_region

        f_nm_paid_user = "{}df_member_paid_user_{}.pickle".format(path_export, date)
        print("Exporting {}".format(f_nm_paid_user))
        with gzip.open(f_nm_paid_user, "wb") as f:
            pickle.dump(df_member_g, f)
            del f
        del df_member_g, f_nm_paid_user

        f_nm_first_to_second_purchase = "{}df_member_first_to_second_purchase_{}.pickle".format(path_export, date)
        print("Exporting {}".format(f_nm_first_to_second_purchase))
        with gzip.open(f_nm_first_to_second_purchase, "wb") as f:
            pickle.dump(df_member_repurchase_first_to_second_purchase_daily, f)
            del f
        del df_member_repurchase_first_to_second_purchase_daily, f_nm_first_to_second_purchase

        f_nm_by_join_date = "{}df_member_by_join_date_{}.pickle".format(path_export, date)
        print("Exporting {}".format(f_nm_by_join_date))
        with gzip.open(f_nm_by_join_date, "wb") as f:
            pickle.dump(df_member_by_join_date, f)
            del f
        del df_member_by_join_date, f_nm_by_join_date

        f_nm_analysis_db = "{}analysis_db/member/df_paid_user_member_analysis_db_{}.pickle".format(path_export, date)
        print("Exporting {}\n".format(f_nm_analysis_db))
        with gzip.open(f_nm_analysis_db, "wb") as f:
            pickle.dump(df_member_export_raw, f)
            del f
        del df_member_export_raw, f_nm_analysis_db

        del df_member

    else:
        print("There is no {}..".format(file_nm_member))


def homelearnfriendsmall_monthly_paid_user_nomember(date=None, dbname=None):
    from glob import glob
    import gzip
    import pickle
    import pandas as pd
    import numpy as np
    from datetime import datetime
    from functools import reduce
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory
    # path_export = file_directory

    df_t_recent_t_goods_linked = recent_t_goods_linked(dbname=dbname)

    file_nm_nomember = "{}raw/nomember/df_t_order_nomember_{}.pickle".format(path_import, date)
    if file_nm_nomember in glob("{}raw/nomember/*".format(path_import)):
        df_nomember = import_pickle(file_nm_nomember)
        del file_nm_nomember

        ind_nomember = df_nomember[df_nomember['USERNO'] == 0].index.tolist()
        ind_member = df_nomember[df_nomember['USERNO'] != 0].index.tolist()
        if df_nomember.shape[0] != len(ind_nomember) + len(ind_member):
            raise ValueError("Check the indices for 비회원 & (비회원이 아닌) 회원..")
        df_nomember.loc[ind_nomember, "USERID"] = df_nomember.loc[ind_nomember, "SESSID"].copy()
        df_nomember.loc[ind_nomember, "MEMBERLEVELNAME"] = "nomember"
        df_nomember.loc[ind_member, "MEMBERLEVELNAME"] = "member"
        del ind_nomember

        df_nomember.drop(['SESSID', 'USERNO', 'MEMBERLEVELNAME', 'USERKIND', 'USERKIND_CAT', 'LEVELIDX_GUBUN_T_MEMBER',
                          'LEVEL_IDX_BEFORE_T_MEMBER', 'LEVELIDXBEFOREAFTER_T_MEMBER', 'GENDER_T_MEMBER',
                          'AGE_BIN_T_MEMBER', 'AGE_BIN_ENG_T_MEMBER', 'STATE_T_MEMBER', 'SIDO_T_MEMBER',
                          'SIGUNGU_T_MEMBER'], axis=1, inplace=True)

        df_nomember["CONFIRMSTATUS"] = df_nomember["ISFINISH"] + df_nomember["ISCONFIRM"]
        df_nomember["CONFIRMSTATUS"] = df_nomember["CONFIRMSTATUS"].map({"TT": "구매 완료", "FF": "미구매", "TF": "주문 완료 및 미입금"})
        if df_nomember[df_nomember["CONFIRMSTATUS"].isnull()].shape[0] != 0:
            raise ValueError("There are null values in CONFIRMSTATUS column in {}..".format(dbname))

        df_nomember["CANCELSTATUS"] = df_nomember["ISDELIVERYFINISH"] + df_nomember["ISCANCEL"]
        df_nomember["CANCELSTATUS"] = df_nomember["CANCELSTATUS"].map({"TT": "배송 후 주문 취소",
                                                                   "TF": "구매 확정",
                                                                   "FT": "배송 전 주문 취소",
                                                                   "FF": "배송 중"})
        if df_nomember[df_nomember["CANCELSTATUS"].isnull()].shape[0] != 0:
            raise ValueError("There are null values in CANCELSTATUS column in {}..".format(dbname))
        if df_nomember[df_nomember["CONFIRMSTATUS"] == "미구매"]["CANCELSTATUS"].nunique(dropna=False) == 1 and \
                df_nomember[df_nomember["CONFIRMSTATUS"] == "미구매"]["CANCELSTATUS"].unique()[0] == "배송 중":
            df_nomember.loc[df_nomember[df_nomember["CONFIRMSTATUS"] == "미구매"].index.tolist(), "CANCELSTATUS"] = "미배송"
        else:
            raise ValueError("Need to check CONFIRMSTATUS == 미구매 for df_t_order in {}".format(dbname))

        col_g_daily = ["REGDATE", "FLATFORM", "DIV_ADDR", "SIDO", "SIGUNGU", "CLAIMTYPE", "REASON", "REASONDETAIL", "CONFIRMSTATUS", "CANCELSTATUS", "GOODSNAME"]
        col_g_monthly = ["YYYYMM", "FLATFORM", "DIV_ADDR", "SIDO", "SIGUNGU", "CLAIMTYPE", "REASON", "REASONDETAIL", "CONFIRMSTATUS", "CANCELSTATUS", "GOODSNAME"]
        col_agg = {"payment": "sum", "cancelPayment": "sum"}

        df_nomember_region = df_nomember.groupby(col_g_daily)["USERID"].count().reset_index().rename(
            columns={"USERID": "COUNT"})
        if df_nomember_region["COUNT"].sum() != df_nomember.shape[0]:
            raise ValueError("Need to check data for USERID count..")

        print("Creating 결제후 취소 건수 dataframe..")
        lst_filter = ["구매 완료", "주문 완료 및 미입금"]
        df_nomember_cancellation = df_nomember[df_nomember["CONFIRMSTATUS"].str.contains("|".join(lst_filter))].reset_index(
            drop=True)
        df_nomember_cancellation["payment"] = 1
        df_nomember_cancellation["cancelPayment"] = np.nan
        df_nomember_cancellation.loc[
            df_nomember_cancellation[df_nomember_cancellation["ISCANCEL"] == "T"].index, "cancelPayment"] = 1
        df_nomember_cancellation["cancelPayment"] = df_nomember_cancellation["cancelPayment"].fillna(0).astype(int)

        df_nomember_cancellation_total_daily = df_nomember_cancellation.groupby(
            ["REGDATE", "FLATFORM"]).agg(col_agg).reset_index().rename(columns={"REGDATE": "Date"})
        df_nomember_cancellation_total_daily.insert(0, "div", "일별")
        df_nomember_cancellation_total_monthly = df_nomember_cancellation.groupby(
            ["YYYYMM", "FLATFORM"]).agg(col_agg).reset_index().rename(columns={"YYYYMM": "Date"})
        df_nomember_cancellation_total_monthly.insert(0, "div", "월별")
        df_nomember_cancellation_g = pd.concat([df_nomember_cancellation_total_daily, df_nomember_cancellation_total_monthly],
                                             axis=0, ignore_index=True)
        del df_nomember_cancellation_total_daily, df_nomember_cancellation_total_monthly

        print("Creating 결제후 취소 건수 리스트 dataframe..")
        df_nomember_cancellation_daily = df_nomember_cancellation.groupby(col_g_daily).agg(col_agg).reset_index().rename(
            columns={"REGDATE": "Date", "payment": "paymentCount", "cancelPayment": "cancelPaymentCount"})
        df_nomember_cancellation_daily.insert(0, "div", "일별")

        df_nomember_cancellation_monthly = df_nomember_cancellation.groupby(col_g_monthly).agg(
            col_agg).reset_index().rename(
            columns={"YYYYMM": "Date", "payment": "paymentCount", "cancelPayment": "cancelPaymentCount"})
        df_nomember_cancellation_monthly.insert(0, "div", "월별")

        df_nomember_cancellation_list_g = pd.concat([df_nomember_cancellation_daily, df_nomember_cancellation_monthly],
                                                  axis=0, ignore_index=True)
        del df_nomember_cancellation_daily, df_nomember_cancellation_monthly

        for col in ["payment", "cancelPayment"]:
            if df_nomember_cancellation_list_g["{}Count".format(col)].sum() != df_nomember_cancellation_g[col].sum():
                raise ValueError(
                    "{} does not match for df_t_order_cancellation_list_g[{}].sum() and df_nomember_cancellation_g[{}].sum()".format(
                        col, col, col))
            del col

        print("Creating total sequence..")
        df_nomember_cancellation_order = df_nomember_cancellation[
            ["USERID", "FLATFORM", "DIV_ADDR", "SIDO", "SIGUNGU", "GOODSNAME", "CANCELSTATUS", "regdate", "CLAIMTYPE", "REASON", "REASONDETAIL"]].copy().rename(
            columns={"regdate": "DATE"})
        df_nomember_cancellation_confirm = df_nomember_cancellation[
            ["USERID", "FLATFORM", "DIV_ADDR", "SIDO", "SIGUNGU", "GOODSNAME", "CANCELSTATUS", "CONFIRMDATE", "CLAIMTYPE", "REASON", "REASONDETAIL"]].copy().rename(
            columns={"CONFIRMDATE": "DATE"})
        df_nomember_cancellation_delivery = df_nomember_cancellation[
            ["USERID", "FLATFORM", "DIV_ADDR", "SIDO", "SIGUNGU", "GOODSNAME", "CANCELSTATUS", "DELIVERYFINISHDATE", "CLAIMTYPE", "REASON", "REASONDETAIL"]].copy().rename(
            columns={"DELIVERYFINISHDATE": "DATE"})
        df_nomember_cancellation_cancel = df_nomember_cancellation[
            ["USERID", "FLATFORM", "DIV_ADDR", "SIDO", "SIGUNGU", "GOODSNAME", "CANCELSTATUS", "CANCELDATE", "CLAIMTYPE", "REASON", "REASONDETAIL"]].copy().rename(
            columns={"CANCELDATE": "DATE"})

        df_nomember_cancellation_order = df_nomember_cancellation_order[df_nomember_cancellation_order["DATE"].notnull()].copy()
        df_nomember_cancellation_confirm = df_nomember_cancellation_confirm[df_nomember_cancellation_confirm["DATE"].notnull()].copy()
        df_nomember_cancellation_delivery = df_nomember_cancellation_delivery[df_nomember_cancellation_delivery["DATE"].notnull()].copy()
        df_nomember_cancellation_cancel = df_nomember_cancellation_cancel[df_nomember_cancellation_cancel["DATE"].notnull()].copy()

        df_nomember_cancellation_order["DATE"] = pd.to_datetime(df_nomember_cancellation_order["DATE"])
        df_nomember_cancellation_confirm["DATE"] = pd.to_datetime(df_nomember_cancellation_confirm["DATE"])
        df_nomember_cancellation_delivery["DATE"] = pd.to_datetime(df_nomember_cancellation_delivery["DATE"])
        df_nomember_cancellation_cancel["DATE"] = pd.to_datetime(df_nomember_cancellation_cancel["DATE"])

        df_nomember_cancellation_order["STATUS"] = "1.주문일자"
        df_nomember_cancellation_confirm["STATUS"] = "2.입금확인일자"
        df_nomember_cancellation_delivery["STATUS"] = "3.배송완료일자"
        df_nomember_cancellation_cancel["STATUS"] = "4.주문취소일자"

        df_nomember_cancellation_seq = pd.concat(
            [df_nomember_cancellation_order, df_nomember_cancellation_confirm, df_nomember_cancellation_delivery,
             df_nomember_cancellation_cancel],
            axis=0).sort_values(by=["DATE", "STATUS"], ascending=["True", "True"], ignore_index=True)
        del df_nomember_cancellation, df_nomember_cancellation_order, df_nomember_cancellation_confirm, df_nomember_cancellation_delivery, df_nomember_cancellation_cancel
        df_nomember_export_raw = df_nomember.copy()
        df_nomember = df_nomember[(df_nomember["CONFIRMSTATUS"] == "구매 완료") & (df_nomember["CANCELSTATUS"] == "구매 확정")].reset_index(drop=True)

        for col in ["CONFIRMSTATUS", "CANCELSTATUS"]:
            v = ""
            if col == "CONFIRMSTATUS":
                v += "구매 완료"
            elif col == "CANCELSTATUS":
                v += "구매 확정"
            else:
                raise ValueError("No value defined..")
            if df_nomember[col].nunique() != 1 or df_nomember[col].unique().tolist()[0] != v:
                raise ValueError(
                    "Need to check data for {} column since ISFINISH==T and ISCONFIRM==T and ISDELIVERYFINISH==T and ISCANCEL==F..".format(
                        col))
            del col, v

        print("Creating 매출 dataframe..")
        df_nomember_price_daily = df_nomember.groupby(["REGDATE", "FLATFORM"])["SETTLEPRICE"].sum().reset_index(name="settlePrice").rename(
            columns={"REGDATE": "Date"})
        df_nomember_price_daily.insert(0, "div", "일별")
        df_nomember_price_monthly = df_nomember.groupby(["YYYYMM", "FLATFORM"])["SETTLEPRICE"].sum().reset_index(name="settlePrice").rename(
            columns={"YYYYMM": "Date"})
        df_nomember_price_monthly.insert(0, "div", "월별")

        df_nomember_price_g = pd.concat([df_nomember_price_daily, df_nomember_price_monthly], axis=0, ignore_index=True)
        del df_nomember_price_daily, df_nomember_price_monthly

        df_nomember_payment_confirm = df_nomember.copy()
        df_nomember_payment_confirm["paymentConfirm"] = 1

        df_nomember_payment_confirm_daily = df_nomember_payment_confirm.groupby(["REGDATE", "FLATFORM"]).agg({"paymentConfirm": "sum"}).reset_index().rename(columns={"REGDATE": "Date"})
        df_nomember_payment_confirm_daily.insert(0, "div", "일별")
        df_nomember_payment_confirm_monthly = df_nomember_payment_confirm.groupby(["YYYYMM", "FLATFORM"]).agg({"paymentConfirm": "sum"}).reset_index().rename(columns={"YYYYMM": "Date"})
        df_nomember_payment_confirm_monthly.insert(0, "div", "월별")

        df_nomember_payment_confirm_g = pd.concat([df_nomember_payment_confirm_daily, df_nomember_payment_confirm_monthly],
                                                axis=0, ignore_index=True)
        del df_nomember_payment_confirm, df_nomember_payment_confirm_daily, df_nomember_payment_confirm_monthly

        print("Creating 구매자수 dataframe..")
        df_nomember_paid_user_daily = df_nomember.groupby(["REGDATE", "FLATFORM"])["USERID"].nunique().reset_index(name="paidUser").rename(columns={"REGDATE": "Date"})
        df_nomember_paid_user_daily.insert(0, "div", "일별")
        df_nomember_paid_user_monthly = df_nomember.groupby(["YYYYMM", "FLATFORM"])["USERID"].nunique().reset_index(name="paidUser").rename(columns={"YYYYMM": "Date"})
        df_nomember_paid_user_monthly.insert(0, "div", "월별")

        df_nomember_paid_user_g = pd.concat([df_nomember_paid_user_daily, df_nomember_paid_user_monthly], axis=0,
                                            ignore_index=True)
        del df_nomember_paid_user_daily, df_nomember_paid_user_monthly

        print("Merging all the dataframes above..")
        df_nomember_g = reduce(lambda left, right: pd.merge(left, right, on=["div", "Date", "FLATFORM"], how="outer"),
                             [df_nomember_cancellation_g, df_nomember_payment_confirm_g, df_nomember_price_g, df_nomember_paid_user_g])
        del df_nomember_cancellation_g, df_nomember_payment_confirm_g, df_nomember_price_g

        df_nomember_g.insert(df_nomember_g.columns.tolist().index("Date") + 1, "weekday", np.nan)
        df_nomember_g_dau = df_nomember_g[df_nomember_g["div"] == "일별"].copy()
        df_nomember_g_dau["weekday"] = df_nomember_g_dau["Date"].apply(lambda x: datetime(year=int(x.split("-")[0]), month=int(x.split("-")[1]),
                                                                                          day=int(x.split("-")[2])).strftime("%A"))
        df_nomember_g_mau = df_nomember_g[df_nomember_g["div"] == "월별"].copy()
        df_nomember_g = pd.concat([df_nomember_g_dau, df_nomember_g_mau], axis=0, ignore_index=True)
        del df_nomember_g_dau, df_nomember_g_mau
        df_nomember_g.insert(df_nomember_g.columns.tolist().index("weekday") + 1, "weekdayDiv",
                           df_nomember_g["weekday"].copy())
        df_nomember_g["weekdayDiv"] = df_nomember_g["weekdayDiv"].map(
            {"Monday": "주중", "Tuesday": "주중", "Wednesday": "주중", "Thursday": "주중", "Friday": "주중",
             "Saturday": "주말", "Sunday": "주말"})
        df_nomember_g["weekday"] = df_nomember_g["weekday"].map(
            {"Monday": "월요일", "Tuesday": "화요일", "Wednesday": "수요일", "Thursday": "목요일", "Friday": "금요일",
             "Saturday": "토요일", "Sunday": "일요일"})
        for col in ["paymentConfirm", "settlePrice"]:
            df_nomember_g[col] = df_nomember_g[col].fillna(0).astype(int)
            del col

        df_nomember_export_raw["FLATFORM"] = "비회원_비회원_" + df_nomember_export_raw["FLATFORM"]
        df_nomember_g["FLATFORM"] = "비회원_비회원_" + df_nomember_g["FLATFORM"]
        df_nomember_cancellation_list_g["FLATFORM"] = "비회원_비회원_" + df_nomember_cancellation_list_g["FLATFORM"]

        df_nomember_export_raw.rename(columns={"FLATFORM": "LEVELIDX_GUBUN_T_MEMBER_FLATFORM"}, inplace=True)
        df_nomember_g.rename(columns={"FLATFORM": "LEVELIDX_GUBUN_T_MEMBER_FLATFORM"}, inplace=True)
        df_nomember_cancellation_list_g.rename(columns={"FLATFORM": "LEVELIDX_GUBUN_T_MEMBER_FLATFORM"}, inplace=True)

        f_nm_order_seq = "{}df_nomember_order_sequence_{}.pickle".format(path_export, date)
        print("Exporting {}".format(f_nm_order_seq))
        with gzip.open(f_nm_order_seq, "wb") as f:
            pickle.dump(df_nomember_cancellation_seq, f)
            del f
        del df_nomember_cancellation_seq, f_nm_order_seq

        f_nm_cancellation_list = "{}df_nomember_cancellation_list_{}.pickle".format(path_export, date)
        print("Exporting {}".format(f_nm_cancellation_list))
        with gzip.open(f_nm_cancellation_list, "wb") as f:
            pickle.dump(df_nomember_cancellation_list_g, f)
            del f
        del df_nomember_cancellation_list_g, f_nm_cancellation_list

        f_nm_paid_user = "{}df_nomember_paid_user_{}.pickle".format(path_export, date)
        print("Exporting {}".format(f_nm_paid_user))
        with gzip.open(f_nm_paid_user, "wb") as f:
            pickle.dump(df_nomember_g, f)
            del f
        del df_nomember_g, f_nm_paid_user

        f_nm_analysis_db = "{}analysis_db/nomember/df_paid_user_nomember_analysis_db_{}.pickle".format(path_export, date)
        print("Exporting {}\n".format(f_nm_analysis_db))
        with gzip.open(f_nm_analysis_db, "wb") as f:
            pickle.dump(df_nomember_export_raw, f)
            del f
        del df_nomember_export_raw, f_nm_analysis_db

        del df_nomember

    else:
        print("There is no {}..".format(file_nm_nomember))


def homelearnfriendsmall_monthly_paid_user_hiclassmember(date=None, dbname=None):
    from glob import glob
    import gzip
    import pickle
    import pandas as pd
    import numpy as np
    from datetime import datetime
    from functools import reduce
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory
    # path_export = file_directory

    df_t_recent_t_goods_linked = recent_t_goods_linked(dbname=dbname)

    file_nm_hiclassmember = "{}raw/hiclassmember/df_t_order_hiclassmember_{}.pickle".format(path_import, date)
    if file_nm_hiclassmember in glob("{}raw/hiclassmember/*".format(path_import)):
        df_hiclassmember = import_pickle(file_nm_hiclassmember)
        del file_nm_hiclassmember

        for col in ["GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "STATE_T_MEMBER"]:
            if df_hiclassmember[df_hiclassmember[col].isnull()].shape[0] != 0:
                raise ValueError("There are null values in {} column in df_hiclassmember (dbname={})".format(col, dbname))
            del col
        df_hiclassmember.drop(["STATE_T_MEMBER"], axis=1, inplace=True)
        for col in ["MEMBERLEVELNAME", "LEVELIDX_GUBUN_T_MEMBER"]:
            if df_hiclassmember[df_hiclassmember[col].isnull()].shape[0] != 0:
                raise ValueError("Need to check {} columns since there are null values..")
            del col
        df_hiclassmember.insert(df_hiclassmember.columns.tolist().index("LEVELIDX_GUBUN_T_MEMBER"), "LEVELIDX_GUBUN_T_MEMBER_FLATFORM",
                         df_hiclassmember["LEVELIDX_GUBUN_T_MEMBER"] + "_" + df_hiclassmember["FLATFORM"])
        df_hiclassmember.drop(["USERKIND", "USERKIND_CAT", "SESSID", "USERID", "MEMBERLEVELNAME", "LEVELIDX_GUBUN_T_MEMBER"], axis=1, inplace=True)

        df_hiclassmember["CONFIRMSTATUS"] = df_hiclassmember["ISFINISH"] + df_hiclassmember["ISCONFIRM"]
        df_hiclassmember["CONFIRMSTATUS"] = df_hiclassmember["CONFIRMSTATUS"].map({"TT": "구매 완료", "FF": "미구매", "TF": "주문 완료 및 미입금"})
        if df_hiclassmember[df_hiclassmember["CONFIRMSTATUS"].isnull()].shape[0] != 0:
            raise ValueError("There are null values in CONFIRMSTATUS column in {}..".format(dbname))

        df_hiclassmember["CANCELSTATUS"] = df_hiclassmember["ISDELIVERYFINISH"] + df_hiclassmember["ISCANCEL"]
        df_hiclassmember["CANCELSTATUS"] = df_hiclassmember["CANCELSTATUS"].map({"TT": "배송 후 주문 취소",
                                                                   "TF": "구매 확정",
                                                                   "FT": "배송 전 주문 취소",
                                                                   "FF": "배송 중"})
        if df_hiclassmember[df_hiclassmember["CANCELSTATUS"].isnull()].shape[0] != 0:
            raise ValueError("There are null values in CANCELSTATUS column in {}..".format(dbname))
        if df_hiclassmember[df_hiclassmember["CONFIRMSTATUS"] == "미구매"]["CANCELSTATUS"].nunique(dropna=False) == 1 and df_hiclassmember[df_hiclassmember["CONFIRMSTATUS"] == "미구매"]["CANCELSTATUS"].unique()[0] == "배송 중":
            df_hiclassmember.loc[df_hiclassmember[df_hiclassmember["CONFIRMSTATUS"] == "미구매"].index.tolist(), "CANCELSTATUS"] = "미배송"
        else:
            raise ValueError("Need to check CONFIRMSTATUS == 미구매 for df_t_order in {}".format(dbname))

        col_g_daily = ["REGDATE", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "DIV_ADDR", "SIDO", "SIGUNGU", "CLAIMTYPE", "REASON",
                       "REASONDETAIL", "CONFIRMSTATUS", "CANCELSTATUS", "GOODSNAME", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER"]
        col_g_monthly = ["YYYYMM", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "DIV_ADDR", "SIDO", "SIGUNGU", "CLAIMTYPE",
                         "REASON", "REASONDETAIL", "CONFIRMSTATUS", "CANCELSTATUS", "GOODSNAME", "GENDER_T_MEMBER",
                         "AGE_BIN_T_MEMBER"]
        col_agg = {"payment": "sum", "cancelPayment": "sum"}

        df_hiclassmember_region = df_hiclassmember.groupby(col_g_daily)["USERNO"].count().reset_index().rename(
            columns={"USERNO": "COUNT"})
        if df_hiclassmember_region["COUNT"].sum() != df_hiclassmember.shape[0]:
            raise ValueError("Need to check data for USERNO count..")

        print("Creating 결제후 취소 건수 dataframe..")
        lst_filter = ["구매 완료", "주문 완료 및 미입금"]
        df_hiclassmember_cancellation = df_hiclassmember[df_hiclassmember["CONFIRMSTATUS"].str.contains("|".join(lst_filter))].reset_index(drop=True)
        df_hiclassmember_cancellation["payment"] = 1
        df_hiclassmember_cancellation["cancelPayment"] = np.nan
        df_hiclassmember_cancellation.loc[df_hiclassmember_cancellation[df_hiclassmember_cancellation["ISCANCEL"] == "T"].index, "cancelPayment"] = 1
        df_hiclassmember_cancellation["cancelPayment"] = df_hiclassmember_cancellation["cancelPayment"].fillna(0).astype(int)

        df_hiclassmember_cancellation_total_daily = df_hiclassmember_cancellation.groupby(["REGDATE", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"]).agg(col_agg).reset_index().rename(columns={"REGDATE": "Date"})
        df_hiclassmember_cancellation_total_daily.insert(0, "div", "일별")
        df_hiclassmember_cancellation_total_monthly = df_hiclassmember_cancellation.groupby(["YYYYMM", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"]).agg(col_agg).reset_index().rename(columns={"YYYYMM": "Date"})
        df_hiclassmember_cancellation_total_monthly.insert(0, "div", "월별")
        df_hiclassmember_cancellation_g = pd.concat([df_hiclassmember_cancellation_total_daily, df_hiclassmember_cancellation_total_monthly], axis=0, ignore_index=True)
        del df_hiclassmember_cancellation_total_daily, df_hiclassmember_cancellation_total_monthly

        print("Creating 결제후 취소 건수 리스트 dataframe..")
        df_hiclassmember_cancellation_daily = df_hiclassmember_cancellation.groupby(col_g_daily).agg(col_agg).reset_index().rename(
            columns={"REGDATE": "Date", "payment": "paymentCount", "cancelPayment": "cancelPaymentCount"})
        df_hiclassmember_cancellation_daily.insert(0, "div", "일별")

        df_hiclassmember_cancellation_monthly = df_hiclassmember_cancellation.groupby(col_g_monthly).agg(col_agg).reset_index().rename(
            columns={"YYYYMM": "Date", "payment": "paymentCount", "cancelPayment": "cancelPaymentCount"})
        df_hiclassmember_cancellation_monthly.insert(0, "div", "월별")

        df_hiclassmember_cancellation_list_g = pd.concat([df_hiclassmember_cancellation_daily, df_hiclassmember_cancellation_monthly], axis=0, ignore_index=True)
        del df_hiclassmember_cancellation_daily, df_hiclassmember_cancellation_monthly

        for col in ["payment", "cancelPayment"]:
            if df_hiclassmember_cancellation_list_g["{}Count".format(col)].sum() != df_hiclassmember_cancellation_g[col].sum():
                raise ValueError("{} does not match for df_t_order_cancellation_list_g[{}].sum() and df_hiclassmember_cancellation_g[{}].sum()".format(
                    col, col, col))
            del col

        print("Creating total sequence..")
        df_hiclassmember_cancellation_order = df_hiclassmember_cancellation[
            ["DIV_ADDR", "SIDO", "SIGUNGU", "USERNO", "FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER", "GOODSNAME", "CANCELSTATUS", "regdate", "CLAIMTYPE", "REASON", "REASONDETAIL"]].copy().rename(columns={"regdate": "DATE"})
        df_hiclassmember_cancellation_confirm = df_hiclassmember_cancellation[
            ["DIV_ADDR", "SIDO", "SIGUNGU", "USERNO", "FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER", "GOODSNAME", "CANCELSTATUS", "CONFIRMDATE", "CLAIMTYPE", "REASON", "REASONDETAIL"]].copy().rename(columns={"CONFIRMDATE": "DATE"})
        df_hiclassmember_cancellation_delivery = df_hiclassmember_cancellation[
            ["DIV_ADDR", "SIDO", "SIGUNGU", "USERNO", "FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER", "GOODSNAME", "CANCELSTATUS", "DELIVERYFINISHDATE", "CLAIMTYPE", "REASON", "REASONDETAIL"]].copy().rename(columns={"DELIVERYFINISHDATE": "DATE"})
        df_hiclassmember_cancellation_cancel = df_hiclassmember_cancellation[
            ["DIV_ADDR", "SIDO", "SIGUNGU", "USERNO", "FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER", "GOODSNAME", "CANCELSTATUS", "CANCELDATE", "CLAIMTYPE", "REASON", "REASONDETAIL"]].copy().rename(columns={"CANCELDATE": "DATE"})

        df_hiclassmember_cancellation_order = df_hiclassmember_cancellation_order[df_hiclassmember_cancellation_order["DATE"].notnull()].copy()
        df_hiclassmember_cancellation_confirm = df_hiclassmember_cancellation_confirm[df_hiclassmember_cancellation_confirm["DATE"].notnull()].copy()
        df_hiclassmember_cancellation_delivery = df_hiclassmember_cancellation_delivery[df_hiclassmember_cancellation_delivery["DATE"].notnull()].copy()
        df_hiclassmember_cancellation_cancel = df_hiclassmember_cancellation_cancel[df_hiclassmember_cancellation_cancel["DATE"].notnull()].copy()

        df_hiclassmember_cancellation_order["DATE"] = pd.to_datetime(df_hiclassmember_cancellation_order["DATE"])
        df_hiclassmember_cancellation_confirm["DATE"] = pd.to_datetime(df_hiclassmember_cancellation_confirm["DATE"])
        df_hiclassmember_cancellation_delivery["DATE"] = pd.to_datetime(df_hiclassmember_cancellation_delivery["DATE"])
        df_hiclassmember_cancellation_cancel["DATE"] = pd.to_datetime(df_hiclassmember_cancellation_cancel["DATE"])

        df_hiclassmember_cancellation_order["STATUS"] = "1.주문일자"
        df_hiclassmember_cancellation_confirm["STATUS"] = "2.입금확인일자"
        df_hiclassmember_cancellation_delivery["STATUS"] = "3.배송완료일자"
        df_hiclassmember_cancellation_cancel["STATUS"] = "4.주문취소일자"

        df_hiclassmember_cancellation_seq = pd.concat([df_hiclassmember_cancellation_order, df_hiclassmember_cancellation_confirm, df_hiclassmember_cancellation_delivery, df_hiclassmember_cancellation_cancel],
                                               axis=0).sort_values(by=["DATE", "STATUS"], ascending=["True", "True"], ignore_index=True)
        del df_hiclassmember_cancellation, df_hiclassmember_cancellation_order, df_hiclassmember_cancellation_confirm, df_hiclassmember_cancellation_delivery, df_hiclassmember_cancellation_cancel

        df_hiclassmember_export_raw = df_hiclassmember.copy()
        df_hiclassmember = df_hiclassmember[(df_hiclassmember["CONFIRMSTATUS"] == "구매 완료") & (df_hiclassmember["CANCELSTATUS"] == "구매 확정")].reset_index(drop=True)

        for col in ["CONFIRMSTATUS", "CANCELSTATUS"]:
            v = ""
            if col == "CONFIRMSTATUS":
                v += "구매 완료"
            elif col == "CANCELSTATUS":
                v += "구매 확정"
            else:
                raise ValueError("No value defined..")
            if df_hiclassmember[col].nunique() != 1 or df_hiclassmember[col].unique().tolist()[0] != v:
                raise ValueError("Need to check data for {} column since ISFINISH==T and ISCONFIRM==T and ISDELIVERYFINISH==T and ISCANCEL==F..".format(col))
            del col, v

        print("Creating 매출 dataframe..")
        df_hiclassmember_price_daily = df_hiclassmember.groupby(["REGDATE", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"])["SETTLEPRICE"].sum().reset_index(name="settlePrice").rename(columns={"REGDATE": "Date"})
        df_hiclassmember_price_daily.insert(0, "div", "일별")
        df_hiclassmember_price_monthly = df_hiclassmember.groupby(["YYYYMM", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"])["SETTLEPRICE"].sum().reset_index(name="settlePrice").rename(columns={"YYYYMM": "Date"})
        df_hiclassmember_price_monthly.insert(0, "div", "월별")

        df_hiclassmember_price_g = pd.concat([df_hiclassmember_price_daily, df_hiclassmember_price_monthly], axis=0, ignore_index=True)
        del df_hiclassmember_price_daily, df_hiclassmember_price_monthly

        df_hiclassmember_payment_confirm = df_hiclassmember.copy()
        df_hiclassmember_payment_confirm["paymentConfirm"] = 1

        df_hiclassmember_payment_confirm_daily = df_hiclassmember_payment_confirm.groupby(["REGDATE", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"]).agg({"paymentConfirm": "sum"}).reset_index().rename(columns={"REGDATE": "Date"})
        df_hiclassmember_payment_confirm_daily.insert(0, "div", "일별")
        df_hiclassmember_payment_confirm_monthly = df_hiclassmember_payment_confirm.groupby(["YYYYMM", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"]).agg({"paymentConfirm": "sum"}).reset_index().rename(columns={"YYYYMM": "Date"})
        df_hiclassmember_payment_confirm_monthly.insert(0, "div", "월별")

        df_hiclassmember_payment_confirm_g = pd.concat([df_hiclassmember_payment_confirm_daily, df_hiclassmember_payment_confirm_monthly], axis=0, ignore_index=True)
        del df_hiclassmember_payment_confirm, df_hiclassmember_payment_confirm_daily, df_hiclassmember_payment_confirm_monthly

        print("Creating 구매자수 dataframe..")
        df_hiclassmember_paid_user_daily = df_hiclassmember.groupby(["REGDATE", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"])["USERNO"].nunique().reset_index(name="paidUser").rename(columns={"REGDATE": "Date"})
        df_hiclassmember_paid_user_daily.insert(0, "div", "일별")
        df_hiclassmember_paid_user_monthly = df_hiclassmember.groupby(["YYYYMM", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"])["USERNO"].nunique().reset_index(name="paidUser").rename(columns={"YYYYMM": "Date"})
        df_hiclassmember_paid_user_monthly.insert(0, "div", "월별")

        df_hiclassmember_paid_user_g = pd.concat([df_hiclassmember_paid_user_daily, df_hiclassmember_paid_user_monthly], axis=0, ignore_index=True)
        del df_hiclassmember_paid_user_daily, df_hiclassmember_paid_user_monthly

        print("Creating 재구매율 dataframe..")
        df_hiclassmember_repurchase_daily = df_hiclassmember.groupby(["USERNO", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"])["REGDATE"].apply(set).reset_index().rename(columns={"REGDATE": "REGDATES"})
        df_hiclassmember_repurchase_daily["REGDATES"] = df_hiclassmember_repurchase_daily["REGDATES"].apply(lambda x: sorted(list(x)))
        df_hiclassmember_repurchase_daily["purchaseDays"] = df_hiclassmember_repurchase_daily["REGDATES"].apply(lambda x: len(x))
        df_hiclassmember_repurchase_daily["repurchase"] = np.nan
        df_hiclassmember_repurchase_daily.loc[df_hiclassmember_repurchase_daily[df_hiclassmember_repurchase_daily["purchaseDays"] == 1].index, "repurchase"] = "N"
        df_hiclassmember_repurchase_daily.loc[df_hiclassmember_repurchase_daily[df_hiclassmember_repurchase_daily["purchaseDays"] > 1].index, "repurchase"] = "Y"
        df_hiclassmember_repurchase_daily["firstREGDATE"] = df_hiclassmember_repurchase_daily["REGDATES"].apply(lambda x: x[0])
        df_hiclassmember_repurchase_daily = df_hiclassmember_repurchase_daily.groupby(["firstREGDATE", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "repurchase", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"])[
            "USERNO"].nunique().reset_index(name="numberOfUsers").rename(columns={"repurchase": "재구매 여부", "firstREGDATE": "Date"})
        df_hiclassmember_repurchase_daily.insert(0, "div", "일별")

        df_hiclassmember_repurchase_daily_repurchase_y = df_hiclassmember_repurchase_daily[df_hiclassmember_repurchase_daily["재구매 여부"] == "Y"].copy().reset_index(drop=True).rename(
            columns={"numberOfUsers": "numberOfUsers_repurchaseY"}).drop(["재구매 여부"], axis=1)
        df_hiclassmember_repurchase_daily_repurchase_n = df_hiclassmember_repurchase_daily[df_hiclassmember_repurchase_daily["재구매 여부"] == "N"].copy().reset_index(drop=True).rename(
            columns={"numberOfUsers": "numberOfUsers_repurchaseN"}).drop(["재구매 여부"], axis=1)
        del df_hiclassmember_repurchase_daily
        df_hiclassmember_repurchase_daily = pd.merge(df_hiclassmember_repurchase_daily_repurchase_y, df_hiclassmember_repurchase_daily_repurchase_n, on=["div", "Date", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"], how="outer")
        del df_hiclassmember_repurchase_daily_repurchase_y, df_hiclassmember_repurchase_daily_repurchase_n

        df_hiclassmember_repurchase_daily.sort_values(["div", "Date"], ignore_index=True, inplace=True)
        df_hiclassmember_repurchase_daily["numberOfUsers_repurchaseY"].fillna(0, inplace=True)
        df_hiclassmember_repurchase_daily["numberOfUsers_repurchaseN"].fillna(0, inplace=True)
        df_hiclassmember_repurchase_daily[["numberOfUsers_repurchaseY", "numberOfUsers_repurchaseN"]] = df_hiclassmember_repurchase_daily[["numberOfUsers_repurchaseY", "numberOfUsers_repurchaseN"]].astype(int)
        df_hiclassmember_repurchase_daily.rename(columns={"numberOfUsers_repurchaseY": "재구매_Y", "numberOfUsers_repurchaseN": "재구매_N"}, inplace=True)

        print("Creating 첫 구매 후 두 번째 구매까지 걸리는 시간 dataframe..")
        df_hiclassmember_repurchase_first_to_second_purchase_daily = df_hiclassmember.groupby(["USERNO", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"])["regdate"].apply(set).reset_index().rename(columns={"regdate": "regdates"})
        df_hiclassmember_repurchase_first_to_second_purchase_daily["regdates"] = df_hiclassmember_repurchase_first_to_second_purchase_daily["regdates"].apply(lambda x: sorted(list(x)))
        df_hiclassmember_repurchase_first_to_second_purchase_daily["regdatesLst"] = df_hiclassmember_repurchase_first_to_second_purchase_daily["regdates"].apply(lambda x: [dt.split(" ")[0] for dt in x])
        df_hiclassmember_repurchase_first_to_second_purchase_daily["regdatesLstUnique"] = df_hiclassmember_repurchase_first_to_second_purchase_daily["regdatesLst"].apply(set)
        df_hiclassmember_repurchase_first_to_second_purchase_daily["regdatesLstUnique"] = df_hiclassmember_repurchase_first_to_second_purchase_daily["regdatesLstUnique"].apply(lambda x: sorted(list(x)))
        df_hiclassmember_repurchase_first_to_second_purchase_daily["purchaseDays"] = df_hiclassmember_repurchase_first_to_second_purchase_daily["regdatesLstUnique"].apply(len)

        purchase_date_greater_than_one = df_hiclassmember_repurchase_first_to_second_purchase_daily[df_hiclassmember_repurchase_first_to_second_purchase_daily["purchaseDays"] > 1].reset_index(drop=True)

        purchase_date_greater_than_one["firstRegdate"] = purchase_date_greater_than_one["regdatesLstUnique"].apply(lambda x: x[0])
        purchase_date_greater_than_one["secondRegdate"] = purchase_date_greater_than_one["regdatesLstUnique"].apply(lambda x: x[1])

        purchase_date_greater_than_one["firstRegdatesTime"] = np.nan
        purchase_date_greater_than_one["secondRegdatesTime"] = np.nan
        for ind in purchase_date_greater_than_one.index:
            first_regdate_index = purchase_date_greater_than_one.loc[ind, "firstRegdate"]
            second_regdate_index = purchase_date_greater_than_one.loc[ind, "secondRegdate"]

            first_regdate_index = purchase_date_greater_than_one.loc[ind, "regdatesLst"].index(first_regdate_index)
            second_regdate_index = purchase_date_greater_than_one.loc[ind, "regdatesLst"].index(second_regdate_index)

            purchase_date_greater_than_one.loc[ind, "firstRegdatesTime"] = purchase_date_greater_than_one.loc[ind, "regdates"][first_regdate_index]
            purchase_date_greater_than_one.loc[ind, "secondRegdatesTime"] = purchase_date_greater_than_one.loc[ind, "regdates"][second_regdate_index]
            del ind, first_regdate_index, second_regdate_index

        for col in ["firstRegdatesTime", "secondRegdatesTime"]:
            purchase_date_greater_than_one[col] = pd.to_datetime(purchase_date_greater_than_one[col])
            del col
        purchase_date_greater_than_one.drop(["regdates", "regdatesLst", "regdatesLstUnique", "purchaseDays", "firstRegdate", "secondRegdate"], axis=1, inplace=True)

        purchase_date_equal_to_one = df_hiclassmember_repurchase_first_to_second_purchase_daily[df_hiclassmember_repurchase_first_to_second_purchase_daily["purchaseDays"] == 1].reset_index(drop=True)
        purchase_date_equal_to_one["firstRegdatesTime"] = purchase_date_equal_to_one["regdates"].apply(lambda x: x[0])
        purchase_date_equal_to_one["secondRegdatesTime"] = np.nan
        purchase_date_equal_to_one.drop(["regdates", "regdatesLst", "regdatesLstUnique", "purchaseDays"], axis=1, inplace=True)

        if df_hiclassmember_repurchase_first_to_second_purchase_daily.shape[0] != purchase_date_greater_than_one.shape[0] + purchase_date_equal_to_one.shape[0]:
            raise ValueError("1. Need to check data..")
        else:
            df_hiclassmember_repurchase_first_to_second_purchase_daily = pd.concat([purchase_date_greater_than_one, purchase_date_equal_to_one], axis=0, ignore_index=True)
            del purchase_date_greater_than_one, purchase_date_equal_to_one

        print("Merging all the dataframes above..")
        df_hiclassmember_g = reduce(lambda left, right: pd.merge(left, right, on=["div", "Date", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"], how="outer"),
                            [df_hiclassmember_cancellation_g, df_hiclassmember_payment_confirm_g, df_hiclassmember_price_g, df_hiclassmember_paid_user_g, df_hiclassmember_repurchase_daily])
        del df_hiclassmember_cancellation_g, df_hiclassmember_payment_confirm_g, df_hiclassmember_price_g, df_hiclassmember_paid_user_g, df_hiclassmember_repurchase_daily

        df_hiclassmember_g.insert(df_hiclassmember_g.columns.tolist().index("Date") + 1, "weekday", np.nan)
        df_hiclassmember_g_dau = df_hiclassmember_g[df_hiclassmember_g["div"] == "일별"].copy()
        df_hiclassmember_g_dau["weekday"] = df_hiclassmember_g_dau["Date"].apply(lambda x: datetime(year=int(x.split("-")[0]), month=int(x.split("-")[1]), day=int(x.split("-")[2])).strftime("%A"))
        df_hiclassmember_g_mau = df_hiclassmember_g[df_hiclassmember_g["div"] == "월별"].copy()
        df_hiclassmember_g = pd.concat([df_hiclassmember_g_dau, df_hiclassmember_g_mau], axis=0, ignore_index=True)
        del df_hiclassmember_g_dau, df_hiclassmember_g_mau
        df_hiclassmember_g.insert(df_hiclassmember_g.columns.tolist().index("weekday") + 1, "weekdayDiv", df_hiclassmember_g["weekday"].copy())
        df_hiclassmember_g["weekdayDiv"] = df_hiclassmember_g["weekdayDiv"].map(
            {"Monday": "주중", "Tuesday": "주중", "Wednesday": "주중", "Thursday": "주중", "Friday": "주중",
             "Saturday": "주말", "Sunday": "주말"})
        df_hiclassmember_g["weekday"] = df_hiclassmember_g["weekday"].map(
            {"Monday": "월요일", "Tuesday": "화요일", "Wednesday": "수요일", "Thursday": "목요일", "Friday": "금요일",
             "Saturday": "토요일", "Sunday": "일요일"})
        for col in ["paymentConfirm", "settlePrice", "paidUser", "재구매_Y", "재구매_N"]:
            df_hiclassmember_g[col] = df_hiclassmember_g[col].fillna(0).astype(int)
            del col

        print("Creating 회원가입일별 구매 확정수 dataframe..")
        df_hiclassmember_by_join_date = df_hiclassmember.copy()
        df_hiclassmember_by_join_date.insert(df_hiclassmember_by_join_date.columns.tolist().index("JOINDATE")+1, "DIV_JOINDATE", np.nan)
        ind_join_date = [x.strftime("%Y-%m-%d") for x in pd.date_range(start="2020-03-06", end=datetime.today().strftime("%Y-%m-%d"), freq="D")]
        ind_join_date = "|".join(ind_join_date)
        ind_join_date = df_hiclassmember_by_join_date[df_hiclassmember_by_join_date["JOINDATE"].str.contains("{}".format(ind_join_date))].index.tolist()
        df_hiclassmember_by_join_date.loc[ind_join_date, "DIV_JOINDATE"] = "2020-03-06 이후"
        df_hiclassmember_by_join_date["DIV_JOINDATE"].fillna("2020-03-05 이전", inplace=True)
        del ind_join_date

        df_hiclassmember_by_join_date["JOINDATE"] = df_hiclassmember_by_join_date["JOINDATE"].apply(lambda x: x[:-3])
        df_hiclassmember_by_join_date = df_hiclassmember_by_join_date.groupby(["YYYYMM", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "JOINDATE", "DIV_JOINDATE"]).agg({"SETTLEPRICE": "sum", "USERNO": pd.Series.nunique}).reset_index().rename(
            columns={"YYYYMM": "paymentConfirmDate", "USERNO": "UNIQUEUSERNO"})

        f_nm_order_seq = "{}df_hiclassmember_order_sequence_{}.pickle".format(path_export, date)
        print("Exporting {}".format(f_nm_order_seq))
        with gzip.open(f_nm_order_seq, "wb") as f:
            pickle.dump(df_hiclassmember_cancellation_seq, f)
            del f
        del df_hiclassmember_cancellation_seq, f_nm_order_seq

        f_nm_cancellation_list = "{}df_hiclassmember_cancellation_list_{}.pickle".format(path_export, date)
        print("Exporting {}".format(f_nm_cancellation_list))
        with gzip.open(f_nm_cancellation_list, "wb") as f:
            pickle.dump(df_hiclassmember_cancellation_list_g, f)
            del f
        del df_hiclassmember_cancellation_list_g, f_nm_cancellation_list

        f_nm_order_region = "{}df_hiclassmember_order_region_{}.pickle".format(path_export, date)
        print("Exporting {}".format(f_nm_order_region))
        with gzip.open(f_nm_order_region, "wb") as f:
            pickle.dump(df_hiclassmember_region, f)
            del f
        del df_hiclassmember_region, f_nm_order_region

        f_nm_paid_user = "{}df_hiclassmember_paid_user_{}.pickle".format(path_export, date)
        print("Exporting {}".format(f_nm_paid_user))
        with gzip.open(f_nm_paid_user, "wb") as f:
            pickle.dump(df_hiclassmember_g, f)
            del f
        del df_hiclassmember_g, f_nm_paid_user

        f_nm_first_to_second_purchase = "{}df_hiclassmember_first_to_second_purchase_{}.pickle".format(path_export, date)
        print("Exporting {}".format(f_nm_first_to_second_purchase))
        with gzip.open(f_nm_first_to_second_purchase, "wb") as f:
            pickle.dump(df_hiclassmember_repurchase_first_to_second_purchase_daily, f)
            del f
        del df_hiclassmember_repurchase_first_to_second_purchase_daily, f_nm_first_to_second_purchase

        f_nm_by_join_date = "{}df_hiclassmember_by_join_date_{}.pickle".format(path_export, date)
        print("Exporting {}".format(f_nm_by_join_date))
        with gzip.open(f_nm_by_join_date, "wb") as f:
            pickle.dump(df_hiclassmember_by_join_date, f)
            del f
        del df_hiclassmember_by_join_date, f_nm_by_join_date

        f_nm_analysis_db = "{}analysis_db/hiclassmember/df_paid_user_member_analysis_db_{}.pickle".format(path_export, date)
        print("Exporting {}\n".format(f_nm_analysis_db))
        with gzip.open(f_nm_analysis_db, "wb") as f:
            pickle.dump(df_hiclassmember_export_raw, f)
            del f
        del df_hiclassmember_export_raw, f_nm_analysis_db

        del df_hiclassmember

    else:
        print("There is no {}..".format(file_nm_hiclassmember))


def conestore_total_active_user(table_name=None, dbname=None, year=None):
    from glob import glob
    import gzip
    import pickle
    import pandas as pd
    from sys import platform
    from datetime import datetime

    df_register_date = conestore_total_t_user(dbname=dbname)

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory

    print("Reading and appending every df_t_user_log for year {}..".format(year))
    df_t_user_log = pd.DataFrame()
    for file in sorted(glob(path_import)):
        if table_name in file and str(year) in file:
            with gzip.open(file, "rb") as f:
                temp = pickle.load(f)
                del f
            temp = pd.merge(temp, df_register_date, on=["USERNO"], how="left")
            temp = temp[temp["USERID"].notnull()].reset_index(drop=True)
            temp["USERID"] = temp["USERID"].str.lower()
            temp = temp[~temp["USERID"].str.contains("test|테스트")].reset_index(drop=True)
            temp = temp[(temp["USERKIND"] == "회원") | (temp["USERKIND"] == "비회원")].reset_index(drop=True)
            temp.drop(["IDX", "IP", "REGDATE", "USERID", "JOINDATE"], axis=1, inplace=True)
            temp.drop_duplicates(keep="first", inplace=True)
            df_t_user_log = df_t_user_log.append(temp, ignore_index=True, sort=False)
            del temp
            df_t_user_log.drop_duplicates(keep="first", inplace=True)
        del file
    del df_register_date

    df_t_user_log = df_t_user_log.groupby(["USERKIND"])["USERNO"].nunique().reset_index().rename(columns={"USERNO": "ACTIVEUSER"})
    df_t_user_log.insert(0, "DIV", "전체")

    return df_t_user_log


def homelearnfriendsmall_total_active_user(table_name=None, dbname=None, year=None):
    from glob import glob
    import gzip
    import pickle
    import pandas as pd
    from sys import platform
    from datetime import datetime

    df_register_date = homelearnfriendsmall_total_t_user(dbname=dbname)

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    path_import = file_directory

    print("Reading and appending every df_t_user_log for year {}..".format(year))
    df_t_user_log = pd.DataFrame()
    for file in sorted(glob(path_import)):
        if table_name in file and str(year) in file:
            with gzip.open(file, "rb") as f:
                temp = pickle.load(f)
                del f
            temp = pd.merge(temp, df_register_date, on=["USERNO"], how="left")
            temp = temp[temp["USERID"].notnull()].reset_index(drop=True)
            temp["USERID"] = temp["USERID"].str.lower()
            temp = temp[~temp["USERID"].str.contains("test|테스트")].reset_index(drop=True)
            temp = temp[(temp["USERKIND"] == "회원") | (temp["USERKIND"] == "비회원")].reset_index(drop=True)
            temp.drop(["IDX", "IP", "REGDATE", "USERID", "JOINDATE"], axis=1, inplace=True)
            temp.drop_duplicates(keep="first", inplace=True)
            df_t_user_log = df_t_user_log.append(temp, ignore_index=True, sort=False)
            del temp
            df_t_user_log.drop_duplicates(keep="first", inplace=True)
        del file
    del df_register_date

    df_t_user_log_userkindcat = df_t_user_log.groupby(["USERKIND", "USERKIND_CAT"])["USERNO"].nunique().reset_index()
    lst_ind_not_hiclassmember = df_t_user_log_userkindcat[df_t_user_log_userkindcat["USERKIND_CAT"] != "하이클래스"].index.tolist()
    df_t_user_log_userkindcat.loc[lst_ind_not_hiclassmember, "USERKIND_CAT"] = "일반회원"
    df_t_user_log_userkind_cat = pd.DataFrame({"USERKIND": ["회원"]})
    if df_t_user_log_userkindcat["USERKIND_CAT"].count() != 2:
        raise ValueError("일반회원 and 하이클래스 are not the only values..")
    for cat in df_t_user_log_userkindcat["USERKIND_CAT"].unique():
        col_name = "USERNO"
        if cat == "하이클래스":
            col_name += "_hiclassMember"
        if cat == "일반회원":
            col_name += "_notHiclassMember"
        t = df_t_user_log_userkindcat[df_t_user_log_userkindcat["USERKIND_CAT"] == cat].copy().rename(
            columns={"USERNO": col_name}).drop(["USERKIND_CAT"], axis=1)
        df_t_user_log_userkind_cat = pd.merge(df_t_user_log_userkind_cat, t, on=["USERKIND"], how="outer")
        del t, cat, col_name
    del df_t_user_log_userkindcat

    df_t_user_log = df_t_user_log.groupby(["USERKIND"])["USERNO"].nunique().reset_index().rename(columns={"USERNO": "ACTIVEUSER"})
    df_t_user_log.insert(0, "DIV", "전체")
    df_t_user_log = pd.merge(df_t_user_log, df_t_user_log_userkind_cat, on=["USERKIND"], how="outer")
    if df_t_user_log.shape[0] != 1:
        raise ValueError("The rows should be one..")

    return df_t_user_log


def conestore_total_paid_user_with_total_active_user(table_name_paid_user=None, table_name_active_user=None, dbname=None, year=None):
    from glob import glob
    import gzip
    import pickle
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta
    from functools import reduce
    from sys import platform
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import_paid_user = file_directory
    # path_import_active_user = file_directory
    # path_import_register = file_directory

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    df_t_user = pd.DataFrame()
    for file in sorted(glob(path_import_register)):
        with gzip.open(file, "rb") as f:
            temp = pickle.load(f)
            temp = temp[["NO", "USERKIND", "USERID"]].rename(columns={"NO": "USERNO"})
            temp["USERID"] = temp["USERID"].str.lower()
            df_t_user = df_t_user.append(temp, ignore_index=True, sort=False)
            del f, temp
        del file

    print("START MODULE : total_paid_user_with_total_active_user ########################")
    print("Reading and appending every df_t_order for {}.. ".format(year))
    df_t_order_total = pd.DataFrame()
    for file in sorted(glob(path_import_paid_user)):
        if str(year) in file and table_name_paid_user in file and "t_order_delivery" not in file and "t_order_option" not in file and "df_t_order_goods" not in file and "df_t_order_goods_delivery_req" not in file and "t_order_claim" not in file and "df_t_order_info" not in file and "t_cart_order" not in file:
            with gzip.open(file, "rb") as f:
                temp = pickle.load(f)
                temp = temp[['IDX', 'FLATFORM', 'ORDNO', 'SESSID', 'USERNO', 'USERID', 'MEMBERLEVELNAME', 'SETTLEPRICE',
                             'ISFINISH', 'ISCONFIRM', 'CONFIRMDATE', "ISDELIVERYFINISH", "ISCANCEL", 'CANCELDATE',
                             'REGDATE', 'TOTALORDERPRICE']]
                # 테이블명세서-홈런프렌즈쇼핑몰 > FLATFORM column 100: PC,  200: 모바일
                temp["SESSID"] = temp["SESSID"].str.strip().str.lower()
                temp["USERID"] = temp["USERID"].str.strip().str.lower()
                temp = temp[~temp["SESSID"].str.contains("test|테스트")].reset_index(drop=True)
                temp = temp[~temp["USERID"].str.contains("test|테스트")].reset_index(drop=True)

                rows = temp.shape[0]
                temp = pd.merge(temp, df_t_user, on=["USERNO", "USERID"], how="left")
                if rows != temp.shape[0]:
                    raise ValueError("USERNO in df_t_user is not unique (file={})..".format(file))
                if temp[temp["USERKIND"].isnull()].shape[0] != 0:
                    raise ValueError("There are null values in USERKIND column (file={})..".format(file))

                df_t_order_total = df_t_order_total.append(temp, ignore_index=True, sort=False)
                del f, temp
        del file
    del df_t_user

    df_t_order_total["regdate"] = df_t_order_total["REGDATE"].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
    df_t_order_total["REGDATE"] = df_t_order_total["REGDATE"].apply(lambda x: x.strftime("%Y-%m-%d"))

    print("Total) Creating 결제후 취소 건수 dataframe..")
    df_t_order_total_cancellation = df_t_order_total[(df_t_order_total["ISFINISH"] == "T") & (df_t_order_total["ISCONFIRM"] == "T")].reset_index(drop=True)
    df_t_order_total_cancellation["payment"] = 1
    df_t_order_total_cancellation["cancelPayment"] = np.nan
    df_t_order_total_cancellation.loc[df_t_order_total_cancellation[df_t_order_total_cancellation["ISCANCEL"] == "T"].index, "cancelPayment"] = 1
    df_t_order_total_cancellation["cancelPayment"] = df_t_order_total_cancellation["cancelPayment"].fillna(0).astype(int)
    df_t_order_total_cancellation = df_t_order_total_cancellation.groupby(["USERKIND"]).agg({"payment": "sum", "cancelPayment": "sum"}).reset_index()

    df_t_order_total_cancellation["cancellationRatio"] = df_t_order_total_cancellation["cancelPayment"] / df_t_order_total_cancellation["payment"]
    df_t_order_total = df_t_order_total[(df_t_order_total["ISFINISH"] == "T") & (df_t_order_total["ISCONFIRM"] == "T") &
                            (df_t_order_total["ISDELIVERYFINISH"] == "T") & (df_t_order_total["ISCANCEL"] == "F")].reset_index(drop=True)

    print("Total) Creating 매출 dataframe..")
    df_t_order_total_price = df_t_order_total.groupby(["USERKIND"])["TOTALORDERPRICE"].sum().reset_index(name="totalOrderPrice")

    print("Total) Creating 구매자수 dataframe..")
    df_t_order_total_paid_user = df_t_order_total.groupby(["USERKIND"])["USERNO"].nunique().reset_index(name="paidUser")

    print("Total) Creating 재구매율 dataframe..")
    df_t_order_total_repurchase_daily = df_t_order_total.groupby(["USERNO", "USERKIND"])["REGDATE"].apply(set).reset_index().rename(columns={"REGDATE": "REGDATES"})
    df_t_order_total_repurchase_daily["REGDATES"] = df_t_order_total_repurchase_daily["REGDATES"].apply(lambda x: sorted(list(x)))
    df_t_order_total_repurchase_daily["purchaseDays"] = df_t_order_total_repurchase_daily["REGDATES"].apply(lambda x: len(x))
    df_t_order_total_repurchase_daily["repurchase"] = np.nan
    df_t_order_total_repurchase_daily.loc[df_t_order_total_repurchase_daily[df_t_order_total_repurchase_daily["purchaseDays"] == 1].index, "repurchase"] = "N"
    df_t_order_total_repurchase_daily.loc[df_t_order_total_repurchase_daily[df_t_order_total_repurchase_daily["purchaseDays"] > 1].index, "repurchase"] = "Y"
    df_t_order_total_repurchase_daily["firstREGDATE"] = df_t_order_total_repurchase_daily["REGDATES"].apply(lambda x: x[0])
    df_t_order_total_repurchase_daily = df_t_order_total_repurchase_daily.groupby(["USERKIND", "repurchase"])[
        "USERNO"].nunique().reset_index(name="numberOfUsers").rename(columns={"repurchase": "재구매 여부"})

    df_t_order_total_repurchase_daily_repurchase_y = df_t_order_total_repurchase_daily[df_t_order_total_repurchase_daily["재구매 여부"] == "Y"].copy().reset_index(drop=True).rename(
        columns={"numberOfUsers": "numberOfUsers_repurchaseY"}).drop(["재구매 여부"], axis=1)
    df_t_order_total_repurchase_daily_repurchase_n = df_t_order_total_repurchase_daily[df_t_order_total_repurchase_daily["재구매 여부"] == "N"].copy().reset_index(drop=True).rename(
        columns={"numberOfUsers": "numberOfUsers_repurchaseN"}).drop(["재구매 여부"], axis=1)
    del df_t_order_total_repurchase_daily
    df_t_order_total_repurchase_daily = pd.merge(df_t_order_total_repurchase_daily_repurchase_y, df_t_order_total_repurchase_daily_repurchase_n, on=["USERKIND"], how="outer")
    del df_t_order_total_repurchase_daily_repurchase_y, df_t_order_total_repurchase_daily_repurchase_n

    df_t_order_total_repurchase_daily["numberOfUsers_repurchaseY"].fillna(0, inplace=True)
    df_t_order_total_repurchase_daily["numberOfUsers_repurchaseN"].fillna(0, inplace=True)
    df_t_order_total_repurchase_daily[["numberOfUsers_repurchaseY", "numberOfUsers_repurchaseN"]] = df_t_order_total_repurchase_daily[["numberOfUsers_repurchaseY", "numberOfUsers_repurchaseN"]].astype(int)
    df_t_order_total_repurchase_daily.rename(columns={"numberOfUsers_repurchaseY": "재구매_Y", "numberOfUsers_repurchaseN": "재구매_N"}, inplace=True)
    df_t_order_total_repurchase_daily["재구매_총"] = df_t_order_total_repurchase_daily["재구매_Y"] + df_t_order_total_repurchase_daily["재구매_N"]
    df_t_order_total_repurchase_daily["재구매율"] = df_t_order_total_repurchase_daily["재구매_Y"] / df_t_order_total_repurchase_daily["재구매_총"]

    print("Total) Merging all the dataframes above..")
    df_t_order_total_g = reduce(lambda left, right: pd.merge(left, right, on=["USERKIND"], how="outer"),
                        [df_t_order_total_cancellation, df_t_order_total_price, df_t_order_total_paid_user, df_t_order_total_repurchase_daily])
    del df_t_order_total_cancellation, df_t_order_total_price, df_t_order_total_paid_user, df_t_order_total_repurchase_daily
    del df_t_order_total
    df_t_order_total_g.insert(0, "YEAR", year)

    df_t_user_log_total = conestore_total_active_user(table_name=table_name_active_user, dbname=dbname, year=year)
    df_t_user_log_total.insert(1, "YEAR", year)

    df = pd.merge(df_t_user_log_total, df_t_order_total_g, on=["YEAR", "USERKIND"], how="outer")
    del df_t_user_log_total, df_t_order_total_g
    print("END MODULE : total_paid_user_with_total_active_user ########################")

    return df


def conestore_total_order_sequence(table_name=None, dbname=None):
    from glob import glob
    import gzip
    import pickle
    import pandas as pd
    from datetime import timedelta
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory

    print("Creating every order sequence..")
    df_t_order_seq = pd.DataFrame()
    for file in sorted(glob(path_import)):
        if table_name in file:
            with gzip.open(file, "rb") as f:
                temp = pickle.load(f)
                col_group = ["SIDO", "SIGUNGU", "USERNO", "ORDNO", "CANCELSTATUS", "CLAIMTYPE", "REASON", "REASONDETAIL"]
                temp_list_date = temp.groupby(col_group)["DATE"].apply(list).reset_index()
                temp_list_status = temp.groupby(col_group)["STATUS"].apply(list).reset_index()
                temp = pd.merge(temp_list_status, temp_list_date, on=col_group, how="outer")
                del temp_list_date, temp_list_status, col_group
                df_t_order_seq = df_t_order_seq.append(temp, ignore_index=True, sort=False)
                del f, temp
        del file
    df_t_order_seq.insert(0, "ORDERDATE", df_t_order_seq["DATE"].apply(lambda x: x[0].strftime("%Y-%m-%d")))

    result = pd.DataFrame()

    for val in df_t_order_seq["CANCELSTATUS"].unique():
        temp = df_t_order_seq[df_t_order_seq["CANCELSTATUS"] == val].copy().reset_index(drop=True)
        temp["TOTALSEC"] = temp["DATE"].apply(lambda x: x[-1] - x[0])
        temp["TOTALSEC"] = temp["TOTALSEC"].dt.total_seconds()
        temp["STATUS"] = temp["STATUS"].apply(lambda x: "{} > {}".format(x[0], x[-1]))
        temp.drop(["DATE"], axis=1, inplace=True)
        result = result.append(temp, sort=False, ignore_index=True)
        del temp
    del df_t_order_seq
    result.rename(columns={"ORDERDATE": "DATE"}, inplace=True)

    return result


def homelearnfriendsmall_total_order_sequence(table_name=None, dbname=None):
    from glob import glob
    import gzip
    import pickle
    import pandas as pd
    from datetime import timedelta
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory

    print("Creating every order sequence..")
    df_member_t_order_seq = pd.DataFrame()
    df_nomember_t_order_seq = pd.DataFrame()
    df_hiclassmember_t_order_seq = pd.DataFrame()

    file_nm_member = table_name.split("_")[0] + "_member_" + "_".join(table_name.split("_")[1:])
    file_nm_nomember = table_name.split("_")[0] + "_nomember_" + "_".join(table_name.split("_")[1:])
    file_nm_hiclassmember = table_name.split("_")[0] + "_hiclassmember_" + "_".join(table_name.split("_")[1:])

    col_group = {file_nm_member: ['DIV_ADDR', 'SIDO', 'SIGUNGU', 'USERNO', 'FLATFORM', 'GENDER_T_MEMBER', 'AGE_BIN_T_MEMBER',
                                  'AGE_BIN_ENG_T_MEMBER', 'GOODSNAME', 'CANCELSTATUS',
                                  'CLAIMTYPE', 'REASON', 'REASONDETAIL'],
                 file_nm_nomember: ['USERID', 'FLATFORM', 'DIV_ADDR', 'SIDO', 'SIGUNGU', 'GOODSNAME',
                                    'CANCELSTATUS', 'CLAIMTYPE', 'REASON', 'REASONDETAIL'],
                 file_nm_hiclassmember: ['DIV_ADDR', 'SIDO', 'SIGUNGU', 'USERNO', 'FLATFORM', 'GENDER_T_MEMBER', 'AGE_BIN_T_MEMBER',
                                         'AGE_BIN_ENG_T_MEMBER', 'GOODSNAME', 'CANCELSTATUS',
                                         'CLAIMTYPE', 'REASON', 'REASONDETAIL']}

    for k in col_group:
        for file in sorted(glob(path_import)):
            if k in file:
                with gzip.open(file, "rb") as f:
                    temp = pickle.load(f)
                    temp_list_date = temp.groupby(col_group[k])["DATE"].apply(list).reset_index()
                    temp_list_status = temp.groupby(col_group[k])["STATUS"].apply(list).reset_index()
                    temp = pd.merge(temp_list_status, temp_list_date, on=col_group[k], how="outer")
                    temp.insert(0, "DIV", k)
                    del temp_list_date, temp_list_status
                    if k == file_nm_member:
                        df_member_t_order_seq = df_member_t_order_seq.append(temp, ignore_index=True, sort=False)
                    if k == file_nm_nomember:
                        df_nomember_t_order_seq = df_nomember_t_order_seq.append(temp, ignore_index=True, sort=False)
                    if k == file_nm_hiclassmember:
                        df_hiclassmember_t_order_seq = df_hiclassmember_t_order_seq.append(temp, ignore_index=True, sort=False)
                    del f, temp
            del file
        del k
    df_member_t_order_seq.insert(0, "ORDERDATE", df_member_t_order_seq["DATE"].apply(lambda x: x[0].strftime("%Y-%m-%d")))
    df_member_t_order_seq["USERNO"] = df_member_t_order_seq["USERNO"].astype(str)
    df_nomember_t_order_seq.insert(0, "ORDERDATE", df_nomember_t_order_seq["DATE"].apply(lambda x: x[0].strftime("%Y-%m-%d")))
    df_nomember_t_order_seq.rename(columns={"USERID": "USERNO"}, inplace=True)
    df_hiclassmember_t_order_seq.insert(0, "ORDERDATE", df_hiclassmember_t_order_seq["DATE"].apply(lambda x: x[0].strftime("%Y-%m-%d")))
    df_hiclassmember_t_order_seq["USERNO"] = df_hiclassmember_t_order_seq["USERNO"].astype(str)

    result_member = pd.DataFrame()
    result_nomember = pd.DataFrame()
    result_hiclassmember = pd.DataFrame()

    for df in [df_member_t_order_seq, df_nomember_t_order_seq, df_hiclassmember_t_order_seq]:
        for val in df["CANCELSTATUS"].unique():
            temp = df[df["CANCELSTATUS"] == val].copy().reset_index(drop=True)
            temp["TOTALSEC"] = temp["DATE"].apply(lambda x: x[-1] - x[0])
            temp["TOTALSEC"] = temp["TOTALSEC"].dt.total_seconds()
            temp["STATUS"] = temp["STATUS"].apply(lambda x: "{} > {}".format(x[0], x[-1]))
            temp.drop(["DATE"], axis=1, inplace=True)
            if df["DIV"].unique()[0] == file_nm_member:
                result_member = result_member.append(temp, sort=False, ignore_index=True)
            if df["DIV"].unique()[0] == file_nm_nomember:
                result_nomember = result_nomember.append(temp, sort=False, ignore_index=True)
            if df["DIV"].unique()[0] == file_nm_hiclassmember:
                result_hiclassmember = result_hiclassmember.append(temp, sort=False, ignore_index=True)
            del temp
        del df
    del df_member_t_order_seq, df_nomember_t_order_seq, df_hiclassmember_t_order_seq

    result = pd.concat([result_member, result_nomember, result_hiclassmember], axis=0, ignore_index=True)
    del result_member, result_nomember, result_hiclassmember
    result.rename(columns={"ORDERDATE": "DATE"}, inplace=True)

    return result


def conestore_user_info(dbname=None, dfname=None):
    from glob import glob
    import pandas as pd
    import gzip
    import pickle
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory
    # path_export = file_directory

    print("Reading and appending every df_{} in {} folder..".format(dfname, dfname))
    df = pd.DataFrame()
    for file in sorted(glob(path_import)):
        if "{}_".format(dfname) in file:
            with gzip.open(file, "rb") as f:
                df = df.append(pickle.load(f), ignore_index=True, sort=False)
                del f
            del file

    if dfname == "paid_user":
        df = pd.concat([df[df["div"] == "일별"].copy(), df[df["div"] == "월별"].copy()], axis=0, ignore_index=True)
    if dfname == "active_user":
        df = pd.concat([df[df["div"] == "일별"].copy(), df[df["div"] == "월별"].copy()], axis=0, ignore_index=True)

    print("Exporting {}{}_{}.xlsx\n".format(path_export, dfname, dbname))
    df.to_excel("{}{}_{}.xlsx".format(path_export, dfname, dbname), index=False)
    del df


def homelearnfriendsmall_user_info_active_user(dbname=None, dfname=None):
    if dfname != "active_user":
        raise ValueError("dfname should be 'active_user'..")
    from glob import glob
    import pandas as pd
    import numpy as np
    import gzip
    import pickle
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory
    # path_export = file_directory

    print("Reading and appending every df_{} in {} folder..".format(dfname, dfname))
    df = pd.DataFrame()
    for file in sorted(glob(path_import)):
        if "{}_".format(dfname) in file:
            with gzip.open(file, "rb") as f:
                df = df.append(pickle.load(f), ignore_index=True, sort=False)
                del f
            del file
    col_df = ["activeUser_socialmember_unavailable", "activeUser_socialmember_10under", "activeUser_hiclass_unavailable", "activeUser_hiclass_10under"]
    for i in range(10, 110, 10):
        col_df.append("activeUser_socialmember_{}s".format(i))
        col_df.append("activeUser_hiclass_{}s".format(i))
        del i
    for col in sorted(list(set(col_df).difference(df.columns.tolist()))):
        df[col] = np.nan
        del col
    if len([x for x in df.columns.tolist() if "activeUser_socialmember" in x]) != 12:
        raise ValueError("Need to check columns to activeUser_socialmember..")
    if len([x for x in df.columns.tolist() if "activeUser_hiclass" in x]) != 12:
        raise ValueError("Need to check columns to activeUser_hiclass..")

    df = df[['div', 'Date', 'activeUser_total',
             'activeUser_socialmember_unavailable', 'activeUser_socialmember_10under', 'activeUser_socialmember_10s',
             'activeUser_socialmember_20s', 'activeUser_socialmember_30s', 'activeUser_socialmember_40s',
             'activeUser_socialmember_50s', 'activeUser_socialmember_60s', 'activeUser_socialmember_70s',
             'activeUser_socialmember_80s', 'activeUser_socialmember_90s', 'activeUser_socialmember_100s',
             'activeUser_hiclass_unavailable', 'activeUser_hiclass_10under', 'activeUser_hiclass_10s',
             'activeUser_hiclass_20s', 'activeUser_hiclass_30s', 'activeUser_hiclass_40s',
             'activeUser_hiclass_50s', 'activeUser_hiclass_60s', 'activeUser_hiclass_70s',
             'activeUser_hiclass_80s', 'activeUser_hiclass_90s', 'activeUser_hiclass_100s']]
    df = pd.concat([df[df["div"] == "일별"].copy(), df[df["div"] == "월별"].copy()], axis=0, ignore_index=True)

    print("Exporting {}{}_{}.xlsx\n".format(path_export, dfname, dbname))
    df.to_excel("{}{}_{}.xlsx".format(path_export, dfname, dbname), index=False)
    del df


def homelearnfriendsmall_user_info_paid_user(dbname=None, dfname=None):
    if dfname != "paid_user":
        raise ValueError("dfname should be 'paid_user'..")
    from glob import glob
    import pandas as pd
    import gzip
    import pickle
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_import = file_directory
    # path_export = file_directory

    map_df = {"df_member": "member", "df_nomember": "nomember", "hiclassmember": "hiclassmember"}

    print("Reading and appending every df_{} in {} folder..".format(dfname, dfname))
    df_total = pd.DataFrame()
    df_div = pd.DataFrame()
    for file in sorted(glob(path_import)):
        if "{}_".format(dfname) in file and "total" not in file:
            with gzip.open(file, "rb") as f:
                temp = pickle.load(f)
                del f
            for k in map_df.keys():
                if k in file:
                    temp.insert(0, "DIV_MEMBER", map_df[k])
                del k
            df_div = df_div.append(temp, ignore_index = True, sort = False)
            del temp
        if "{}_".format(dfname) in file and "total" in file:
            with gzip.open(file, "rb") as f:
                df_total = df_total.append(pickle.load(f))
        del file

    df_total = pd.concat([df_total[df_total["div"] == "일별"].copy().sort_values(by=["Date"]),
                          df_total[df_total["div"] == "월별"].copy().sort_values(by=["Date"])], axis=0, ignore_index=True)

    df_div = pd.concat([df_div[df_div["div"] == "일별"].copy().sort_values(by=["DIV_MEMBER", "Date"]),
                        df_div[df_div["div"] == "월별"].copy().sort_values(by=["DIV_MEMBER", "Date"])], axis=0, ignore_index=True)

    file_nm_df_total = "{}{}_total_{}.xlsx".format(path_export, dfname, dbname)
    print("Exporting {}".format(file_nm_df_total))
    df_total.to_excel(file_nm_df_total, index=False)

    file_nm_df_div = "{}{}_div_{}.xlsx".format(path_export, dfname, dbname)
    print("Exporting {}".format(file_nm_df_div))
    df_div.to_excel(file_nm_df_div, index=False)
    del df_total, df_div


def conestore_tableau_upload_df_user(dbname=None):
    from glob import glob
    from sys import platform
    import pandas as pd
    import numpy as np
    import gzip
    import pickle
    from datetime import timedelta, datetime
    from functools import reduce
    from calendar import monthrange
    from dateutil.relativedelta import relativedelta

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # folder_active_user = file_directory
    # folder_paid_user = file_directory
    #
    # file_active_user = file_directory
    # file_paid_user = file_directory
    #
    # file_export_df_by_join_date = file_directory
    # file_export_df_main = file_directory
    # file_export_df_main_total = file_directory
    # file_export_df_main_total_temp = file_directory
    # file_export_df_cart_info = file_directory
    # file_export_df_first_to_second_purchase = file_directory
    # file_export_df_retention_rate = file_directory
    # file_export_df_time_to_purchase = file_directory
    # file_export_df_weekday_engagement_rate = file_directory
    # file_export_df_cancellation_list = file_directory
    # file_export_df_order_region = file_directory
    # file_export_df_total_order_sequence = file_directory

    df_register_date = conestore_total_t_user(dbname=dbname)
    df_register_date["JOINDATE"] = df_register_date["JOINDATE"].apply(lambda x: str(x).split(" ")[0])

    print("Reading and appending every df_retention_rate..")
    df_retention_rate = pd.DataFrame()
    for file in sorted(glob(file_directory)):
        if "df_retention_rate" in file:
            with gzip.open(file, "rb") as f:
                temp = pickle.load(f)
                temp = temp[(temp["USERKIND"] == "회원") | (temp["USERKIND"] == "비회원")].reset_index(drop=True)
                df_retention_rate = df_retention_rate.append(temp, ignore_index=True, sort=False)
                del f, temp
        del file

    df_goods = recent_t_goods(dbname=dbname)

    print("Reading and appending every df_cart..")
    df_cart = pd.DataFrame()
    for file in sorted(glob(file_directory)):
        with gzip.open(file, "rb") as f:
            df_cart = df_cart.append(pickle.load(f), ignore_index=True, sort=False)
            del f
        del file
    df_cart = df_cart[(df_cart["USERKIND"] == "회원") | (df_cart["USERKIND"] == "비회원")].reset_index(drop=True)
    df_cart["REGDATE_DAILY"] = df_cart["REGDATE"].apply(lambda x: x.strftime("%Y-%m-%d"))
    df_cart["REGDATE_MONTHLY"] = df_cart["REGDATE"].apply(lambda x: x.strftime("%Y-%m"))
    rows = df_cart.shape[0]
    df_cart = pd.merge(df_cart, df_goods, on=["GOODSNO"], how="left")
    if rows != df_cart.shape[0]:
        raise ValueError("values in GOODSNAME column in df_goods are not unique..")
    del df_goods

    df_cart["NULLPRICE"] = np.nan
    ind_null_price = df_cart[df_cart["PRICE"].isnull()].index.tolist()
    ind_notnull_price = df_cart[df_cart["PRICE"].notnull()].index.tolist()

    df_cart.loc[ind_null_price, "NULLPRICE"] = "Y"
    df_cart.loc[ind_notnull_price, "NULLPRICE"] = "N"
    del ind_null_price, ind_notnull_price

    if df_cart[df_cart["GOODSNAME"].isnull()].shape[0] != 0:
        raise ValueError("There are some null values after merging df_cart and df_goods..")

    df_cart_daily = df_cart.groupby(["REGDATE_MONTHLY", "REGDATE_DAILY", "GOODSNAME", "ISDIRECT", "USERKIND"]).agg(
        {"USERNO": pd.Series.nunique, "EA": "sum"}).reset_index().rename(
        columns={"REGDATE_MONTHLY": "REGDATE_DIV", "REGDATE_DAILY": "REGDATE", "USERNO": "UNIQUEUSERNO"})
    df_cart_daily.insert(0, "DIV", "일별")

    df_cart_monthly = df_cart.groupby(["REGDATE_MONTHLY", "GOODSNAME", "ISDIRECT", "USERKIND"]).agg(
        {"USERNO": pd.Series.nunique, "EA": "sum"}).reset_index().rename(
        columns={"REGDATE_MONTHLY": "REGDATE", "USERNO": "UNIQUEUSERNO"})
    df_cart_monthly["REGDATE_DIV"] = df_cart_monthly["REGDATE"].copy()
    df_cart_monthly.insert(0, "DIV", "월별")

    df_cart_total = df_cart.groupby(["GOODSNAME", "ISDIRECT", "USERKIND"]).agg(
        {"USERNO": pd.Series.nunique, "EA": "sum"}).reset_index().rename(
        columns={"USERNO": "UNIQUEUSERNO"})
    df_cart_total.insert(0, "DIV", "전체")

    df_cart_info = pd.concat([df_cart_daily, df_cart_monthly, df_cart_total], axis=0, ignore_index=True)
    del df_cart_daily, df_cart_monthly, df_cart_total

    df_cart_car_daily = df_cart.groupby(["REGDATE_DAILY", "ISDIRECT", "USERKIND"])["GOODSNAME"].count().reset_index().rename(
        columns={"GOODSNAME": "CARTGOODSNAMECOUNT"})
    df_cart_car_daily_TOTAL = df_cart_car_daily.groupby(["REGDATE_DAILY", "USERKIND"])["CARTGOODSNAMECOUNT"].sum().reset_index()
    df_cart_car_daily_T = df_cart_car_daily[df_cart_car_daily["ISDIRECT"] == "T"].index.tolist()
    df_cart_car_daily_T = df_cart_car_daily.loc[df_cart_car_daily_T, ["REGDATE_DAILY", "USERKIND", "CARTGOODSNAMECOUNT"]].sort_values(
        by=["REGDATE_DAILY"]).reset_index(drop=True).rename(columns={"CARTGOODSNAMECOUNT": "CARTGOODSNAMECOUNT_T"})
    df_cart_car_daily_F = df_cart_car_daily[df_cart_car_daily["ISDIRECT"] == "F"].index.tolist()
    df_cart_car_daily_F = df_cart_car_daily.loc[df_cart_car_daily_F, ["REGDATE_DAILY", "USERKIND", "CARTGOODSNAMECOUNT"]].sort_values(
        by=["REGDATE_DAILY"]).reset_index(drop=True).rename(columns={"CARTGOODSNAMECOUNT": "CARTGOODSNAMECOUNT_F"})
    if df_cart_car_daily_F.shape[0] == 0:
        print("There are no ISDIRECT == F.. Making df_cart_car_daily_F['CARTGOODSNAMECOUNT_F'] = 0")
        df_cart_car_daily_F = df_cart_car_daily_TOTAL.copy().rename(columns={"CARTGOODSNAMECOUNT": "CARTGOODSNAMECOUNT_F"})
        df_cart_car_daily_F["CARTGOODSNAMECOUNT_F"] = 0

    df_cart_car_daily = reduce(lambda left, right: pd.merge(left, right, on=["REGDATE_DAILY", "USERKIND"], how="left"),
                               [df_cart_car_daily_TOTAL, df_cart_car_daily_T, df_cart_car_daily_F])
    del df_cart_car_daily_TOTAL, df_cart_car_daily_T, df_cart_car_daily_F
    for col in ["CARTGOODSNAMECOUNT", "CARTGOODSNAMECOUNT_T", "CARTGOODSNAMECOUNT_F"]:
        df_cart_car_daily[col] = df_cart_car_daily[col].fillna(0).astype(int)
        del col
    df_cart_car_daily["CARTPURCHASERATIO"] = df_cart_car_daily["CARTGOODSNAMECOUNT_T"] / df_cart_car_daily["CARTGOODSNAMECOUNT"]
    df_cart_car_daily["CAR"] = 1 - df_cart_car_daily["CARTGOODSNAMECOUNT_T"] / df_cart_car_daily["CARTGOODSNAMECOUNT"]
    df_cart_car_daily.insert(0, "DIV", "일별")
    df_cart_car_daily.rename(columns={"REGDATE_DAILY": "REGDATE"}, inplace=True)

    df_cart_car_monthly = df_cart.groupby(["REGDATE_MONTHLY", "ISDIRECT", "USERKIND"])["GOODSNAME"].count().reset_index().rename(
        columns={"GOODSNAME": "CARTGOODSNAMECOUNT"})
    df_cart_car_monthly_TOTAL = df_cart_car_monthly.groupby(["REGDATE_MONTHLY", "USERKIND"])["CARTGOODSNAMECOUNT"].sum().reset_index()
    df_cart_car_monthly_T = df_cart_car_monthly[df_cart_car_monthly["ISDIRECT"] == "T"].index.tolist()
    df_cart_car_monthly_T = df_cart_car_monthly.loc[df_cart_car_monthly_T, ["REGDATE_MONTHLY", "USERKIND", "CARTGOODSNAMECOUNT"]].sort_values(
        by=["REGDATE_MONTHLY"]).reset_index(drop=True).rename(columns={"CARTGOODSNAMECOUNT": "CARTGOODSNAMECOUNT_T"})
    df_cart_car_monthly_F = df_cart_car_monthly[df_cart_car_monthly["ISDIRECT"] == "F"].index.tolist()
    df_cart_car_monthly_F = df_cart_car_monthly.loc[df_cart_car_monthly_F, ["REGDATE_MONTHLY", "USERKIND", "CARTGOODSNAMECOUNT"]].sort_values(
        by=["REGDATE_MONTHLY"]).reset_index(drop=True).rename(columns={"CARTGOODSNAMECOUNT": "CARTGOODSNAMECOUNT_F"})
    if df_cart_car_monthly_F.shape[0] == 0:
        print("There are no ISDIRECT == F.. Making df_cart_car_monthly_F['CARTGOODSNAMECOUNT_F'] = 0")
        df_cart_car_monthly_F = df_cart_car_monthly_TOTAL.copy().rename(columns={"CARTGOODSNAMECOUNT": "CARTGOODSNAMECOUNT_F"})
        df_cart_car_monthly_F["CARTGOODSNAMECOUNT_F"] = 0

    df_cart_car_monthly = reduce(lambda left, right: pd.merge(left, right, on=["REGDATE_MONTHLY", "USERKIND"], how="left"),
                               [df_cart_car_monthly_TOTAL, df_cart_car_monthly_T, df_cart_car_monthly_F])
    del df_cart_car_monthly_TOTAL, df_cart_car_monthly_T, df_cart_car_monthly_F
    for col in ["CARTGOODSNAMECOUNT", "CARTGOODSNAMECOUNT_T", "CARTGOODSNAMECOUNT_F"]:
        df_cart_car_monthly[col] = df_cart_car_monthly[col].fillna(0).astype(int)
        del col
    df_cart_car_monthly["CARTPURCHASERATIO"] = df_cart_car_monthly["CARTGOODSNAMECOUNT_T"] / df_cart_car_monthly["CARTGOODSNAMECOUNT"]
    df_cart_car_monthly["CAR"] = 1 - df_cart_car_monthly["CARTGOODSNAMECOUNT_T"] / df_cart_car_monthly["CARTGOODSNAMECOUNT"]
    df_cart_car_monthly.insert(0, "DIV", "월별")
    df_cart_car_monthly.rename(columns={"REGDATE_MONTHLY": "REGDATE"}, inplace=True)

    df_cart_car = pd.concat([df_cart_car_daily, df_cart_car_monthly], axis=0, ignore_index=True)
    del df_cart_car_daily, df_cart_car_monthly
    df_cart_user_daily = df_cart.groupby(["REGDATE_DAILY", "ISDIRECT", "USERKIND"])["USERNO"].nunique().reset_index().rename(
        columns={"REGDATE_DAILY": "REGDATE", "USERNO": "CARTUSERS"})
    df_cart_user_daily_T = df_cart_user_daily[df_cart_user_daily["ISDIRECT"] == "T"].copy().rename(
        columns={"CARTUSERS": "CARTUSERS_T"}).drop(["ISDIRECT"], axis=1)
    df_cart_user_daily_F = df_cart_user_daily[df_cart_user_daily["ISDIRECT"] == "F"].copy().rename(
        columns={"CARTUSERS": "CARTUSERS_F"}).drop(["ISDIRECT"], axis=1)
    df_cart_user_daily = pd.merge(df_cart_user_daily_T, df_cart_user_daily_F, on=["REGDATE", "USERKIND"], how="outer")
    df_cart_user_daily["CARTUSERS_F"] = df_cart_user_daily["CARTUSERS_F"].fillna(0).astype(int)
    del df_cart_user_daily_T, df_cart_user_daily_F
    df_cart_user_daily.insert(0, "DIV", "일별")

    df_cart_user_monthly = df_cart.groupby(["REGDATE_MONTHLY", "ISDIRECT", "USERKIND"])["USERNO"].nunique().reset_index().rename(
        columns={"REGDATE_MONTHLY": "REGDATE", "USERNO": "CARTUSERS"})
    df_cart_user_monthly_T = df_cart_user_monthly[df_cart_user_monthly["ISDIRECT"] == "T"].copy().rename(
        columns={"CARTUSERS": "CARTUSERS_T"}).drop(["ISDIRECT"], axis=1)
    df_cart_user_monthly_F = df_cart_user_monthly[df_cart_user_monthly["ISDIRECT"] == "F"].copy().rename(
        columns={"CARTUSERS": "CARTUSERS_F"}).drop(["ISDIRECT"], axis=1)
    df_cart_user_monthly = pd.merge(df_cart_user_monthly_T, df_cart_user_monthly_F, on=["REGDATE", "USERKIND"], how="outer")
    df_cart_user_monthly["CARTUSERS_F"] = df_cart_user_monthly["CARTUSERS_F"].fillna(0).astype(int)
    del df_cart_user_monthly_T, df_cart_user_monthly_F
    df_cart_user_monthly.insert(0, "DIV", "월별")

    df_cart_user = pd.concat([df_cart_user_daily, df_cart_user_monthly], axis=0, ignore_index=True)
    del df_cart_user_daily, df_cart_user_monthly

    df_cart_car = pd.merge(df_cart_user, df_cart_car, on=["DIV", "REGDATE", "USERKIND"], how="outer")
    del df_cart, df_cart_user

    print("Reading and appending every df_first_to_second_purchase..")
    df_first_to_second_purchase = pd.DataFrame()
    for file in sorted(glob(file_directory)):
        if "first_to_second_purchase" in file:
            with gzip.open(file, "rb") as f:
                df_first_to_second_purchase = df_first_to_second_purchase.append(pickle.load(f), ignore_index=True, sort=False)
                del f
        del file

    print("Creating time between first and second purchase dataframe..")
    rows = df_first_to_second_purchase.shape[0]
    df_first_to_second_purchase = pd.merge(df_first_to_second_purchase, df_register_date, on=["USERNO", "USERID", "USERKIND"], how="left")
    if rows != df_first_to_second_purchase.shape[0]:
        raise ValueError("Need to check unique values in df_register_date..")
    else:
        del rows

    df_first_to_second_purchase["firstPurchaseTime"] = pd.to_datetime(df_first_to_second_purchase["firstRegdatesTime"]) - pd.to_datetime(df_first_to_second_purchase["JOINDATE"])
    df_first_to_second_purchase["firstPurchaseTime"] = df_first_to_second_purchase["firstPurchaseTime"].dt.total_seconds()
    ind_join_date = [x.strftime("%Y-%m-%d") for x in pd.date_range(start="2020-03-06", end=datetime.today().strftime("%Y-%m-%d"), freq="D")]
    ind_join_date = "|".join(ind_join_date)
    ind_join_date = df_first_to_second_purchase[df_first_to_second_purchase["JOINDATE"].str.contains("{}".format(ind_join_date))].index.tolist()
    df_first_to_second_purchase["DIV_JOINDATE"] = np.nan
    df_first_to_second_purchase.loc[ind_join_date, "DIV_JOINDATE"] = "2020-03-06 이후"
    df_first_to_second_purchase.drop(["JOINDATE"], axis=1, inplace=True)
    del ind_join_date

    df_first_to_second_purchase = pd.concat([df_first_to_second_purchase.drop(["DIV_JOINDATE"], axis=1),
                                                    df_first_to_second_purchase[df_first_to_second_purchase["DIV_JOINDATE"].notnull()].copy()],
                                                   ignore_index=True)
    df_first_to_second_purchase["DIV_JOINDATE"].fillna("전체", inplace=True)
    df_first_to_second_purchase["filter"] = df_first_to_second_purchase["firstRegdatesTime"].astype(str)
    df_first_to_second_purchase.sort_values(by=["USERNO", "filter"], ascending=[True, True], ignore_index=True, inplace=True)
    df_first_to_second_purchase.drop(["filter"], axis=1, inplace=True)
    notnull_second_purchase = df_first_to_second_purchase[df_first_to_second_purchase["secondRegdatesTime"].notnull()].copy().reset_index(drop=True)
    null_second_purchase = df_first_to_second_purchase[df_first_to_second_purchase["secondRegdatesTime"].isnull()].copy().reset_index(drop=True)
    null_second_purchase = null_second_purchase.groupby(["SESSID", "USERNO", "USERID", "USERKIND", "DIV_JOINDATE"])["firstRegdatesTime"].apply(list).reset_index().rename(
        columns={"firstRegdatesTime": "firstRegdatesTimeLst"})
    null_second_purchase["firstRegdatesTimeLst"] = null_second_purchase["firstRegdatesTimeLst"].apply(lambda x: sorted(list(x)))
    null_second_purchase["purchaseDays"] = null_second_purchase["firstRegdatesTimeLst"].apply(len)

    # puchase date > 1
    purchase_date_greater_than_one = null_second_purchase[null_second_purchase["purchaseDays"] > 1].reset_index(drop=True)
    purchase_date_greater_than_one["firstRegdatesTime"] = purchase_date_greater_than_one["firstRegdatesTimeLst"].apply(lambda x: x[0])
    purchase_date_greater_than_one["secondRegdatesTime"] = purchase_date_greater_than_one["firstRegdatesTimeLst"].apply(lambda x: x[1])

    purchase_date_greater_than_one["firstCheck"] = purchase_date_greater_than_one["firstRegdatesTime"].apply(lambda x: x.split(" ")[0])
    purchase_date_greater_than_one["secondCheck"] = purchase_date_greater_than_one["secondRegdatesTime"].apply(lambda x: x.split(" ")[0])
    if purchase_date_greater_than_one[purchase_date_greater_than_one["firstCheck"] == purchase_date_greater_than_one["secondCheck"]].shape[0] == 0:
        purchase_date_greater_than_one.drop(["firstRegdatesTimeLst", "purchaseDays", "firstCheck", "secondCheck"], axis=1, inplace=True)
    else:
        raise ValueError("Need to check firstCheck and secondCheck columns..")

    # purchase date == 1
    purchase_date_equal_to_one = null_second_purchase[null_second_purchase["purchaseDays"] == 1].reset_index(drop=True)
    purchase_date_equal_to_one["firstRegdatesTime"] = purchase_date_equal_to_one["firstRegdatesTimeLst"].apply(lambda x: x[0])
    purchase_date_equal_to_one["secondRegdatesTime"] = np.nan
    purchase_date_equal_to_one.drop(["firstRegdatesTimeLst", "purchaseDays"], axis=1, inplace=True)

    if null_second_purchase["USERNO"].shape[0] == purchase_date_greater_than_one["USERNO"].shape[0] + purchase_date_equal_to_one["USERNO"].shape[0]:
        null_second_purchase = pd.concat([purchase_date_greater_than_one, purchase_date_equal_to_one], axis=0, ignore_index=True)
        null_second_purchase["secondRegdatesTime"] = pd.to_datetime(null_second_purchase["secondRegdatesTime"])
        del purchase_date_greater_than_one, purchase_date_equal_to_one
    else:
        raise ValueError("Need to check data after concatenating purchase_date_greater_than_one and purchase_date_equal_to_one..")

    df_first_to_second_purchase = pd.concat([notnull_second_purchase.drop(["firstPurchaseTime"], axis=1), null_second_purchase], axis=0, ignore_index=True)
    del notnull_second_purchase, null_second_purchase
    df_first_to_second_purchase["filter"] = df_first_to_second_purchase["firstRegdatesTime"].astype(str)
    df_first_to_second_purchase.sort_values(by=["filter"], ascending=True, inplace=True, ignore_index=True)
    df_first_to_second_purchase.drop(["filter"], axis=1, inplace=True)
    df_first_to_second_purchase.drop_duplicates(["USERNO", "DIV_JOINDATE"], keep="first", inplace=True, ignore_index=True)

    ind_notnull = df_first_to_second_purchase[df_first_to_second_purchase["secondRegdatesTime"].notnull()].index.tolist()
    ind_null = df_first_to_second_purchase[df_first_to_second_purchase["secondRegdatesTime"].isnull()].index.tolist()

    df_first_to_second_purchase.insert(0, "DIV", np.nan)
    df_first_to_second_purchase.loc[ind_notnull, "DIV"] = "첫 구매 후 두 번째 구매까지 걸리는 시간" # firstAndSecondPurchase
    df_first_to_second_purchase.loc[ind_null, "DIV"] = "첫 구매만" # firstPurchaseOnly

    df_first_to_second_purchase.loc[ind_null, "secondRegdatesTime"] = pd.to_datetime(datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
    df_first_to_second_purchase["firstRegdatesTime"] = pd.to_datetime(df_first_to_second_purchase["firstRegdatesTime"])

    df_first_to_second_purchase["firstToSecondPurchaseTime"] = df_first_to_second_purchase["secondRegdatesTime"] - df_first_to_second_purchase["firstRegdatesTime"]
    df_first_to_second_purchase["firstToSecondPurchaseTimeInSeconds"] = df_first_to_second_purchase["firstToSecondPurchaseTime"].dt.total_seconds()

    df_first_to_second_purchase_g = df_first_to_second_purchase.groupby(["DIV", "USERKIND", "DIV_JOINDATE"]).agg(
        {"firstToSecondPurchaseTimeInSeconds": "sum", "USERNO": pd.Series.nunique}).reset_index().rename(
        columns={"firstToSecondPurchaseTimeInSeconds": "firstToSecondPurchaseTotalTime", "USERNO": "UNIQUEUSERNO"})
    df_first_to_second_purchase_g["firstToSecondPurchaseTimePerPerson"] = df_first_to_second_purchase_g["firstToSecondPurchaseTotalTime"] / df_first_to_second_purchase_g["UNIQUEUSERNO"]
    df_first_to_second_purchase_g["firstToSecondPurchaseTimePerPerson"] = df_first_to_second_purchase_g["firstToSecondPurchaseTimePerPerson"].apply(lambda x: str(timedelta(seconds=x)))
    df_first_to_second_purchase_g["firstToSecondPurchaseTimePerPerson"] = df_first_to_second_purchase_g["firstToSecondPurchaseTimePerPerson"].apply(lambda x: x.split(".")[0])
    for val in ["days", "day"]:
        df_first_to_second_purchase_g["firstToSecondPurchaseTimePerPerson"] = df_first_to_second_purchase_g[
            "firstToSecondPurchaseTimePerPerson"].str.replace(val, "일")
        del val
    ind = df_first_to_second_purchase_g[~df_first_to_second_purchase_g["firstToSecondPurchaseTimePerPerson"].str.contains("일")].index.tolist()
    df_first_to_second_purchase_g.loc[ind, "firstToSecondPurchaseTimePerPerson"] = "0 일, " + df_first_to_second_purchase_g.loc[ind, "firstToSecondPurchaseTimePerPerson"]
    del ind
    purchaseTime = df_first_to_second_purchase_g["firstToSecondPurchaseTimePerPerson"].str.split(" 일, ", expand=True)
    purchaseTime["firstToSecondPurchaseTimePerPerson_DAYS"] = purchaseTime[0].apply(lambda x: int(x)).copy()
    purchaseTime["firstToSecondPurchaseTimePerPerson_HOUR"] = purchaseTime[1].apply(lambda x: int(x.split(":")[0]))
    purchaseTime["firstToSecondPurchaseTimePerPerson_MIN"] = purchaseTime[1].apply(lambda x: int(x.split(":")[1]))
    purchaseTime["firstToSecondPurchaseTimePerPerson_SEC"] = purchaseTime[1].apply(lambda x: int(x.split(":")[2]))
    purchaseTime["firstToSecondPurchaseTimePerPerson_YEAR"] = purchaseTime["firstToSecondPurchaseTimePerPerson_DAYS"] // 365
    purchaseTime["firstToSecondPurchaseTimePerPerson_MONTH"] = purchaseTime["firstToSecondPurchaseTimePerPerson_DAYS"] - 365 * purchaseTime["firstToSecondPurchaseTimePerPerson_YEAR"]
    purchaseTime["firstToSecondPurchaseTimePerPerson_MONTH"] = purchaseTime["firstToSecondPurchaseTimePerPerson_MONTH"] // 30
    purchaseTime["firstToSecondPurchaseTimePerPerson_DAY"] = purchaseTime["firstToSecondPurchaseTimePerPerson_DAYS"] - 365 * purchaseTime["firstToSecondPurchaseTimePerPerson_YEAR"] - 30 * purchaseTime["firstToSecondPurchaseTimePerPerson_MONTH"]
    purchaseTime.drop([0, 1], axis=1, inplace=True)
    purchaseTime["firstToSecondPurchaseTimePerPerson_string"] = purchaseTime["firstToSecondPurchaseTimePerPerson_YEAR"].astype(str) + "년 " + purchaseTime["firstToSecondPurchaseTimePerPerson_MONTH"].astype(str) + "개월 " + purchaseTime["firstToSecondPurchaseTimePerPerson_DAY"].astype(str) + "일 " + purchaseTime["firstToSecondPurchaseTimePerPerson_HOUR"].astype(str) + "시간 " + purchaseTime["firstToSecondPurchaseTimePerPerson_MIN"].astype(str) + "분 " + purchaseTime["firstToSecondPurchaseTimePerPerson_SEC"].astype(str) + "초"
    purchaseTime = purchaseTime[["firstToSecondPurchaseTimePerPerson_string"]]
    df_first_to_second_purchase_g = pd.concat([df_first_to_second_purchase_g, purchaseTime], axis=1)
    del purchaseTime
    df_first_to_second_purchase_g = df_first_to_second_purchase_g[["DIV_JOINDATE", "DIV", "USERKIND", "UNIQUEUSERNO", "firstToSecondPurchaseTimePerPerson_string"]].rename(
        columns={"firstToSecondPurchaseTimePerPerson_string": "firstToSecondPurchaseTimePerPerson"})
    df_first_to_second_purchase_g["firstToSecondPurchaseTimePerPerson"] = df_first_to_second_purchase_g["firstToSecondPurchaseTimePerPerson"].str.replace("0년 ", "")
    df_first_to_second_purchase_g.loc[df_first_to_second_purchase_g[df_first_to_second_purchase_g["DIV"] == "첫 구매만"].index.tolist(), "firstToSecondPurchaseTimePerPerson"] = np.nan

    print("Creating time to purchase dataframe..")
    df_time_to_purchase = df_first_to_second_purchase[df_first_to_second_purchase["DIV_JOINDATE"] == "전체"][["DIV", "SESSID", "USERNO", "USERID", "USERKIND", "firstRegdatesTime"]].copy()
    del df_first_to_second_purchase
    rows = df_time_to_purchase.shape[0]
    df_time_to_purchase = pd.merge(df_time_to_purchase, df_register_date, on=["USERNO", "USERID", "USERKIND"], how="left")
    if rows != df_time_to_purchase.shape[0]:
        raise ValueError("Need to check unique values in df_register_date..")
    else:
        del rows

    df_time_to_purchase["firstPurchaseTime"] = df_time_to_purchase["firstRegdatesTime"] - pd.to_datetime(df_time_to_purchase["JOINDATE"])
    df_time_to_purchase["firstPurchaseTime"] = df_time_to_purchase["firstPurchaseTime"].dt.total_seconds()
    ind_join_date = [x.strftime("%Y-%m-%d") for x in pd.date_range(start="2020-03-06", end=datetime.today().strftime("%Y-%m-%d"), freq="D")]
    ind_join_date = "|".join(ind_join_date)
    ind_join_date = df_time_to_purchase[df_time_to_purchase["JOINDATE"].str.contains("{}".format(ind_join_date))].index.tolist()
    df_time_to_purchase["DIV_JOINDATE"] = np.nan
    df_time_to_purchase.loc[ind_join_date, "DIV_JOINDATE"] = "2020-03-06 이후"
    df_time_to_purchase.drop(["JOINDATE"], axis=1, inplace=True)

    df_time_to_purchase_total = df_time_to_purchase.groupby(["USERKIND"]).agg(
        {"USERNO": pd.Series.nunique, "firstPurchaseTime": "sum"}).reset_index().rename(
        columns={"USERNO": "UNIQUEUSERNO", "firstPurchaseTime": "firstPurchaseTimeTotal"})
    df_time_to_purchase_total.insert(0, "DIV_JOINDATE", "전체")
    df_time_to_purchase_filter = df_time_to_purchase.groupby(["DIV_JOINDATE", "USERKIND"]).agg(
        {"USERNO": pd.Series.nunique, "firstPurchaseTime": "sum"}).reset_index().rename(
        columns={"USERNO": "UNIQUEUSERNO", "firstPurchaseTime": "firstPurchaseTimeTotal"})

    df_time_to_purchase = pd.concat([df_time_to_purchase_total, df_time_to_purchase_filter], axis=0, ignore_index=True)
    del df_time_to_purchase_total, df_time_to_purchase_filter

    df_time_to_purchase["firstPurchaseTimePerPerson"] = df_time_to_purchase["firstPurchaseTimeTotal"] / df_time_to_purchase["UNIQUEUSERNO"]
    df_time_to_purchase["firstPurchaseTimePerPerson"] = df_time_to_purchase["firstPurchaseTimePerPerson"].apply(lambda x: str(timedelta(seconds=x)))
    df_time_to_purchase["firstPurchaseTimePerPerson"] = df_time_to_purchase["firstPurchaseTimePerPerson"].apply(lambda x: x.split(".")[0])

    for val in ["days", "day"]:
        df_time_to_purchase["firstPurchaseTimePerPerson"] = df_time_to_purchase["firstPurchaseTimePerPerson"].str.replace(val, "일")
        del val
    ind = df_time_to_purchase[~df_time_to_purchase["firstPurchaseTimePerPerson"].str.contains("일")].index.tolist()
    df_time_to_purchase.loc[ind, "firstPurchaseTimePerPerson"] = "0 일, " + df_time_to_purchase.loc[ind, "firstPurchaseTimePerPerson"]
    del ind
    purchaseTime = df_time_to_purchase["firstPurchaseTimePerPerson"].str.split(" 일, ", expand=True)
    purchaseTime["firstPurchaseTimePerPerson_DAYS"] = purchaseTime[0].apply(lambda x: int(x)).copy()
    purchaseTime["firstPurchaseTimePerPerson_HOUR"] = purchaseTime[1].apply(lambda x: int(x.split(":")[0]))
    purchaseTime["firstPurchaseTimePerPerson_MIN"] = purchaseTime[1].apply(lambda x: int(x.split(":")[1]))
    purchaseTime["firstPurchaseTimePerPerson_SEC"] = purchaseTime[1].apply(lambda x: int(x.split(":")[2]))
    purchaseTime["firstPurchaseTimePerPerson_YEAR"] = purchaseTime["firstPurchaseTimePerPerson_DAYS"] // 365
    purchaseTime["firstPurchaseTimePerPerson_MONTH"] = purchaseTime["firstPurchaseTimePerPerson_DAYS"] - 365 * purchaseTime["firstPurchaseTimePerPerson_YEAR"]
    purchaseTime["firstPurchaseTimePerPerson_MONTH"] = purchaseTime["firstPurchaseTimePerPerson_MONTH"] // 30
    purchaseTime["firstPurchaseTimePerPerson_DAY"] = purchaseTime["firstPurchaseTimePerPerson_DAYS"] - 365 * purchaseTime["firstPurchaseTimePerPerson_YEAR"] - 30 * purchaseTime["firstPurchaseTimePerPerson_MONTH"]
    purchaseTime.drop([0, 1], axis=1, inplace=True)
    purchaseTime["firstPurchaseTimePerPerson_string"] = purchaseTime["firstPurchaseTimePerPerson_YEAR"].astype(str) + "년 " + purchaseTime["firstPurchaseTimePerPerson_MONTH"].astype(str) + "개월 " + purchaseTime["firstPurchaseTimePerPerson_DAY"].astype(str) + "일 " + purchaseTime["firstPurchaseTimePerPerson_HOUR"].astype(str) + "시간 " + purchaseTime["firstPurchaseTimePerPerson_MIN"].astype(str) + "분 " + purchaseTime["firstPurchaseTimePerPerson_SEC"].astype(str) + "초"
    purchaseTime = purchaseTime[["firstPurchaseTimePerPerson_string"]]
    df_time_to_purchase = pd.concat([df_time_to_purchase, purchaseTime], axis=1)
    del purchaseTime
    df_time_to_purchase = df_time_to_purchase[["DIV_JOINDATE", "USERKIND", "UNIQUEUSERNO", "firstPurchaseTimePerPerson_string"]].rename(
        columns={"firstPurchaseTimePerPerson_string": "firstPurchaseTimePerPerson"})
    df_time_to_purchase["firstPurchaseTimePerPerson"] = df_time_to_purchase["firstPurchaseTimePerPerson"].str.replace("0년 ", "")

    del df_register_date
    today = datetime.today()
    if today.month == 1 and today.day == 1:
        update_year = today.year - 1
    else:
        update_year = today.year
    df_main_total = conestore_total_paid_user_with_total_active_user(table_name_paid_user="df_t_order",
                                                                      table_name_active_user="df_t_user_log",
                                                                      dbname=dbname, year=update_year) # update 날짜 전일 해당 연도
    df_main_total.rename(columns={"재구매_Y": "REPURCHASE_Y", "재구매_N": "REPURCHASE_N", "재구매_총": "REPURCHASE_TOTAL", "재구매율": "REPURCHASERATE"}, inplace=True)
    df_main_total.columns = [x.upper() for x in df_main_total.columns.tolist()]

    print("Merging active user and paid user dataframe..")
    df_active_user = pd.read_excel(file_active_user)
    df_active_user = df_active_user[(df_active_user["USERKIND"] == "회원") | (df_active_user["USERKIND"] == "비회원")].reset_index(drop=True)
    df_paid_user = pd.read_excel(file_paid_user)
    df_paid_user = df_paid_user[(df_paid_user["USERKIND"] == "회원") | (df_paid_user["USERKIND"] == "비회원")].reset_index(drop=True)

    df_main = pd.merge(df_active_user, df_paid_user, on=["div", "Date", "USERKIND"], how="outer")
    del df_active_user, df_paid_user
    print("Creating additional KPIs..")
    df_main["paidUserRate"] = df_main["paidUser"] / df_main["activeUser"]
    df_main["ARPPU"] = df_main["totalOrderPrice"] / df_main["paidUser"]
    df_main["ARPDAU"] = df_main["totalOrderPrice"] / df_main["activeUser"]

    df_main["monthlyMAU"] = np.nan
    df_main["monthlyMAU_date"] = np.nan
    df_main["engagement"] = np.nan
    for dt in df_main[df_main["div"] == "월별"]["Date"].unique():
        lst_ind = df_main[(df_main["div"] == "일별") & (df_main["Date"].str.contains(dt))].index
        mau = df_main[(df_main["div"] == "월별") & (df_main["Date"] == dt)]["activeUser"].unique()[0]
        df_main.loc[lst_ind, "monthlyMAU"] = mau
        df_main.loc[lst_ind, "monthlyMAU_date"] = dt
        df_main.loc[lst_ind, "engagement"] = df_main.loc[lst_ind, "activeUser"] / df_main.loc[lst_ind, "monthlyMAU"]
        del dt, lst_ind, mau
    ####################################################################################################################
    print("Changing all the column names to upper case english names..")
    df_main = df_main.rename(columns={"재구매율": "repurchaserate", "재구매_Y": "repurchase_Y", "재구매_N": "repurchase_N", "재구매_총": "repurchase_TOTAL"})
    df_main.columns = [x.upper()for x in df_main.columns.tolist()]

    df_first_to_second_purchase_g.columns = [x.upper() for x in df_first_to_second_purchase_g.columns.tolist()]

    df_time_to_purchase.columns = [x.upper() for x in df_time_to_purchase.columns.tolist()]

    df_retention_rate = df_retention_rate.rename(columns={"JOINDATE": "DATE"})

    df_cart_car = df_cart_car.rename(columns={"REGDATE": "DATE"})

    df_main = pd.merge(df_main, df_cart_car, on=["DIV", "DATE", "USERKIND"], how="outer")
    df_main_daily = df_main[df_main["DIV"] == "일별"].copy()
    df_main_monthly = df_main[df_main["DIV"] == "월별"].copy()
    df_main_monthly["DATE"] = df_main_monthly["DATE"].apply(lambda x: x+"-{}".format(monthrange(year=int(x.split("-")[0]), month=int(x.split("-")[1]))[1]))
    last_day_daily = sorted(df_main_daily["DATE"].unique().tolist())[-1]
    last_day_monthly = sorted(df_main_monthly["DATE"].unique().tolist())[-1]
    if last_day_daily != last_day_monthly:
        df_main_monthly.loc[df_main_monthly[df_main_monthly["DATE"] == last_day_monthly].index.tolist()[0], "DATE"] = last_day_daily
    del last_day_daily, last_day_monthly

    df_main = pd.concat([df_main_daily, df_main_monthly], axis=0, ignore_index=True)
    del df_main_daily, df_main_monthly, df_cart_car

    df_cart_info.drop(["REGDATE_DIV"], axis=1, inplace=True)
    for col in ["RANK_UNIQUEUSERNO", "RANK_EA"]:
        df_cart_info[col] = np.nan
        del col

    print("Creating ranking for df_cart_info_daily..")
    df_cart_info_daily = df_cart_info[df_cart_info["DIV"] == "일별"].copy().reset_index(drop=True)
    for dt in df_cart_info_daily["REGDATE"].unique():
        temp = df_cart_info_daily[df_cart_info_daily["REGDATE"] == dt].copy().reset_index().rename(columns={"index": "INDEX"}).drop(["RANK_UNIQUEUSERNO", "RANK_EA"], axis=1)
        temp = temp.sort_values(by=["UNIQUEUSERNO", "EA"], ignore_index=True, ascending=[False, False]).reset_index().rename(columns={"index": "RANK_UNIQUEUSERNO"})
        temp["RANK_UNIQUEUSERNO"] += 1
        temp = temp.sort_values(by=["EA", "UNIQUEUSERNO"], ignore_index=True, ascending=[False, False]).reset_index().rename(columns={"index": "RANK_EA"})
        temp["RANK_EA"] += 1
        temp = temp.set_index(["INDEX"])
        temp.drop(["DIV", "REGDATE", "GOODSNAME", "ISDIRECT", "UNIQUEUSERNO", "EA"], axis=1, inplace=True)

        df_cart_info_daily.loc[temp.index.tolist(), "RANK_UNIQUEUSERNO"] = temp.loc[temp.index.tolist(), "RANK_UNIQUEUSERNO"]
        df_cart_info_daily.loc[temp.index.tolist(), "RANK_EA"] = temp.loc[temp.index.tolist(), "RANK_EA"]
        del temp, dt
    for col in ["RANK_UNIQUEUSERNO", "RANK_EA"]:
        df_cart_info_daily[col] = df_cart_info_daily[col].astype(int)
        del col
    df_cart_info_daily["REGDATE_PREV"] = pd.to_datetime(df_cart_info_daily["REGDATE"])
    df_cart_info_daily["REGDATE_PREV"] = df_cart_info_daily["REGDATE_PREV"].apply(lambda x: x-timedelta(days=1)).astype(str)
    df_cart_info_daily_prev_day = df_cart_info_daily[["DIV", "REGDATE", "GOODSNAME", "ISDIRECT", "RANK_UNIQUEUSERNO", "RANK_EA"]].rename(
        columns={"REGDATE": "REGDATE_PREV", "RANK_UNIQUEUSERNO": "RANK_UNIQUEUSERNO_PREV", "RANK_EA": "RANK_EA_PREV"})
    df_cart_info_daily = pd.merge(df_cart_info_daily, df_cart_info_daily_prev_day, on=["DIV", "REGDATE_PREV", "GOODSNAME", "ISDIRECT"], how="left")
    del df_cart_info_daily_prev_day
    for col in ["RANK_UNIQUEUSERNO", "RANK_EA"]:
        df_cart_info_daily["{}_CHANGES".format(col)] = df_cart_info_daily["{}_PREV".format(col)] - df_cart_info_daily[col]
        del col
    df_cart_info_daily.drop(["REGDATE_PREV"], axis=1, inplace=True)

    print("Creating ranking for df_cart_info_monthly..")
    df_cart_info_monthly = df_cart_info[df_cart_info["DIV"] == "월별"].copy().reset_index(drop=True)
    for dt in df_cart_info_monthly["REGDATE"].unique():
        temp = df_cart_info_monthly[df_cart_info_monthly["REGDATE"] == dt].copy().reset_index().rename(columns={"index": "INDEX"}).drop(["RANK_UNIQUEUSERNO", "RANK_EA"], axis=1)
        temp = temp.sort_values(by=["UNIQUEUSERNO", "EA"], ignore_index=True, ascending=[False, False]).reset_index().rename(columns={"index": "RANK_UNIQUEUSERNO"})
        temp["RANK_UNIQUEUSERNO"] += 1
        temp = temp.sort_values(by=["EA", "UNIQUEUSERNO"], ignore_index=True, ascending=[False, False]).reset_index().rename(columns={"index": "RANK_EA"})
        temp["RANK_EA"] += 1
        temp = temp.set_index(["INDEX"])
        temp.drop(["DIV", "REGDATE", "GOODSNAME", "ISDIRECT", "UNIQUEUSERNO", "EA"], axis=1, inplace=True)

        df_cart_info_monthly.loc[temp.index.tolist(), "RANK_UNIQUEUSERNO"] = temp.loc[temp.index.tolist(), "RANK_UNIQUEUSERNO"]
        df_cart_info_monthly.loc[temp.index.tolist(), "RANK_EA"] = temp.loc[temp.index.tolist(), "RANK_EA"]
        del temp, dt
    for col in ["RANK_UNIQUEUSERNO", "RANK_EA"]:
        df_cart_info_monthly[col] = df_cart_info_monthly[col].astype(int)
        del col
    df_cart_info_monthly["REGDATE_PREV"] = pd.to_datetime(df_cart_info_monthly["REGDATE"])

    df_cart_info_monthly["REGDATE_PREV"] = df_cart_info_monthly["REGDATE_PREV"].apply(lambda x: x - relativedelta(months=+1)).astype(str).apply(lambda x: x[:-3])
    df_cart_info_monthly_prev_month = df_cart_info_monthly[["DIV", "REGDATE", "GOODSNAME", "ISDIRECT", "RANK_UNIQUEUSERNO", "RANK_EA"]].rename(
        columns={"REGDATE": "REGDATE_PREV", "RANK_UNIQUEUSERNO": "RANK_UNIQUEUSERNO_PREV", "RANK_EA": "RANK_EA_PREV"})
    df_cart_info_monthly = pd.merge(df_cart_info_monthly, df_cart_info_monthly_prev_month, on=["DIV", "REGDATE_PREV", "GOODSNAME", "ISDIRECT"], how="left")
    del df_cart_info_monthly_prev_month
    for col in ["RANK_UNIQUEUSERNO", "RANK_EA"]:
        df_cart_info_monthly["{}_CHANGES".format(col)] = df_cart_info_monthly["{}_PREV".format(col)] - df_cart_info_monthly[col]
        del col
    df_cart_info_monthly.drop(["REGDATE_PREV"], axis=1, inplace=True)
    df_cart_info_monthly["REGDATE"] = df_cart_info_monthly["REGDATE"].apply(lambda x: x+"-{}".format(monthrange(year=int(x.split("-")[0]), month=int(x.split("-")[1]))[1]))
    last_day_daily = sorted(df_cart_info_daily["REGDATE"].unique().tolist())[-1]
    last_day_monthly = sorted(df_cart_info_monthly["REGDATE"].unique().tolist())[-1]
    if last_day_daily != last_day_monthly:
        df_cart_info_monthly.loc[df_cart_info_monthly[df_cart_info_monthly["REGDATE"] == last_day_monthly].index.tolist(), "REGDATE"] = last_day_daily
    del last_day_daily, last_day_monthly

    print("Creating ranking for df_cart_info_total..")
    df_cart_info_total = df_cart_info[df_cart_info["DIV"] == "전체"].copy().reset_index(drop=True).drop(["RANK_UNIQUEUSERNO", "RANK_EA"], axis=1)
    df_cart_info_total = df_cart_info_total.sort_values(by=["UNIQUEUSERNO", "EA"], ignore_index=True, ascending=[False, False]).reset_index().rename(columns={"index": "RANK_UNIQUEUSERNO"})
    df_cart_info_total["RANK_UNIQUEUSERNO"] += 1
    df_cart_info_total = df_cart_info_total.sort_values(by=["EA", "UNIQUEUSERNO"], ignore_index=True, ascending=[False, False]).reset_index().rename(columns={"index": "RANK_EA"})
    df_cart_info_total["RANK_EA"] += 1
    del df_cart_info

    df_cart_info = pd.concat([df_cart_info_daily, df_cart_info_monthly, df_cart_info_total], axis=0, ignore_index=True).rename(columns={"REGDATE": "Date"})
    del df_cart_info_daily, df_cart_info_monthly, df_cart_info_total

    for col in ["TOTALORDERPRICE"]:
        if df_main[df_main["DIV"] == "일별"][col].sum() != df_main[df_main["DIV"] == "월별"][col].sum():
            raise ValueError("2. Need to check data for column {}\n'DIV' == 일별 : {}\n'DIV' == 월별 : {}".format(
                col, df_main[df_main["DIV"] == "일별"][col].sum(), df_main[df_main["DIV"] == "월별"][col].sum()))
        del col

    df_weekday_engagement_rate = pd.DataFrame()
    print("Reading all df_weekday_engagement_rate files..")
    for file_nm in sorted(glob(folder_active_user)):
        if "weekday_engagement_rate" in file_nm:
            with gzip.open(file_nm, "rb") as f:
                temp = pickle.load(f)
                temp = temp[(temp["USERKIND"] == "회원") | (temp["USERKIND"] == "비회원")].reset_index(drop=True).rename(columns={"REGDATE": "DATE"})
                df_weekday_engagement_rate = df_weekday_engagement_rate.append(temp, sort=False, ignore_index=True)
                del f, temp
        del file_nm
    del folder_active_user
    df_weekday_engagement_rate.columns = [x.upper() for x in df_weekday_engagement_rate.columns.tolist()]
    df_weekday_engagement_rate_prev = df_weekday_engagement_rate[["DATE", "WEEKDAY", "WEEKDAYACTIVEUSER"]].copy().rename(
        columns={"DATE": "DATE_PREV", "WEEKDAY": "WEEKDAY_PREV", "WEEKDAYACTIVEUSER": "WEEKDAYACTIVEUSER_PREV"})
    temp = df_weekday_engagement_rate.groupby(["DATE"])["WEEKDAY"].nunique().reset_index().drop(["WEEKDAY"], axis=1)
    temp["DATE_PREV"] = temp["DATE"].shift(1)

    df_weekday_engagement_rate = pd.merge(df_weekday_engagement_rate, temp, on=["DATE"], how="outer")
    del temp
    df_weekday_engagement_rate = pd.merge(df_weekday_engagement_rate, df_weekday_engagement_rate_prev, on=["DATE_PREV"], how="left")
    del df_weekday_engagement_rate_prev
    df_weekday_engagement_rate["WEEKDAYACTIVEUSER_CHANGES"] = df_weekday_engagement_rate["WEEKDAYACTIVEUSER"] - df_weekday_engagement_rate["WEEKDAYACTIVEUSER_PREV"]
    df_weekday_engagement_rate["WEEKDAYACTIVEUSER_CHANGES_RATE"] = df_weekday_engagement_rate["WEEKDAYACTIVEUSER_CHANGES"] / df_weekday_engagement_rate["WEEKDAYACTIVEUSER_PREV"]

    print("Reading all df_by_join_date files..")
    df_by_join_date = pd.DataFrame()
    for file in sorted(glob(folder_paid_user)):
        if "df_by_join_date" in file:
            with gzip.open(file, "rb") as f:
                df_by_join_date = df_by_join_date.append(pickle.load(f), sort=False, ignore_index=True)
                del f
        del file
    df_by_join_date.rename(columns={"paymentConfirmDate": "DATE"}, inplace=True)

    df_cancellation_list = pd.DataFrame()
    print("Reading every df_cancellation_list files..")
    for file in sorted(glob(folder_paid_user)):
        if "cancellation_list" in file:
            with gzip.open(file, "rb") as f:
                df_cancellation_list = df_cancellation_list.append(pickle.load(f), sort=False, ignore_index=True)
                del f
        del file
    df_cancellation_list = pd.concat([df_cancellation_list[df_cancellation_list["div"] == "일별"].copy(),
                                      df_cancellation_list[df_cancellation_list["div"] == "월별"].copy()], axis=0, ignore_index=True)
    df_cancellation_list.rename(columns={"회원구분": "member", "paymentCount": "payment", "cancelPaymentCount": "cancelPayment"},
                                inplace=True)
    df_cancellation_list.columns = [x.upper() for x in df_cancellation_list.columns.tolist()]

    for val in df_cancellation_list["CANCELSTATUS"].unique():
        if df_cancellation_list[df_cancellation_list["CANCELSTATUS"] == val]["CANCELPAYMENT"].sum() == 0:
            df_cancellation_list = df_cancellation_list[df_cancellation_list["CANCELSTATUS"] != val].reset_index(drop=True)

    df_order_region = pd.DataFrame()
    print("Reading every df_order_region files..")
    for file in sorted(glob(folder_paid_user)):
        if "order_region" in file:
            with gzip.open(file, "rb") as f:
                df_order_region = df_order_region.append(pickle.load(f), sort=False, ignore_index=True)
                del f
        del file
    df_order_region.drop(["DIV_ADDR"], axis=1, inplace=True)
    df_order_region["CLAIMTYPE"] = df_order_region["CLAIMTYPE"].str.replace("NULL", "클레임 없음")
    df_order_region["REASON"] = df_order_region["REASON"].str.replace("NULL", "취소 사유 없음")
    df_order_region.rename(columns={"REGDATE": "DATE", "회원구분": "member"}, inplace=True)
    df_order_region.columns = [x.upper() for x in df_order_region.columns.tolist()]
    for val in ["구매 확정", "배송 중"]:
        df_order_region = df_order_region[df_order_region["CANCELSTATUS"] != val].reset_index(drop=True)
        del val

    df_order_region_reasondetail = df_order_region.groupby(["REASONDETAIL"])["REASON"].count().reset_index()
    if df_order_region.shape[0] == df_order_region_reasondetail["REASON"].sum():
        df_order_region_reasondetail.drop(["REASON"], axis=1, inplace=True)
        df_order_region_reasondetail["REASONDETAIL"] = df_order_region_reasondetail["REASONDETAIL"].str.replace(" ", "")
        df_order_region_reasondetail.drop_duplicates(keep="first", inplace=True)
    else:
        raise ValueError("Need to check data for df_order_region_reasondetail..")
    df_order_region_reasondetail["REASONDETAIL_STR"] = df_order_region_reasondetail["REASONDETAIL"].copy().astype(str)
    df_order_region_reasondetail["REASONDETAIL_CATEGORY"] = np.nan
    fill_ind = df_order_region_reasondetail[df_order_region_reasondetail["REASONDETAIL_CATEGORY"].isnull()].copy()
    fill_ind["REASONDETAIL_CATEGORY"] = fill_ind["REASONDETAIL_STR"].str.extract("(배송)") # 배송 관련
    fill_ind = fill_ind[fill_ind["REASONDETAIL_CATEGORY"].notnull()].copy()
    fill_ind = fill_ind[~fill_ind["REASONDETAIL_STR"].str.contains("배송비")].index.tolist()
    df_order_region_reasondetail.loc[fill_ind,  "REASONDETAIL_CATEGORY"] = "배송"
    # key 순서 변경하지 말기
    map_reason = {"다음에|나중에|추후에|추후": "다른기간",
                  "마음|마으|마으미|마음이|마미|맘이|맘|그냥|변심|갑자기|싫어|않할|생각|원하지않|재미|고민|원하는|싶어": "단순변심",
                  "실수|재구매|혼자|인증|중복|동일|충동|변경|잘못|다시|재주문|다른|딴거|딴걸|바꾸|다르|작|취소|막|잘목|잘 목|잘 못|잘못|짤못|짤몼|이미|하지말라|사지말라|오류|호기심|다른상품|다릉상품|바꿀|반품": "취소",
                  "필료|필요없|핑요업|핑요없|필요업|필요엇|필요가없|필요가업|필요하지않|유용|안쓸것|불필요|사용하지못|미사용": "불필요",
                  "기프티콘|기프트콘|콘|기프티쇼|기프트쇼": "기프티콘",
                  "부족|품절|없대요|없어요|재고": "부족",
                  "프렌": "홈런프렌즈",
                  "구매의사|모르|아이|동의": "단순변심",
                  "집|이사|주소": "집",
                  "코로나": "코로나",
                  "할아버지|할머니|아빠|엄마|아버지|어머니|형|오빠|누나|언니|부모님|동생|가족": "가족",
                  "죄송|죄|미안": "죄송",
                  "개인사정|개인사유": "개인사유",
                  "사고|사기|사용": "단순변심",
                  "비쌈|비싸|가격|돈|금액": "가격",
                  "메세지|문자": "문자"}
    for k in map_reason.keys():
        fill_ind = df_order_region_reasondetail[df_order_region_reasondetail["REASONDETAIL_CATEGORY"].isnull()].copy()
        fill_ind["REASONDETAIL_CATEGORY"] = fill_ind["REASONDETAIL_STR"].str.extract("({})".format(k))
        fill_ind = fill_ind[fill_ind["REASONDETAIL_CATEGORY"].notnull()].copy().index.tolist()
        df_order_region_reasondetail.loc[fill_ind, "REASONDETAIL_CATEGORY"] = map_reason[k]
        del k, fill_ind
    df_order_region_reasondetail["REASONDETAIL_CATEGORY"].fillna("기타", inplace=True)
    df_order_region_reasondetail.drop(["REASONDETAIL"], axis=1, inplace=True)

    df_order_region["REASONDETAIL_STR"] = df_order_region["REASONDETAIL"].copy().str.replace(" ", "")
    rows = df_order_region.shape[0]
    df_order_region = pd.merge(df_order_region, df_order_region_reasondetail, on=["REASONDETAIL_STR"], how="left")
    if rows == df_order_region.shape[0]:
        del rows
        df_order_region.drop(["REASONDETAIL_STR"], axis=1, inplace=True)
    else:
        raise ValueError("Need to check data for df_order_region_reason_detail..")

    print("Creating (전광판용) 1.전일,  2.전주 동일,  3.전월 동일..")
    df_prev = df_main[df_main["DIV"] == "일별"][["DIV", "DATE"]].copy()
    df_prev["DATE"] = pd.to_datetime(df_prev["DATE"])
    df_prev["DATE_PREV_DAY"] = df_prev["DATE"].apply(lambda x: x - timedelta(days=1))
    df_prev["DATE_PREV_WEEK"] = df_prev["DATE"].apply(lambda x: x - timedelta(days=7))
    df_prev["DATE_PREV_MONTH"] = df_prev["DATE"].apply(lambda x: x - timedelta(days=30))
    for col in [x for x in df_prev.columns if "DATE" in x]:
        df_prev[col] = df_prev[col].astype(str)
        del col

    col_main_total_temp = ['DIV', 'DATE', 'USERKIND', 'WEEKDAY', 'ACTIVEUSER', 'TOTALORDERPRICE', 'PAYMENT', 'CANCELPAYMENT', 'CANCELLATIONRATIO']
    df_main_total_temp = df_main[df_main["DIV"] == "일별"][col_main_total_temp].copy()

    df_main_total_temp_prev_day = df_main_total_temp.copy()
    df_main_total_temp_prev_day.columns = ["{}_PREV_DAY".format(x) for x in df_main_total_temp_prev_day.columns]
    df_main_total_temp_prev_day = df_main_total_temp_prev_day.rename(columns={"DIV_PREV_DAY": "DIV", "USERKIND_PREV_DAY": "USERKIND"}).drop(
        ["WEEKDAY_PREV_DAY", "CANCELLATIONRATIO_PREV_DAY"], axis=1)

    df_main_total_temp_prev_week = df_main_total_temp.copy()
    df_main_total_temp_prev_week.columns = ["{}_PREV_WEEK".format(x) for x in df_main_total_temp_prev_week.columns]
    df_main_total_temp_prev_week = df_main_total_temp_prev_week.rename(columns={"DIV_PREV_WEEK": "DIV", "USERKIND_PREV_WEEK": "USERKIND"}).drop(
        ["WEEKDAY_PREV_WEEK", "CANCELLATIONRATIO_PREV_WEEK"], axis=1)

    df_main_total_temp_prev_month = df_main_total_temp.copy()
    df_main_total_temp_prev_month.columns = ["{}_PREV_MONTH".format(x) for x in df_main_total_temp_prev_month.columns]
    df_main_total_temp_prev_month = df_main_total_temp_prev_month.rename(columns={"DIV_PREV_MONTH": "DIV", "USERKIND_PREV_MONTH": "USERKIND"}).drop(
        ["WEEKDAY_PREV_MONTH", "CANCELLATIONRATIO_PREV_MONTH"], axis=1)

    row = df_main_total_temp.shape[0]
    df_main_total_temp = pd.merge(df_prev, df_main_total_temp, on=["DIV", "DATE"], how="right")
    del df_prev
    df_main_total_temp = pd.merge(df_main_total_temp, df_main_total_temp_prev_day, on=["DIV", "USERKIND", "DATE_PREV_DAY"], how="left")
    df_main_total_temp = pd.merge(df_main_total_temp, df_main_total_temp_prev_week, on=["DIV", "USERKIND", "DATE_PREV_WEEK"], how="left")
    df_main_total_temp = pd.merge(df_main_total_temp, df_main_total_temp_prev_month, on=["DIV", "USERKIND", "DATE_PREV_MONTH"], how="left")
    df_main_total_temp.drop([x for x in df_main_total_temp.columns if "DATE" in x and "PREV" in x], axis=1, inplace=True)

    if row != df_main_total_temp.shape[0]:
        raise ValueError("Need to check data after matching day, week, month..")
    else:
        del df_main_total_temp_prev_day, df_main_total_temp_prev_week, df_main_total_temp_prev_month, row

    for dt_div in ["PREV_DAY", "PREV_WEEK", "PREV_MONTH"]:
        for col in ["ACTIVEUSER", "TOTALORDERPRICE", "PAYMENT", "CANCELPAYMENT"]:
            df_main_total_temp["{}_CHANGES_RATE_{}".format(col, dt_div)] = (df_main_total_temp[col] - df_main_total_temp["{}_{}".format(col, dt_div)]) / df_main_total_temp["{}_{}".format(col, dt_div)]
            del col
        del dt_div

    df_total_order_sequence = conestore_total_order_sequence(table_name="df_order_sequence", dbname=dbname)
    df_total_order_sequence["CLAIMTYPE"] = df_total_order_sequence["CLAIMTYPE"].str.replace("NULL", "클레임 없음")
    df_total_order_sequence["REASON"] = df_total_order_sequence["REASON"].str.replace("NULL", "취소 사유 없음")
    df_total_order_sequence["REASONDETAIL_STR"] = df_total_order_sequence["REASONDETAIL"].str.replace(" ", "")
    df_total_order_sequence = pd.merge(df_total_order_sequence, df_order_region_reasondetail,  on=["REASONDETAIL_STR"], how="left")
    df_total_order_sequence.drop(["REASONDETAIL", "REASONDETAIL_STR"], axis=1, inplace=True)

    export_pickle(file_name=file_export_df_total_order_sequence, df=df_total_order_sequence)
    del file_export_df_total_order_sequence, df_total_order_sequence
    export_pickle(file_name=file_export_df_cancellation_list, df=df_cancellation_list)
    del file_export_df_cancellation_list, df_cancellation_list
    export_pickle(file_name=file_export_df_order_region, df=df_order_region)
    del file_export_df_order_region, df_order_region
    export_pickle(file_name=file_export_df_by_join_date, df=df_by_join_date)
    del file_export_df_by_join_date, df_by_join_date
    export_pickle(file_name=file_export_df_main_total, df=df_main_total)
    del file_export_df_main_total, df_main_total
    export_pickle(file_name=file_export_df_main_total_temp, df=df_main_total_temp)
    del file_export_df_main_total_temp, df_main_total_temp
    export_pickle(file_name=file_export_df_main, df=df_main)
    del file_active_user, file_paid_user, file_export_df_main, df_main
    export_pickle(file_name=file_export_df_cart_info, df=df_cart_info)
    del file_export_df_cart_info, df_cart_info
    export_pickle(file_name=file_export_df_first_to_second_purchase, df=df_first_to_second_purchase_g)
    del file_export_df_first_to_second_purchase, df_first_to_second_purchase_g
    export_pickle(file_name=file_export_df_time_to_purchase, df=df_time_to_purchase)
    del file_export_df_time_to_purchase, df_time_to_purchase
    export_pickle(file_name=file_export_df_retention_rate, df=df_retention_rate)
    del file_export_df_retention_rate, df_retention_rate
    export_pickle(file_name=file_export_df_weekday_engagement_rate, df=df_weekday_engagement_rate)
    del file_export_df_weekday_engagement_rate, df_weekday_engagement_rate


def homelearnfriendsmall_tableau_upload_df_user(dbname=None):
    from glob import glob
    from sys import platform
    import pandas as pd
    import numpy as np
    import gzip
    import pickle
    from datetime import timedelta, datetime
    from functools import reduce
    from calendar import monthrange
    from dateutil.relativedelta import relativedelta

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    today = datetime.today()
    if today.month == 1 and today.day == 1:
        update_year_main_board = today.year - 1
    else:
        update_year_main_board = today.year
    del today

    map_df = {"df_hiclassmember": "hiclassmember", "df_nomember": "nomember", "df_member": "member"}

    # file_active_user = file_directory
    # file_paid_user_total = file_directory
    # file_paid_user_div = file_directory
    #
    # folder_homelearnfriendsmall_database = file_directory
    # folder_homelearnfriendsmall_active_user = file_directory
    # folder_homelearnfriendsmall_paid_user = file_directory

    df_register_date = homelearnfriendsmall_total_t_user(dbname=dbname)
    df_register_date["JOINDATE"] = df_register_date["JOINDATE"].apply(lambda x: str(x).split(" ")[0])

    df_retention_rate = pd.DataFrame()
    for file in sorted(glob(folder_homelearnfriendsmall_active_user)):
        if "df_retention_rate" in file:
            df_retention_rate = df_retention_rate.append(import_pickle(file), sort=False, ignore_index=True)
        del file
    df_retention_rate = df_retention_rate.rename(columns={"JOINDATE": "DATE"})
    df_retention_rate["USERKIND_CAT"] = df_retention_rate["USERKIND_CAT"].map({"socialmember": "회원", "hiclass": "하이클래스"})
    if df_retention_rate[df_retention_rate["USERKIND_CAT"].isnull()].shape[0] != 0:
        raise ValueError("There are null values in USERKIND_CAT after mapping..")
    df_retention_rate["LEVELIDX"] = df_retention_rate["LEVELIDX_GUBUN"].apply(lambda x: x.split("_")[0])
    df_retention_rate["GUBUN"] = df_retention_rate["LEVELIDX_GUBUN"].apply(lambda x: x.split("_")[1])
    df_retention_rate = df_retention_rate[['DATE', 'JOINDATE_TO_REGDATE', 'AGE_BIN', 'USERKIND_CAT',
                                           'LEVELIDX', 'GUBUN', 'USERNO_total']]
    df_retention_rate = df_retention_rate[df_retention_rate["DATE"] >= "2020-03-06"].reset_index(drop=True)
    df_retention_rate.columns = [x.upper() for x in df_retention_rate.columns]

    df_goods = recent_t_goods(dbname=dbname)

    df_first_to_second_purchase = pd.DataFrame()
    for file in sorted(glob(folder_homelearnfriendsmall_paid_user)):
        if "first_to_second_purchase" in file:
            temp = import_pickle(file)
            for k in map_df.keys():
                if k in file:
                    temp.insert(0, "DIV_MEMBER", map_df[k])
                del k
            df_first_to_second_purchase = df_first_to_second_purchase.append(temp, sort=False, ignore_index=True)
            del temp
        del file
    df_first_to_second_purchase["LEVELIDX_GUBUN_T_MEMBER"] = df_first_to_second_purchase["LEVELIDX_GUBUN_T_MEMBER_FLATFORM"].apply(
        lambda x: x.split("_")[0] + "_" + x.split("_")[1])
    df_first_to_second_purchase["FLATFORM"] = df_first_to_second_purchase["LEVELIDX_GUBUN_T_MEMBER_FLATFORM"].apply(
        lambda x: x.split("_")[-1])
    df_first_to_second_purchase = pd.concat([df_first_to_second_purchase[["DIV_MEMBER", "USERNO", "LEVELIDX_GUBUN_T_MEMBER",
                                                                          "FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER",
                                                                          "AGE_BIN_ENG_T_MEMBER", "firstRegdatesTime"]].rename(columns={"firstRegdatesTime": "RegdatesTime"}),
                                             df_first_to_second_purchase[["DIV_MEMBER", "USERNO", "LEVELIDX_GUBUN_T_MEMBER",
                                                                          "FLATFORM", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER",
                                                                          "AGE_BIN_ENG_T_MEMBER", "secondRegdatesTime"]].rename(columns={"secondRegdatesTime": "RegdatesTime"})],
                                            axis=0, ignore_index=True)
    df_first_to_second_purchase = df_first_to_second_purchase[df_first_to_second_purchase["RegdatesTime"].notnull()].reset_index(drop=True)
    df_first_to_second_purchase["RegdatesTime"] = pd.to_datetime(df_first_to_second_purchase["RegdatesTime"])
    df_first_to_second_purchase["RegdatesTimeTemp"] = df_first_to_second_purchase["RegdatesTime"].copy().astype(str)
    df_first_to_second_purchase = df_first_to_second_purchase.sort_values(by=["RegdatesTimeTemp"], ignore_index=True).drop(
        ["RegdatesTimeTemp"], axis=1)

    col_group = ["DIV_MEMBER", "USERNO", "LEVELIDX_GUBUN_T_MEMBER", "GENDER_T_MEMBER", "AGE_BIN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"]
    df_first_to_second_purchase_regdates = df_first_to_second_purchase.groupby(col_group)["RegdatesTime"].apply(list).reset_index()
    df_first_to_second_purchase_flatform = df_first_to_second_purchase.groupby(col_group)["FLATFORM"].apply(list).reset_index()
    df_first_to_second_purchase = pd.merge(df_first_to_second_purchase_regdates, df_first_to_second_purchase_flatform,
                                           on=col_group, how="outer")
    del df_first_to_second_purchase_regdates, df_first_to_second_purchase_flatform

    if df_first_to_second_purchase.shape[0] != df_first_to_second_purchase["USERNO"].nunique():
        raise ValueError("Need to check unique USERNOs for finding first_to_second_purchase..")
    df_first_to_second_purchase["purchaseDays"] = df_first_to_second_purchase["RegdatesTime"].str.len()
    df_first_to_second_purchase["firstFLATFORM"] = np.nan
    df_first_to_second_purchase["secondFLATFORM"] = np.nan
    df_first_to_second_purchase["firstRegdatesTime"] = np.nan
    df_first_to_second_purchase["secondRegdatesTime"] = np.nan

    lst_purchasedays_equals_to_one = df_first_to_second_purchase[df_first_to_second_purchase["purchaseDays"] == 1].index.tolist()
    lst_purchasedays_greater_than_one = df_first_to_second_purchase[df_first_to_second_purchase["purchaseDays"] > 1].index.tolist()

    df_first_to_second_purchase.loc[lst_purchasedays_equals_to_one, "firstRegdatesTime"] = df_first_to_second_purchase.loc[lst_purchasedays_equals_to_one, "RegdatesTime"].apply(lambda x: x[0])
    df_first_to_second_purchase.loc[lst_purchasedays_equals_to_one, "firstFLATFORM"] = df_first_to_second_purchase.loc[lst_purchasedays_equals_to_one, "FLATFORM"].apply(lambda x: x[0])

    df_first_to_second_purchase.loc[lst_purchasedays_greater_than_one, "firstRegdatesTime"] = df_first_to_second_purchase.loc[lst_purchasedays_greater_than_one, "RegdatesTime"].apply(lambda x: x[0])
    df_first_to_second_purchase.loc[lst_purchasedays_greater_than_one, "secondRegdatesTime"] = df_first_to_second_purchase.loc[lst_purchasedays_greater_than_one, "RegdatesTime"].apply(lambda x: x[1])
    df_first_to_second_purchase.loc[lst_purchasedays_greater_than_one, "firstFLATFORM"] = df_first_to_second_purchase.loc[lst_purchasedays_greater_than_one, "FLATFORM"].apply(lambda x: x[0])
    df_first_to_second_purchase.loc[lst_purchasedays_greater_than_one, "secondFLATFORM"] = df_first_to_second_purchase.loc[lst_purchasedays_greater_than_one, "FLATFORM"].apply(lambda x: x[1])

    df_first_to_second_purchase.drop(["RegdatesTime", "FLATFORM", "purchaseDays"], axis=1, inplace=True)
    df_first_to_second_purchase["FLATFORM"] = np.nan
    df_first_to_second_purchase["firstToSecondFLATFORM"] = np.nan
    df_first_to_second_purchase.loc[lst_purchasedays_equals_to_one, "FLATFORM"] = df_first_to_second_purchase.loc[lst_purchasedays_equals_to_one, "firstFLATFORM"]
    df_first_to_second_purchase.loc[lst_purchasedays_greater_than_one, "FLATFORM"] = df_first_to_second_purchase.loc[lst_purchasedays_greater_than_one, "firstFLATFORM"]
    df_first_to_second_purchase.loc[lst_purchasedays_greater_than_one, "firstToSecondFLATFORM"] = df_first_to_second_purchase.loc[lst_purchasedays_greater_than_one, "firstFLATFORM"] + " > " + df_first_to_second_purchase.loc[lst_purchasedays_greater_than_one, "secondFLATFORM"]
    df_first_to_second_purchase.drop(["firstFLATFORM", "secondFLATFORM"], axis=1, inplace=True)

    print("Creating time between first and second purchase dataframe..")
    rows = df_first_to_second_purchase.shape[0]
    df_first_to_second_purchase = pd.merge(df_first_to_second_purchase, df_register_date, on=["USERNO"], how="left")
    if rows != df_first_to_second_purchase.shape[0]:
        raise ValueError("Need to check unique values in df_register_date..")
    else:
        del rows
        df_first_to_second_purchase.drop(["USERID", "USERKIND"], axis=1, inplace=True)

    df_first_to_second_purchase["firstPurchaseTime"] = pd.to_datetime(df_first_to_second_purchase["firstRegdatesTime"]) - pd.to_datetime(df_first_to_second_purchase["JOINDATE"])
    df_first_to_second_purchase["firstPurchaseTime"] = df_first_to_second_purchase["firstPurchaseTime"].dt.total_seconds()
    ind_join_date = [x.strftime("%Y-%m-%d") for x in pd.date_range(start="2020-03-06", end=datetime.today().strftime("%Y-%m-%d"), freq="D")]
    ind_join_date = "|".join(ind_join_date)
    ind_join_date = df_first_to_second_purchase[df_first_to_second_purchase["JOINDATE"].str.contains("{}".format(ind_join_date))].index.tolist()
    df_first_to_second_purchase["DIV_JOINDATE"] = np.nan
    df_first_to_second_purchase.loc[ind_join_date, "DIV_JOINDATE"] = "2020-03-06 이후"
    df_first_to_second_purchase["DIV_JOINDATE"].fillna("2020-03-05 이전", inplace=True)
    del ind_join_date
    df_first_to_second_purchase["firstToSecondPurchaseTime"] = df_first_to_second_purchase["secondRegdatesTime"].copy() - df_first_to_second_purchase["firstRegdatesTime"].copy()
    df_first_to_second_purchase["firstToSecondPurchaseTime"] = df_first_to_second_purchase["firstToSecondPurchaseTime"].dt.total_seconds()
    df_first_to_second_purchase["USERKIND_CAT"] = df_first_to_second_purchase["USERKIND_CAT"].map({"소셜회원": "member", "하이클래스": "hiclassmember"})
    if df_first_to_second_purchase[df_first_to_second_purchase["USERKIND_CAT"].isnull()].shape[0] != 0:
        raise ValueError("There are null values in USERKIND_CAT column after mapping..")
    if df_first_to_second_purchase[df_first_to_second_purchase["DIV_MEMBER"] != df_first_to_second_purchase["USERKIND_CAT"]].shape[0] != 0:
        raise ValueError("The values in DIV_MEMBER and USERKIND_CAT do not match..")
    else:
        df_first_to_second_purchase.drop(["USERKIND_CAT"], axis=1, inplace=True)
    df_first_to_second_purchase["secondPURCHASER"] = np.nan
    lst_ind_second_purchaser = df_first_to_second_purchase[df_first_to_second_purchase["secondRegdatesTime"].notnull()].index.tolist()
    df_first_to_second_purchase.loc[lst_ind_second_purchaser, "secondPURCHASER"] = "y"
    df_first_to_second_purchase["secondPURCHASER"].fillna("n", inplace=True)

    df_first_to_second_purchase = df_first_to_second_purchase[['DIV_MEMBER', 'USERNO', 'secondPURCHASER', 'JOINDATE', 'DIV_JOINDATE',
                                                               'LEVELIDX_GUBUN_T_MEMBER', 'GENDER_T_MEMBER',
                                                               'AGE_BIN_T_MEMBER', 'AGE_BIN_ENG_T_MEMBER', 'FLATFORM',
                                                               'firstToSecondFLATFORM', 'firstRegdatesTime', 'secondRegdatesTime',
                                                               'firstPurchaseTime', 'firstToSecondPurchaseTime']]
    df_first_to_second_purchase.columns = [x.upper() for x in df_first_to_second_purchase.columns]
    del df_register_date
    df_first_to_second_purchase["DIV_MEMBER"] = df_first_to_second_purchase["DIV_MEMBER"].map({"hiclassmember": "하이클래스", "member": "회원"})
    if df_first_to_second_purchase[df_first_to_second_purchase["DIV_MEMBER"].isnull()].shape[0] != 0:
        raise ValueError("There are null values in DIV_MEMBER after mapping..")
    col_ind_levelidxgubuntmember = df_first_to_second_purchase.columns.tolist().index("LEVELIDX_GUBUN_T_MEMBER")
    df_first_to_second_purchase.insert(col_ind_levelidxgubuntmember, "GUBUN", df_first_to_second_purchase["LEVELIDX_GUBUN_T_MEMBER"].copy().apply(lambda x: x.split("_")[1]))
    df_first_to_second_purchase.insert(col_ind_levelidxgubuntmember, "LEVELIDX", df_first_to_second_purchase["LEVELIDX_GUBUN_T_MEMBER"].copy().apply(lambda x: x.split("_")[0]))
    df_first_to_second_purchase.drop(["LEVELIDX_GUBUN_T_MEMBER", "AGE_BIN_ENG_T_MEMBER"], axis=1, inplace=True)
    del col_ind_levelidxgubuntmember

    df_first_to_second_purchase.rename(columns={"FIRSTREGDATESTIME": "DATE"}, inplace=True)
    ind_date = df_first_to_second_purchase.columns.tolist().index("DATE")
    df_first_to_second_purchase.insert(ind_date+1, "WEEKDAY_DIV", df_first_to_second_purchase["DATE"].copy().astype(str))
    df_first_to_second_purchase.insert(ind_date+1, "WEEKDAY", df_first_to_second_purchase["DATE"].copy().astype(str))
    for col in ["WEEKDAY", "WEEKDAY_DIV"]:
        df_first_to_second_purchase[col] = df_first_to_second_purchase[col].apply(lambda x: x.split(" ")[0])
        df_first_to_second_purchase[col] = df_first_to_second_purchase[col].apply(
            lambda x: datetime(year=int(x.split("-")[0]), month=int(x.split("-")[1]),
                               day=int(x.split("-")[2])).strftime("%A"))
        del col
    df_first_to_second_purchase["WEEKDAY_DIV"] = df_first_to_second_purchase["WEEKDAY_DIV"].map(
        {"Monday": "주중", "Tuesday": "주중", "Wednesday": "주중", "Thursday": "주중", "Friday": "주중",
         "Saturday": "주말", "Sunday": "주말"})
    df_first_to_second_purchase["WEEKDAY"] = df_first_to_second_purchase["WEEKDAY"].map(
        {"Monday": "월요일", "Tuesday": "화요일", "Wednesday": "수요일", "Thursday": "목요일", "Friday": "금요일",
         "Saturday": "토요일", "Sunday": "일요일"})

    for col in [x for x in df_first_to_second_purchase if "_T_MEMBER" in x]:
        df_first_to_second_purchase.rename(columns={col: col.replace("_T_MEMBER", "")}, inplace=True)
        del col
    ####################################################################################################################
    df_active_user = pd.read_excel(file_active_user)
    for val in ["socialmember", "hiclass"]:
        col_drop = [x for x in df_active_user.columns if val in x]
        df_active_user["activeUser_{}".format(val)] = df_active_user[col_drop].sum(axis=1)
        df_active_user["activeUser_{}".format(val)] = df_active_user["activeUser_{}".format(val)].astype(int)
        df_active_user.drop(col_drop, axis=1, inplace=True)
        del val
    if df_active_user["activeUser_total"].sum() != df_active_user["activeUser_socialmember"].sum() + df_active_user["activeUser_hiclass"].sum():
        raise ValueError("Need to check number of active users..")
    df_active_user.columns = [x.upper() for x in df_active_user.columns]
    df_active_user_monthly = df_active_user[df_active_user["DIV"] == "월별"].copy()
    df_active_user_monthly["DATE"] = df_active_user_monthly["DATE"].apply(lambda x: "{}-{}".format(
        x, monthrange(year= int(x.split("-")[0]), month=int(x.split("-")[1]))[1]))
    df_active_user_daily = df_active_user[df_active_user["DIV"] == "일별"].copy()
    row_df_active_user = df_active_user.shape[0]
    del df_active_user

    print("Creating (전광판용) 1.전일,  2.전주 동일,  3.전월 동일.. (for df_active_user)")
    df_prev_active_user_daily = df_active_user_daily.copy()
    df_prev_active_user_daily["DATE"] = pd.to_datetime(df_prev_active_user_daily["DATE"])
    df_prev_active_user_daily["DATE_PREV_DAY"] = df_prev_active_user_daily["DATE"].apply(lambda x: x - timedelta(days=1))
    df_prev_active_user_daily["DATE_PREV_WEEK"] = df_prev_active_user_daily["DATE"].apply(lambda x: x - timedelta(days=7))
    df_prev_active_user_daily["DATE_PREV_MONTH"] = df_prev_active_user_daily["DATE"].apply(lambda x: x - timedelta(days=30))
    for col in [x for x in df_prev_active_user_daily.columns if "DATE" in x]:
        df_prev_active_user_daily[col] = df_prev_active_user_daily[col].astype(str)
        del col

    df_active_user_daily_prev_day = df_active_user_daily.copy()
    df_active_user_daily_prev_day.columns = ["{}_PREV_DAY".format(x) for x in df_active_user_daily_prev_day.columns]
    df_active_user_daily_prev_day = df_active_user_daily_prev_day.rename(
        columns={"DIV_PREV_DAY": "DIV"})

    df_active_user_daily_prev_week = df_active_user_daily.copy()
    df_active_user_daily_prev_week.columns = ["{}_PREV_WEEK".format(x) for x in df_active_user_daily_prev_week.columns]
    df_active_user_daily_prev_week = df_active_user_daily_prev_week.rename(
        columns={"DIV_PREV_WEEK": "DIV"})

    df_active_user_daily_prev_month = df_active_user_daily.copy()
    df_active_user_daily_prev_month.columns = ["{}_PREV_MONTH".format(x) for x in df_active_user_daily_prev_month.columns]
    df_active_user_daily_prev_month = df_active_user_daily_prev_month.rename(
        columns={"DIV_PREV_MONTH": "DIV"})

    row = df_active_user_daily.shape[0]
    df_active_user_daily = pd.merge(df_prev_active_user_daily, df_active_user_daily,
                                    on=["DIV", "DATE", "ACTIVEUSER_TOTAL", "ACTIVEUSER_SOCIALMEMBER", "ACTIVEUSER_HICLASS"],
                                    how="right")
    del df_prev_active_user_daily
    df_active_user_daily = pd.merge(df_active_user_daily, df_active_user_daily_prev_day,
                                    on=["DIV", "DATE_PREV_DAY"], how="left")
    df_active_user_daily = pd.merge(df_active_user_daily, df_active_user_daily_prev_week,
                                    on=["DIV", "DATE_PREV_WEEK"], how="left")
    df_active_user_daily = pd.merge(df_active_user_daily, df_active_user_daily_prev_month,
                                    on=["DIV", "DATE_PREV_MONTH"], how="left")
    df_active_user_daily.drop([x for x in df_active_user_daily.columns if "DATE" in x and "PREV" in x], axis=1,
                              inplace=True)

    if row != df_active_user_daily.shape[0]:
        raise ValueError("Need to check data after matching day, week, month..")
    else:
        del df_active_user_daily_prev_day, df_active_user_daily_prev_week, df_active_user_daily_prev_month, row
        for col in df_active_user_daily.columns.tolist()[df_active_user_daily.columns.tolist().index("ACTIVEUSER_TOTAL"):]:
            df_active_user_daily[col] = df_active_user_daily[col].fillna(0).astype(int)
            del col

    for dt_div in ["PREV_DAY", "PREV_WEEK", "PREV_MONTH"]:
        for col in [x for x in df_active_user_daily.columns.tolist() if "ACTIVEUSER" in x and
                                                                        "PREV_DAY" not in x and
                                                                        "PREV_WEEK" not in x and
                                                                        "PREV_MONTH" not in x]:
            col_div = "{}_{}".format(col, dt_div)
            df_active_user_daily["{}_CHANGES_RATE_{}".format(col, dt_div)] = (df_active_user_daily[col] - df_active_user_daily["{}_{}".format(col, dt_div)]) / df_active_user_daily["{}_{}".format(col, dt_div)]
            lst_zero = df_active_user_daily[df_active_user_daily["{}".format(col_div)] == 0].index.tolist()
            df_active_user_daily.loc[lst_zero, "{}_CHANGES_RATE_{}".format(col, dt_div)] = np.nan

            result = ""
            if dt_div == "PREV_DAY":
                result += "전일 인입 없음"
            if dt_div == "PREV_WEEK":
                result += "전주 인입 없음"
            if dt_div == "PREV_MONTH":
                result += "30일전 인입 없음"

            df_active_user_daily.loc[lst_zero, "{}_{}_STR".format(col, dt_div)] = result

            ind_pos_changes = df_active_user_daily[
                (df_active_user_daily["{}_CHANGES_RATE_{}".format(col, dt_div)].notnull()) &
                (df_active_user_daily["{}_CHANGES_RATE_{}".format(col, dt_div)] > 0)].index.tolist()
            ind_no_changes = df_active_user_daily[
                (df_active_user_daily["{}_CHANGES_RATE_{}".format(col, dt_div)].notnull()) &
                (df_active_user_daily["{}_CHANGES_RATE_{}".format(col, dt_div)] == 0)].index.tolist()
            ind_neg_changes = df_active_user_daily[
                (df_active_user_daily["{}_CHANGES_RATE_{}".format(col, dt_div)].notnull()) &
                (df_active_user_daily["{}_CHANGES_RATE_{}".format(col, dt_div)] < 0)].index.tolist()

            df_active_user_daily.loc[ind_neg_changes, "{}_CHANGES_RATE_{}".format(col, dt_div)] = -1 * df_active_user_daily.loc[
                ind_neg_changes, "{}_CHANGES_RATE_{}".format(col, dt_div)]

            df_active_user_daily.loc[ind_pos_changes, "{}_{}_STR".format(col, dt_div)] = "상승"
            df_active_user_daily.loc[ind_no_changes, "{}_{}_STR".format(col, dt_div)] = "변동 없음"
            df_active_user_daily.loc[ind_neg_changes, "{}_{}_STR".format(col, dt_div)] = "하락"

            del col, col_div, lst_zero, result, ind_pos_changes, ind_no_changes, ind_neg_changes
        del dt_div
    df_active_user_daily.insert(2, "WEEKDAY_DIV", df_active_user_daily["DATE"].copy())
    df_active_user_daily.insert(2, "WEEKDAY", df_active_user_daily["DATE"].copy())
    for col in ["WEEKDAY", "WEEKDAY_DIV"]:
        df_active_user_daily[col] = df_active_user_daily[col].apply(
            lambda x: datetime(year=int(x.split("-")[0]), month=int(x.split("-")[1]), day=int(x.split("-")[2])).strftime(
                "%A"))
        del col
    df_active_user_daily["WEEKDAY_DIV"] = df_active_user_daily["WEEKDAY_DIV"].map(
        {"Monday": "주중", "Tuesday": "주중", "Wednesday": "주중", "Thursday": "주중", "Friday": "주중",
         "Saturday": "주말", "Sunday": "주말"})
    df_active_user_daily["WEEKDAY"] = df_active_user_daily["WEEKDAY"].map(
        {"Monday": "월요일", "Tuesday": "화요일", "Wednesday": "수요일", "Thursday": "목요일", "Friday": "금요일",
         "Saturday": "토요일", "Sunday": "일요일"})

    df_active_user_total_daily_main_board = df_active_user_daily.copy()
    df_active_user_total_daily_main_board = df_active_user_total_daily_main_board[df_active_user_total_daily_main_board["DATE"].str.contains(str(update_year_main_board))].reset_index(drop=True)
    df_active_user_total_daily_main_board.drop(["DIV"], axis=1, inplace=True)
    df_active_user_daily = df_active_user_daily[[x for x in df_active_user_daily.columns if "PREV" not in x]]

    df_active_user = pd.concat([df_active_user_daily, df_active_user_monthly], axis=0, ignore_index=True)
    if row_df_active_user != df_active_user.shape[0]:
        raise ValueError("row_df_active_user and df_active_user.shape[0] (which is modified) are not equal..")
    del df_active_user_daily, df_active_user_monthly

    df_paid_user_total = pd.read_excel(file_paid_user_total)
    df_paid_user_total.columns = [x.upper() for x in df_paid_user_total.columns]

    df_paid_user_div = pd.read_excel(file_paid_user_div)
    df_paid_user_div.columns = [x.upper() for x in df_paid_user_div.columns]
    check_paid_user_div = df_paid_user_div[["DIV_MEMBER", "DIV", "DATE", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM", "PAYMENT", "PAYMENTCONFIRM"]].copy()
    df_paid_user_div.drop(["AGE_BIN_ENG_T_MEMBER"], axis=1, inplace=True)
    for col in [x for x in df_paid_user_div.columns if "T_MEMBER" in x]:
        df_paid_user_div.rename(columns={col: col.replace("_T_MEMBER", "")}, inplace=True)
        del col
    if df_paid_user_div[df_paid_user_div["LEVELIDX_GUBUN_FLATFORM"].isnull()].shape[0] != 0:
        raise ValueError("There should be no null values in LEVELIDX_GUBUN_FLATFORM column..")

    lst_ind_levelidxgubunflatform_notnull = df_paid_user_div[df_paid_user_div["LEVELIDX_GUBUN_FLATFORM"].notnull()].index.tolist()
    df_paid_user_div.loc[lst_ind_levelidxgubunflatform_notnull, "LEVELIDX"] = df_paid_user_div.loc[lst_ind_levelidxgubunflatform_notnull, "LEVELIDX_GUBUN_FLATFORM"].apply(lambda x: x.split("_")[0])
    df_paid_user_div.loc[lst_ind_levelidxgubunflatform_notnull, "GUBUN"] = df_paid_user_div.loc[lst_ind_levelidxgubunflatform_notnull, "LEVELIDX_GUBUN_FLATFORM"].apply(lambda x: x.split("_")[1])
    df_paid_user_div.loc[lst_ind_levelidxgubunflatform_notnull, "FLATFORM"] = df_paid_user_div.loc[lst_ind_levelidxgubunflatform_notnull, "LEVELIDX_GUBUN_FLATFORM"].apply(lambda x: x.split("_")[2])
    for col in ["LEVELIDX", "GUBUN", "FLATFORM"]:
        if df_paid_user_div[df_paid_user_div[col].isnull()].shape[0] != 0:
            raise ValueError("There should be no null values in {} column..".format(col))
        del col
    lst_ind_levelidxgubunflatform_isnull = df_paid_user_div[(df_paid_user_div["LEVELIDX"] == "비회원") | (df_paid_user_div["GUBUN"] == "비회원")].index.tolist()
    for col in ["GENDER", "AGE_BIN"]:
        df_paid_user_div.loc[lst_ind_levelidxgubunflatform_isnull, col] = "비회원"
        if df_paid_user_div[df_paid_user_div[col].isnull()].shape[0] != 0:
            raise ValueError("There are null values in {} column..".format(col))
        del col
    df_paid_user_div.drop(["LEVELIDX_GUBUN_FLATFORM"], axis=1, inplace=True)
    del lst_ind_levelidxgubunflatform_notnull, lst_ind_levelidxgubunflatform_isnull
    df_paid_user_div = df_paid_user_div[['DIV_MEMBER', 'DIV', 'DATE', 'WEEKDAY', 'WEEKDAYDIV',
                                         'FLATFORM', 'LEVELIDX', 'GUBUN', 'GENDER', 'AGE_BIN', 'PAYMENT',
                                         'CANCELPAYMENT', 'PAYMENTCONFIRM', 'SETTLEPRICE', 'PAIDUSER', '재구매_Y', '재구매_N']]

    temp_paid_user_total = df_paid_user_total[df_paid_user_total['DIV'] == '일별'].copy()
    temp_paid_user_div = df_paid_user_div[df_paid_user_div['DIV'] == '일별'].copy()
    print("Checking number of PAIDUSERs..")
    if temp_paid_user_total["PAIDUSER_MEMBER"].sum() != temp_paid_user_div[temp_paid_user_div["DIV_MEMBER"] != "nomember"]["PAIDUSER"].sum():
        raise ValueError("PAIRDUSER_MEMBER in total dataframe and PAIDUSER in div dataframe(member) is not equal.. It should be equal..")
    if temp_paid_user_total["PAIDUSER_NOMEMBER"].sum() != temp_paid_user_div[temp_paid_user_div["DIV_MEMBER"] == "nomember"]["PAIDUSER"].sum():
        raise ValueError("PAIRDUSER_NOMEMBER in total dataframe and PAIDUSER in div dataframe(nomember) is not equal.. It should be equal..")
    print("Checking number of 'PAYMENT', 'CANCELPAYMENT', 'PAYMENTCONFIRM', 'SETTLEPRICE'..")
    for col in ['PAYMENT', 'CANCELPAYMENT', 'PAYMENTCONFIRM', 'SETTLEPRICE']:
        temp_paid_user_total = df_paid_user_total[df_paid_user_total['DIV'] == '일별'].copy()
        temp_paid_user_div = df_paid_user_div[df_paid_user_div['DIV'] == '일별'].copy()
        if temp_paid_user_total[col].sum() != temp_paid_user_div[col].sum():
            raise ValueError("column: {} // temp_paid_user_total[col].sum(): {} // temp_paid_user_div[col].sum(): {}".format(
                col, temp_paid_user_total[col].sum(), temp_paid_user_div[col].sum()))
        del col
    del temp_paid_user_total, temp_paid_user_div
    print("DO NOT FORGET!!!\nNumber of 1. (재구매_Y_total & 재구매_Y_div) and 2. (재구매_N_total & 재구매_N_div) could be different!\n[_total] has no flatform columns but [_div] has flatform column!!")

    print("Creating (전광판용) 1.전일,  2.전주 동일,  3.전월 동일..")
    df_prev_paid_user_total = df_paid_user_total[df_paid_user_total["DIV"] == "일별"][["DIV", "DATE"]].copy()
    df_prev_paid_user_total["DATE"] = pd.to_datetime(df_prev_paid_user_total["DATE"])
    df_prev_paid_user_total["DATE_PREV_DAY"] = df_prev_paid_user_total["DATE"].apply(lambda x: x - timedelta(days=1))
    df_prev_paid_user_total["DATE_PREV_WEEK"] = df_prev_paid_user_total["DATE"].apply(lambda x: x - timedelta(days=7))
    df_prev_paid_user_total["DATE_PREV_MONTH"] = df_prev_paid_user_total["DATE"].apply(lambda x: x - timedelta(days=30))
    for col in [x for x in df_prev_paid_user_total.columns if "DATE" in x]:
        df_prev_paid_user_total[col] = df_prev_paid_user_total[col].astype(str)
        del col

    col_paid_user_total = ['DIV', 'DATE', 'WEEKDAY', 'PAIDUSER_TOTAL', 'SETTLEPRICE', 'PAYMENT', 'CANCELPAYMENT', 'PAYMENTCONFIRM', 'CANCELLATIONRATIO']
    df_paid_user_total_daily = df_paid_user_total[df_paid_user_total["DIV"] == "일별"][col_paid_user_total].copy()
    df_paid_user_total_monthly = df_paid_user_total[df_paid_user_total["DIV"] == "월별"][col_paid_user_total].copy()
    if df_paid_user_total.shape[0] != df_paid_user_total_daily.shape[0] + df_paid_user_total_monthly.shape[0]:
        raise ValueError("Need to check after splitting df_paid_user_total into df_paid_user_total_daily and df_paid_user_total_monthly..")
    else:
        del df_paid_user_total

    df_paid_user_total_daily_prev_day = df_paid_user_total_daily.copy()
    df_paid_user_total_daily_prev_day.columns = ["{}_PREV_DAY".format(x) for x in df_paid_user_total_daily_prev_day.columns]
    df_paid_user_total_daily_prev_day = df_paid_user_total_daily_prev_day.rename(
        columns={"DIV_PREV_DAY": "DIV"}).drop(["WEEKDAY_PREV_DAY", "PAIDUSER_TOTAL_PREV_DAY", "CANCELLATIONRATIO_PREV_DAY"], axis=1)

    df_paid_user_total_daily_prev_week = df_paid_user_total_daily.copy()
    df_paid_user_total_daily_prev_week.columns = ["{}_PREV_WEEK".format(x) for x in df_paid_user_total_daily_prev_week.columns]
    df_paid_user_total_daily_prev_week = df_paid_user_total_daily_prev_week.rename(
        columns={"DIV_PREV_WEEK": "DIV"}).drop(["WEEKDAY_PREV_WEEK", "PAIDUSER_TOTAL_PREV_WEEK", "CANCELLATIONRATIO_PREV_WEEK"], axis=1)

    df_paid_user_total_daily_prev_month = df_paid_user_total_daily.copy()
    df_paid_user_total_daily_prev_month.columns = ["{}_PREV_MONTH".format(x) for x in df_paid_user_total_daily_prev_month.columns]
    df_paid_user_total_daily_prev_month = df_paid_user_total_daily_prev_month.rename(
        columns={"DIV_PREV_MONTH": "DIV"}).drop(["WEEKDAY_PREV_MONTH", "PAIDUSER_TOTAL_PREV_MONTH", "CANCELLATIONRATIO_PREV_MONTH"], axis=1)

    row = df_paid_user_total_daily.shape[0]
    df_paid_user_total_daily = pd.merge(df_prev_paid_user_total, df_paid_user_total_daily, on=["DIV", "DATE"], how="right")
    del df_prev_paid_user_total
    df_paid_user_total_daily = pd.merge(df_paid_user_total_daily, df_paid_user_total_daily_prev_day,
                                        on=["DIV", "DATE_PREV_DAY"], how="left")
    df_paid_user_total_daily = pd.merge(df_paid_user_total_daily, df_paid_user_total_daily_prev_week,
                                        on=["DIV", "DATE_PREV_WEEK"], how="left")
    df_paid_user_total_daily = pd.merge(df_paid_user_total_daily, df_paid_user_total_daily_prev_month,
                                        on=["DIV", "DATE_PREV_MONTH"], how="left")
    df_paid_user_total_daily.drop([x for x in df_paid_user_total_daily.columns if "DATE" in x and "PREV" in x], axis=1,
                                  inplace=True)

    if row != df_paid_user_total_daily.shape[0]:
        raise ValueError("Need to check data after matching day, week, month..")
    else:
        del df_paid_user_total_daily_prev_day, df_paid_user_total_daily_prev_week, df_paid_user_total_daily_prev_month, row

    for dt_div in ["PREV_DAY", "PREV_WEEK", "PREV_MONTH"]:
        for col in ["SETTLEPRICE", "PAYMENT", "CANCELPAYMENT", "PAYMENTCONFIRM"]:
            col_div = "{}_{}".format(col, dt_div)
            df_paid_user_total_daily[col_div] = df_paid_user_total_daily[col_div].fillna(0).astype(int)
            df_paid_user_total_daily["{}_CHANGES_RATE_{}".format(col, dt_div)] = (df_paid_user_total_daily[col] - df_paid_user_total_daily[col_div]) / df_paid_user_total_daily[col_div]

            lst_zero = df_paid_user_total_daily[df_paid_user_total_daily["{}".format(col_div)] == 0].index.tolist()
            df_paid_user_total_daily.loc[lst_zero, "{}_CHANGES_RATE_{}".format(col, dt_div)] = np.nan

            result = ""
            if dt_div == "PREV_DAY":
                result += "전일"
            if dt_div == "PREV_WEEK":
                result += "전주"
            if dt_div == "PREV_MONTH":
                result += "30일전"
            if col == "SETTLEPRICE":
                result += " 매출"
            if col == "PAYMENT":
                result += " 구매 건수"
            if col == "CANCELPAYMENT":
                result += " 구매 취소 건수"
            if col == "PAYMENTCONFIRM":
                result += " 구매 확정 건수"
            result += " 없음"

            df_paid_user_total_daily.loc[lst_zero, "{}_{}_STR".format(col, dt_div)] = result

            ind_pos_changes = df_paid_user_total_daily[
                (df_paid_user_total_daily["{}_CHANGES_RATE_{}".format(col, dt_div)].notnull()) &
                (df_paid_user_total_daily["{}_CHANGES_RATE_{}".format(col, dt_div)] > 0)].index.tolist()
            ind_no_changes = df_paid_user_total_daily[
                (df_paid_user_total_daily["{}_CHANGES_RATE_{}".format(col, dt_div)].notnull()) &
                (df_paid_user_total_daily["{}_CHANGES_RATE_{}".format(col, dt_div)] == 0)].index.tolist()
            ind_neg_changes = df_paid_user_total_daily[
                (df_paid_user_total_daily["{}_CHANGES_RATE_{}".format(col, dt_div)].notnull()) &
                (df_paid_user_total_daily["{}_CHANGES_RATE_{}".format(col, dt_div)] < 0)].index.tolist()

            df_paid_user_total_daily.loc[ind_neg_changes, "{}_CHANGES_RATE_{}".format(col, dt_div)] = -1 * df_paid_user_total_daily.loc[
                ind_neg_changes, "{}_CHANGES_RATE_{}".format(col, dt_div)]

            df_paid_user_total_daily.loc[ind_pos_changes, "{}_{}_STR".format(col, dt_div)] = "상승"
            df_paid_user_total_daily.loc[ind_no_changes, "{}_{}_STR".format(col, dt_div)] = "변동 없음"
            df_paid_user_total_daily.loc[ind_neg_changes, "{}_{}_STR".format(col, dt_div)] = "하락"
            #####
            del col, col_div, lst_zero, result, ind_pos_changes, ind_no_changes, ind_neg_changes
        del dt_div

    df_paid_user_total_daily = df_paid_user_total_daily[
        ['DIV', 'DATE', 'WEEKDAY', 'PAIDUSER_TOTAL',
         'SETTLEPRICE', 'SETTLEPRICE_PREV_DAY_STR', 'SETTLEPRICE_PREV_DAY', 'SETTLEPRICE_CHANGES_RATE_PREV_DAY', 'SETTLEPRICE_PREV_WEEK_STR', 'SETTLEPRICE_PREV_WEEK', 'SETTLEPRICE_CHANGES_RATE_PREV_WEEK', 'SETTLEPRICE_PREV_MONTH_STR', 'SETTLEPRICE_PREV_MONTH', 'SETTLEPRICE_CHANGES_RATE_PREV_MONTH',
         'PAYMENT', 'PAYMENT_PREV_DAY_STR', 'PAYMENT_PREV_DAY', 'PAYMENT_CHANGES_RATE_PREV_DAY', 'PAYMENT_PREV_WEEK_STR', 'PAYMENT_PREV_WEEK', 'PAYMENT_CHANGES_RATE_PREV_WEEK', 'PAYMENT_PREV_MONTH_STR', 'PAYMENT_PREV_MONTH', 'PAYMENT_CHANGES_RATE_PREV_MONTH',
         'PAYMENTCONFIRM', 'PAYMENTCONFIRM_PREV_DAY_STR', 'PAYMENTCONFIRM_PREV_DAY', 'PAYMENTCONFIRM_CHANGES_RATE_PREV_DAY', 'PAYMENTCONFIRM_PREV_WEEK_STR', 'PAYMENTCONFIRM_PREV_WEEK', 'PAYMENTCONFIRM_CHANGES_RATE_PREV_WEEK', 'PAYMENTCONFIRM_PREV_MONTH_STR', 'PAYMENTCONFIRM_PREV_MONTH', 'PAYMENTCONFIRM_CHANGES_RATE_PREV_MONTH',
         'CANCELPAYMENT', 'CANCELPAYMENT_PREV_DAY_STR', 'CANCELPAYMENT_PREV_DAY', 'CANCELPAYMENT_CHANGES_RATE_PREV_DAY', 'CANCELPAYMENT_PREV_WEEK_STR', 'CANCELPAYMENT_PREV_WEEK', 'CANCELPAYMENT_CHANGES_RATE_PREV_WEEK', 'CANCELPAYMENT_PREV_MONTH_STR', 'CANCELPAYMENT_PREV_MONTH', 'CANCELPAYMENT_CHANGES_RATE_PREV_MONTH',
         'CANCELLATIONRATIO']]

    df_paid_user_total_daily = pd.merge(df_active_user[["DIV", "DATE", "WEEKDAY", "WEEKDAY_DIV", "ACTIVEUSER_TOTAL"]].copy(), df_paid_user_total_daily,
                                        on=["DIV", "DATE", "WEEKDAY"], how="right")
    ind_active_user = df_paid_user_total_daily.columns.tolist().index("ACTIVEUSER_TOTAL")+1
    print("Creating additional KPIs (1. PAIDUSERRATE, 2. ARPPU, 3. ARPDAU)..")
    df_paid_user_total_daily.insert(ind_active_user, "ARPDAU", df_paid_user_total_daily["SETTLEPRICE"] / df_paid_user_total_daily["ACTIVEUSER_TOTAL"])
    df_paid_user_total_daily.insert(ind_active_user, "ARPPU", df_paid_user_total_daily["SETTLEPRICE"] / df_paid_user_total_daily["PAIDUSER_TOTAL"])
    df_paid_user_total_daily.insert(ind_active_user, "PAIDUSERRATE", df_paid_user_total_daily["PAIDUSER_TOTAL"] / df_paid_user_total_daily["ACTIVEUSER_TOTAL"])

    if df_paid_user_total_daily["SETTLEPRICE"].sum() != df_paid_user_div[df_paid_user_div["DIV"] == "일별"]["SETTLEPRICE"].sum():
        raise ValueError('SETTLEPRICE in df_paid_user_total_daily and df_paid_user_div[df_paid_user_div["DIV"] == "일별"]["SETTLEPRICE"].sum() is not equal..')
    if df_paid_user_total_monthly["SETTLEPRICE"].sum() != df_paid_user_total_daily["SETTLEPRICE"].sum():
        raise ValueError("SETTLEPRICE in df_paid_user_total_monthly and df_paid_user_total_daily is not equal..")
    if df_paid_user_total_monthly["SETTLEPRICE"].sum() != df_paid_user_div[df_paid_user_div["DIV"] == "월별"]["SETTLEPRICE"].sum():
        raise ValueError('SETTLEPRICE in df_paid_user_total_monthly and df_paid_user_div[df_paid_user_div["DIV"] == "월별"]["SETTLEPRICE"].sum() is not equal..')
    else:
        del df_paid_user_total_monthly
        df_paid_user_div = df_paid_user_div[df_paid_user_div["DIV"] == "일별"].copy()

    df_paid_user_total_daily_main_board = df_paid_user_total_daily.copy()
    col_final_df_paid_user = ['ACTIVEUSER_TOTAL', 'PAIDUSERRATE', 'ARPPU', 'ARPDAU', 'PAIDUSER_TOTAL']
    df_paid_user_total_daily_main_board.drop(col_final_df_paid_user, axis=1, inplace=True)
    df_paid_user_total_daily = df_paid_user_total_daily[[x for x in df_paid_user_total_daily.columns if "PREV" not in x]]
    df_paid_user_total_daily_main_board = df_paid_user_total_daily_main_board[df_paid_user_total_daily_main_board["DATE"].str.contains(str(update_year_main_board))].reset_index(drop=True)
    df_paid_user_total_daily_main_board.drop(["DIV"], axis=1, inplace=True)

    df_total_engagement_rate = homelearnfriendsmall_total_engagement_rate(dbname=dbname)
    df_total_engagement_rate.drop(["DIV"], axis=1, inplace=True)
    df_total_engagement_rate.rename(columns={"REGDATE": "DATE"}, inplace=True)
    df_total_engagement_rate.columns = [x.upper() for x in df_total_engagement_rate.columns]

    df_total_engagement_rate_weekdayactiveuser = pd.DataFrame()
    for col in [x for x in df_total_engagement_rate.columns if "WEEKDAYACTIVEUSER" in x]:
        temp = df_total_engagement_rate[["YYYYMM", "DATE", "WEEKDAY", "TOTALAU_TOTAL", col]].copy().rename(
            columns={col: "VALUE"})
        temp["DIV"] = col
        temp = temp[['YYYYMM', 'DATE', 'WEEKDAY', 'DIV', 'TOTALAU_TOTAL', 'VALUE']]
        df_total_engagement_rate_weekdayactiveuser = df_total_engagement_rate_weekdayactiveuser.append(temp, sort=False, ignore_index=True)
        del col, temp
    df_total_engagement_rate_weekdayengagementrate = pd.DataFrame()
    for col in [x for x in df_total_engagement_rate.columns if "WEEKDAYENGAGEMENTRATE" in x]:
        temp = df_total_engagement_rate[["YYYYMM", "DATE", "WEEKDAY", "TOTALAU_TOTAL", col]].copy().rename(
            columns={col: "ENGAGEMENTRATE"})
        temp["DIV"] = col.replace("ENGAGEMENTRATE", "ACTIVEUSER")
        df_total_engagement_rate_weekdayengagementrate = df_total_engagement_rate_weekdayengagementrate.append(temp, sort=False, ignore_index=True)
        del temp

    df_engagement = pd.merge(df_total_engagement_rate_weekdayactiveuser, df_total_engagement_rate_weekdayengagementrate,
                             on=["YYYYMM", "DATE", "DIV", "WEEKDAY", "TOTALAU_TOTAL"], how="outer")
    del df_total_engagement_rate_weekdayactiveuser, df_total_engagement_rate_weekdayengagementrate
    df_engagement["check"] = df_engagement["VALUE"] / df_engagement["TOTALAU_TOTAL"]
    if df_engagement[df_engagement["ENGAGEMENTRATE"] != df_engagement["check"]].shape[0] != 0:
        raise ValueError("Need to check df_engagement before exporting..")
    else:
        df_engagement.drop(["check"], axis=1, inplace=True)
        del df_total_engagement_rate

        df_engagement["DIV_MEMBER"] = df_engagement["DIV"].str.extract("(TOTAL|SOCIALMEMBER|HICLASS)")
        df_engagement["DIV_MEMBER"] = df_engagement["DIV_MEMBER"].map({"TOTAL": "전체", "SOCIALMEMBER": "회원", "HICLASS": "하이클래스"})
        if df_engagement[df_engagement["DIV_MEMBER"].isnull()].shape[0] != 0:
            raise ValueError("There are null values in DIV_MEMBER after mapping..")
        df_engagement["DIV_AGE"] = df_engagement["DIV"].apply(lambda x: x.split("_")[-1])
        map_age = {'TOTAL': '전체', 'UNAVAILABLE': '미제공', '10UNDER': '10대 미만'}
        for i in range(10, 110, 10):
            map_age.update({"{}S".format(i): "{}대".format(i)})
            del i
        df_engagement["DIV_AGE"] = df_engagement["DIV_AGE"].map(map_age)
        del map_age
        if df_engagement[df_engagement["DIV_AGE"].isnull()].shape[0] != 0:
            raise ValueError("There are null values in DIV_AGE after mapping..")
        df_engagement = df_engagement[['YYYYMM', 'DATE', 'WEEKDAY', 'DIV_MEMBER', 'DIV_AGE', 'TOTALAU_TOTAL', 'VALUE',
                                       'ENGAGEMENTRATE']]

    print("Reading all df_cancellation_list files..")
    df_cancellation_list = pd.DataFrame()
    for file in sorted(glob(folder_homelearnfriendsmall_paid_user)):
        if "cancellation_list" in file:
            temp = import_pickle(file)
            for k in map_df.keys():
                if k in file:
                    temp.insert(0, "DIV_MEMBER", map_df[k])
                del k
            df_cancellation_list = df_cancellation_list.append(temp, sort=False, ignore_index=True)
            del temp
        del file
    if df_cancellation_list[df_cancellation_list["LEVELIDX_GUBUN_T_MEMBER_FLATFORM"].isnull()].shape[0] != 0:
        raise ValueError("There should be no null values in LEVELIDX_GUBUN_T_MEMBER_FLATFORM column in df_cancellation_list..")

    df_cancellation_list = pd.concat([df_cancellation_list[df_cancellation_list["div"] == "일별"].copy(),
                                      df_cancellation_list[df_cancellation_list["div"] == "월별"].copy()], axis=0, ignore_index=True)
    df_cancellation_list.rename(columns={"회원구분": "member", "paymentCount": "payment", "cancelPaymentCount": "cancelPayment"},
                                inplace=True)
    df_cancellation_list.columns = [x.upper() for x in df_cancellation_list.columns]

    df_confirmation_list = pd.DataFrame()
    for val in df_cancellation_list["CANCELSTATUS"].unique():
        if df_cancellation_list[df_cancellation_list["CANCELSTATUS"] == val]["CANCELPAYMENT"].sum() == 0:
            if val == "구매 확정":
                temp_confirmation_list = df_cancellation_list[df_cancellation_list["CANCELSTATUS"] == val].copy()
                df_confirmation_list = df_confirmation_list.append(temp_confirmation_list, sort=False, ignore_index=True)
                del temp_confirmation_list
            df_cancellation_list = df_cancellation_list[df_cancellation_list["CANCELSTATUS"] != val].reset_index(drop=True)
    check_paid_user_div = check_paid_user_div.groupby(["DIV_MEMBER", "DIV", "DATE", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM"]).agg(
        {"PAYMENT": "sum", "PAYMENTCONFIRM": "sum"}).reset_index().rename(
        columns={"PAYMENT": "PAYMENT_PAIDUSER", "PAYMENTCONFIRM": "PAYMENTCONFIRM_PAIDUSER"})
    check_confirmation_list = df_confirmation_list.copy()
    check_confirmation_list["LEVELIDX_GUBUN_T_MEMBER_FLATFORM"].fillna("", inplace=True)
    check_confirmation_list = check_confirmation_list.groupby(["DIV_MEMBER", "DIV", "DATE", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM"]).agg(
        {"PAYMENT": "sum"}).reset_index().rename(
        columns={"PAYMENT": "PAYMENT_CONFIRMATIONLIST"})
    check = pd.merge(check_paid_user_div, check_confirmation_list,
                     on=["DIV_MEMBER", "DIV", "DATE", "LEVELIDX_GUBUN_T_MEMBER_FLATFORM"], how="outer")
    check["PAYMENT_CONFIRMATIONLIST"] = check["PAYMENT_CONFIRMATIONLIST"].fillna(0).astype(int)
    if check[check["PAYMENTCONFIRM_PAIDUSER"] != check["PAYMENT_CONFIRMATIONLIST"]].shape[0] != 0:
        raise ValueError("Some values in PAYMENTCONFIRM in df_paid_user and PAYMENT in df_confirmation_list are different..")
    else:
        del check, check_paid_user_div, check_confirmation_list
        df_confirmation_list.drop(["CANCELPAYMENT"], axis=1, inplace=True)

    df_cancellation_list["DIV_MEMBER"] = df_cancellation_list["DIV_MEMBER"].map(
        {"hiclassmember": "하이클래스", "member": "회원", "nomember": "비회원"})
    df_confirmation_list["DIV_MEMBER"] = df_confirmation_list["DIV_MEMBER"].map(
        {"hiclassmember": "하이클래스", "member": "회원", "nomember": "비회원"})
    if df_cancellation_list[df_cancellation_list["DIV_MEMBER"].isnull()].shape[0] != 0:
        raise ValueError("There are null values in DIV_MEMBER (df_cancellation_list) after mapping..")
    if df_confirmation_list[df_confirmation_list["DIV_MEMBER"].isnull()].shape[0] != 0:
        raise ValueError("There are null values in DIV_MEMBER (df_confirmation_list) after mapping..")

    col_cancel_confirm = "LEVELIDX_GUBUN_T_MEMBER_FLATFORM"
    col_ind_levelidxgubuntmember_cancellation = df_cancellation_list.columns.tolist().index(col_cancel_confirm)
    col_ind_levelidxgubuntmember_confirmation = df_confirmation_list.columns.tolist().index(col_cancel_confirm)

    map_levelidx_gubun = {"LEVELIDX": 0, "GUBUN": 1, "FLATFORM": 2}
    for k in map_levelidx_gubun:
        v = map_levelidx_gubun[k]
        ind_cancellation_notnull = df_cancellation_list[df_cancellation_list[col_cancel_confirm].notnull()].index.tolist()
        ind_confirmation_notnull = df_confirmation_list[df_confirmation_list[col_cancel_confirm].notnull()].index.tolist()

        df_cancellation_list.insert(col_ind_levelidxgubuntmember_cancellation, k,
                                    df_cancellation_list.loc[ind_cancellation_notnull, col_cancel_confirm].copy().apply(lambda x: x.split("_")[v]))
        df_confirmation_list.insert(col_ind_levelidxgubuntmember_confirmation, k,
                                    df_confirmation_list.loc[ind_confirmation_notnull, col_cancel_confirm].copy().apply(lambda x: x.split("_")[v]))
        del k, v, ind_cancellation_notnull, ind_confirmation_notnull
    df_cancellation_list.drop([col_cancel_confirm], axis=1, inplace=True)
    df_confirmation_list.drop([col_cancel_confirm], axis=1, inplace=True)
    del col_cancel_confirm, col_ind_levelidxgubuntmember_cancellation, col_ind_levelidxgubuntmember_confirmation, map_levelidx_gubun

    for col in [x for x in df_cancellation_list.columns if "_T_MEMBER" in x]:
        df_cancellation_list.rename(columns={col: col.replace("_T_MEMBER", "")}, inplace=True)
        del col
    for col in [x for x in df_confirmation_list.columns if "_T_MEMBER" in x]:
        df_confirmation_list.rename(columns={col: col.replace("_T_MEMBER", "")}, inplace=True)
        del col
    for col in ["CLAIMTYPE", "REASON", "REASONDETAIL"]:
        ind_cancellation_null = df_cancellation_list[df_cancellation_list[col] == "NULL"].index.tolist()
        df_cancellation_list.loc[ind_cancellation_null, col] = np.nan
        df_confirmation_list.drop(col, axis=1, inplace=True)
        del col, ind_cancellation_null

    ind_cancellation_nomember = df_cancellation_list[df_cancellation_list["DIV_MEMBER"] == "비회원"].index.tolist()
    ind_confirmation_nomember = df_confirmation_list[df_confirmation_list["DIV_MEMBER"] == "비회원"].index.tolist()
    for col in ["GENDER", "AGE_BIN"]:
        df_cancellation_list.loc[ind_cancellation_nomember, col] = "비회원"
        df_confirmation_list.loc[ind_confirmation_nomember, col] = "비회원"
        if df_cancellation_list[df_cancellation_list[col].isnull()].shape[0] != 0:
            raise ValueError("There are null values in {} column (df_cancellation_list) after mapping..".format(col))
        if df_confirmation_list[df_confirmation_list[col].isnull()].shape[0] != 0:
            raise ValueError("There are null values in {} column (df_confirmation_list) after mapping..".col)
        del col
    df_cancellation_list.drop(["PAYMENT"], axis=1, inplace=True)

    df_cancellation_list = df_cancellation_list[df_cancellation_list["DIV"] == "일별"].reset_index(drop=True).drop(["DIV"], axis=1)
    df_confirmation_list = df_confirmation_list[df_confirmation_list["DIV"] == "일별"].reset_index(drop=True).drop(["DIV"], axis=1)

    df_confirmation_and_cancellation_list = pd.merge(df_confirmation_list, df_cancellation_list,
                                                     on=list(set(df_confirmation_list.columns.tolist()).intersection(df_cancellation_list.columns.tolist())),
                                                     how="outer")
    del df_confirmation_list, df_cancellation_list
    col_confirm_cancal = ['DIV_MEMBER', 'DATE', 'GUBUN', 'LEVELIDX', 'DIV_ADDR', 'SIDO', 'SIGUNGU', 'CLAIMTYPE',
                          'REASON', 'REASONDETAIL', 'CONFIRMSTATUS', 'CANCELSTATUS', 'GOODSNAME', 'GENDER', 'AGE_BIN',
                          'FLATFORM', 'PAYMENT', "CANCELPAYMENT"]
    df_confirmation_and_cancellation_list = df_confirmation_and_cancellation_list[col_confirm_cancal]

    df_paid_user_div.drop(["DIV"], axis=1, inplace=True)
    df_paid_user_total_daily.drop(["DIV"], axis=1, inplace=True)
    df_paid_user_div["DIV_MEMBER"] = df_paid_user_div["DIV_MEMBER"].map(
        {"hiclassmember": "하이클래스", "member": "회원", "nomember": "비회원"})
    if df_paid_user_div[df_paid_user_div["DIV_MEMBER"].isnull()].shape[0] != 0:
        raise ValueError("There are null values in DIV_MEMBER column in df_paid_user_div after mapping..")

    temp_paid_user = df_paid_user_div.drop(["재구매_Y", "재구매_N"], axis=1).rename(columns={"WEEKDAYDIV": "WEEKDAY_DIV"})
    col_agg_temp_paid_user = {"PAYMENT": "sum", "CANCELPAYMENT": "sum", "PAYMENTCONFIRM": "sum", "SETTLEPRICE": "sum", "PAIDUSER": "sum"}
    temp_paid_user_g = temp_paid_user.groupby(["DATE"]).agg(col_agg_temp_paid_user).reset_index()
    for col in col_agg_temp_paid_user.keys():
        if col == "PAIDUSER":
            col_df_paid_user_total_daily = col + "_TOTAL"
            if df_paid_user_total_daily[col_df_paid_user_total_daily].sum() != temp_paid_user[col].sum():
                del col_df_paid_user_total_daily
                raise ValueError("Sum of {}_TOTAL and {} column for in df_paid_user_total_daily and df_paid_user_div..".format(col, col))
        else:
            if df_paid_user_total_daily[col].sum() != temp_paid_user[col].sum():
                print(col, df_paid_user_total_daily[col].sum(), temp_paid_user[col].sum())
                raise ValueError("Sum of {} column for in df_paid_user_total_daily and df_paid_user_div..".format(col))
        del col
    del temp_paid_user_g
    col_temp_paid_user = ['FLATFORM', 'LEVELIDX', 'GUBUN', 'GENDER', 'AGE_BIN', 'PAYMENT', 'CANCELPAYMENT', 'PAYMENTCONFIRM', 'SETTLEPRICE', 'PAIDUSER']
    for col in col_temp_paid_user:
        col_map = col
        if col == "PAIDUSER":
            col_map += "_TOTAL"
        if col != "PAIDUSER":
            col_map = "DIV_" + col_map
        temp_paid_user.rename(columns={col: col_map}, inplace=True)
        del col, col_map
    if len([x for x in temp_paid_user.columns if x not in ["DATE", "WEEKDAY", "WEEKDAY_DIV"]]) != len(col_temp_paid_user + ["DIV_MEMBER"]):
        raise ValueError("Some columns does not have 'DIV_' for prefix (excluding 'DATE', 'WEEKDAY', 'WEEKDAY_DIV')..")
    else:
        del col_temp_paid_user, col_agg_temp_paid_user
        temp_paid_user = temp_paid_user.groupby(["DIV_MEMBER", "DATE", "WEEKDAY", "WEEKDAY_DIV", "DIV_LEVELIDX", "DIV_GUBUN"]).agg(
            {"DIV_PAYMENT": "sum", "DIV_CANCELPAYMENT": "sum", "DIV_PAYMENTCONFIRM": "sum", "DIV_SETTLEPRICE": "sum", "PAIDUSER_TOTAL": "sum"}).reset_index()

        temp_active_user = pd.DataFrame()
        for f in sorted(glob("{}raw/*".format(folder_homelearnfriendsmall_active_user.replace("*", "")))):
            t_append_active_user = import_pickle(f)
            t_append_active_user["USERKIND_CAT"] = t_append_active_user["USERKIND_CAT"].str.replace("소셜회원", "회원")
            t_append_active_user = t_append_active_user.groupby(["REGDATE", "USERKIND_CAT", "LEVELIDX_GUBUN"])["USERNO"].nunique().reset_index().rename(
                columns={"REGDATE": "DATE", "USERKIND_CAT": "DIV_MEMBER"})

            result_userno = t_append_active_user["USERNO"].sum()

            result_active_user_total = df_active_user[df_active_user["DIV"] == "일별"].copy()
            result_active_user_total = result_active_user_total[result_active_user_total["DATE"].str.contains("|".join(t_append_active_user["DATE"].unique().tolist()))]["ACTIVEUSER_TOTAL"].sum()
            if result_userno != result_active_user_total:
                raise ValueError("There is a problem in number of active users..")
            else:
                del result_userno, result_active_user_total
                ind_levelidx_gubun = t_append_active_user.columns.tolist().index("LEVELIDX_GUBUN")
                t_append_active_user.insert(ind_levelidx_gubun, "GUBUN", t_append_active_user["LEVELIDX_GUBUN"].apply(lambda x: x.split("_")[1]))
                t_append_active_user.insert(ind_levelidx_gubun, "LEVELIDX", t_append_active_user["LEVELIDX_GUBUN"].apply(lambda x: x.split("_")[0]))
                t_append_active_user = t_append_active_user.drop(["LEVELIDX_GUBUN"], axis=1).rename(
                    columns={"USERNO": "ACTIVEUSER_TOTAL", "LEVELIDX": "DIV_LEVELIDX", "GUBUN": "DIV_GUBUN"})
                temp_active_user = temp_active_user.append(t_append_active_user, sort=False, ignore_index=True)
                del t_append_active_user
            del f
        temp_active_paid_user = pd.merge(temp_active_user, temp_paid_user, on=["DATE", "DIV_MEMBER", "DIV_LEVELIDX", "DIV_GUBUN"], how="outer")
        if temp_active_paid_user[temp_active_paid_user["DIV_MEMBER"] == "비회원"].shape[0] != temp_paid_user[temp_paid_user["DIV_MEMBER"] == "비회원"].shape[0]:
            raise ValueError("Need to check the shape of 비회원 in DIV_MEMBER..")
        else:
            temp_active_paid_user = temp_active_paid_user[['DIV_MEMBER', 'DATE', 'WEEKDAY', 'WEEKDAY_DIV', 'ACTIVEUSER_TOTAL',
                                                           'DIV_LEVELIDX', 'DIV_GUBUN', 'DIV_PAYMENT', 'DIV_CANCELPAYMENT',
                                                           'DIV_PAYMENTCONFIRM', 'DIV_SETTLEPRICE', 'PAIDUSER_TOTAL']].sort_values(by=["DATE", "DIV_MEMBER"])
            temp_active_paid_user["ACTIVEUSER_TOTAL"] = temp_active_paid_user["ACTIVEUSER_TOTAL"].fillna(0).astype(int)
            if temp_active_paid_user["ACTIVEUSER_TOTAL"].sum() != df_active_user[df_active_user["DIV"] == "일별"]["ACTIVEUSER_TOTAL"].sum():
                raise ValueError("There is a problem in number of active users after filling nulls to zeros..")

            for col in ['DIV_PAYMENT', 'DIV_CANCELPAYMENT', 'DIV_PAYMENTCONFIRM', 'DIV_SETTLEPRICE', 'PAIDUSER_TOTAL']:
                temp_active_paid_user[col] = temp_active_paid_user[col].fillna(0).astype(int)
                col_paid_user_total_daily = col.replace("DIV_", "")
                # if col_paid_user_total_daily == "PAIDUSER":
                #     col_paid_user_total_daily += "_TOTAL"
                if df_paid_user_total_daily[col_paid_user_total_daily].sum() != temp_active_paid_user[col].sum():
                    raise ValueError("Sum of {} column in df_paid_user_total_daily and temp_active_paid_user is not equal..")
                del col, col_paid_user_total_daily
            del temp_active_user, temp_paid_user

            for col in ["WEEKDAY", "WEEKDAY_DIV"]:
                temp_active_paid_user[col] = temp_active_paid_user["DATE"].copy()
                temp_active_paid_user[col] = temp_active_paid_user[col].apply(lambda x: datetime(year=int(x.split("-")[0]), month=int(x.split("-")[1]),
                                                                                                 day=int(x.split("-")[2])).strftime("%A"))
                del col
            temp_active_paid_user["WEEKDAY_DIV"] = temp_active_paid_user["WEEKDAY_DIV"].map(
                {"Monday": "주중", "Tuesday": "주중", "Wednesday": "주중", "Thursday": "주중", "Friday": "주중",
                 "Saturday": "주말", "Sunday": "주말"})
            temp_active_paid_user["WEEKDAY"] = temp_active_paid_user["WEEKDAY"].map(
                {"Monday": "월요일", "Tuesday": "화요일", "Wednesday": "수요일", "Thursday": "목요일", "Friday": "금요일",
                 "Saturday": "토요일", "Sunday": "일요일"})

            if temp_active_paid_user[temp_active_paid_user["DIV_MEMBER"] == "비회원"]["ACTIVEUSER_TOTAL"].sum() > 0:
                raise ValueError("There should be no ACTIVEUSER_TOTAL for DIV_MEMBER == 비회원 in temp_active_paid_user..")

    df_paid_user_total_daily.insert(0, "FILTER", "전체")
    temp_active_paid_user.insert(0, "FILTER", "회원 유형별")
    for col in [x for x in temp_active_paid_user.columns if "DIV_" in x]:
        if col == "DIV_MEMBER":
            continue
        temp_active_paid_user.rename(columns={col: col.replace("DIV_", "")}, inplace=True)

    df_paid_user_total_daily_no_date = sorted(list(set(df_active_user["DATE"].unique().tolist()).difference(df_paid_user_total_daily["DATE"].unique().tolist())))
    df_paid_user_total_daily_no_date_data = {k: [] for k in df_paid_user_total_daily.columns if k not in ["WEEKDAY", "WEEKDAY_DIV"]}
    df_paid_user_total_daily_no_date_data["FILTER"] = ["전체"] * len(df_paid_user_total_daily_no_date)
    df_paid_user_total_daily_no_date_data["DATE"] = df_paid_user_total_daily_no_date
    for col in [k for k in df_paid_user_total_daily_no_date_data.keys() if len(df_paid_user_total_daily_no_date_data[k]) == 0]:
        df_paid_user_total_daily_no_date_data[col] = [0] * len(df_paid_user_total_daily_no_date)
        del col
    del df_paid_user_total_daily_no_date
    df_paid_user_total_daily_no_date = pd.DataFrame(data=df_paid_user_total_daily_no_date_data)
    del df_paid_user_total_daily_no_date_data
    for col in ["WEEKDAY", "WEEKDAY_DIV"]:
        df_paid_user_total_daily_no_date[col] = df_paid_user_total_daily_no_date["DATE"].copy()
        df_paid_user_total_daily_no_date[col] = df_paid_user_total_daily_no_date[col].apply(lambda x: datetime(year=int(x.split("-")[0]), month=int(x.split("-")[1]),
                                                                                                               day=int(x.split("-")[2])).strftime("%A"))
        del col
    df_paid_user_total_daily_no_date["WEEKDAY_DIV"] = df_paid_user_total_daily_no_date["WEEKDAY_DIV"].map(
        {"Monday": "주중", "Tuesday": "주중", "Wednesday": "주중", "Thursday": "주중", "Friday": "주중",
         "Saturday": "주말", "Sunday": "주말"})
    df_paid_user_total_daily_no_date["WEEKDAY"] = df_paid_user_total_daily_no_date["WEEKDAY"].map(
        {"Monday": "월요일", "Tuesday": "화요일", "Wednesday": "수요일", "Thursday": "목요일", "Friday": "금요일",
         "Saturday": "토요일", "Sunday": "일요일"})
    for dt in df_paid_user_total_daily_no_date["DATE"].unique().tolist():
        result = df_active_user[df_active_user["DIV"] == "일별"].copy()
        ind_result = result[result["DATE"] == dt].index.tolist()
        ind_df_paid_user_total_daily_no_date = df_paid_user_total_daily_no_date[df_paid_user_total_daily_no_date["DATE"] == dt].index.tolist()
        series = df_active_user.loc[ind_result, "ACTIVEUSER_TOTAL"]
        if len(series) != 0:
            df_paid_user_total_daily_no_date.loc[ind_df_paid_user_total_daily_no_date, "ACTIVEUSER_TOTAL"] = df_active_user.loc[ind_result, "ACTIVEUSER_TOTAL"].unique()[0]
        del dt, result, ind_result, ind_df_paid_user_total_daily_no_date, series
    df_paid_user_total_daily = pd.concat([df_paid_user_total_daily, df_paid_user_total_daily_no_date], axis=0, ignore_index=True).sort_values(by=["DATE"])
    del df_paid_user_total_daily_no_date

    temp_active_paid_user_no_date = sorted(list(set(df_active_user["DATE"].unique().tolist()).difference(temp_active_paid_user["DATE"].unique().tolist())))
    temp_active_paid_user_no_date_data = {k: [] for k in temp_active_paid_user.columns if k not in ["WEEKDAY", "WEEKDAY_DIV"]}
    col_temp_active_paid_user_g = ["DIV_MEMBER", "LEVELIDX", "GUBUN"]
    temp_active_paid_user_no_date_data_g_leveldix_gubun = temp_active_paid_user.groupby(col_temp_active_paid_user_g)["DATE"].count().reset_index().drop(["DATE"], axis=1)
    final = pd.DataFrame()
    for val in temp_active_paid_user_no_date:
        final_append = temp_active_paid_user_no_date_data_g_leveldix_gubun.copy()
        final_append["DATE"] = val
        final = final.append(final_append, sort=False, ignore_index=True)
        del val, final_append
    del temp_active_paid_user_no_date_data_g_leveldix_gubun, temp_active_paid_user_no_date

    temp_active_paid_user_no_date_data["FILTER"] = ["회원 유형별"] * final.shape[0]
    for col in ["DATE"] + col_temp_active_paid_user_g:
        temp_active_paid_user_no_date_data[col] = final[col].tolist()
        del col
    for col in [k for k in temp_active_paid_user_no_date_data.keys() if len(temp_active_paid_user_no_date_data[k]) == 0]:
        temp_active_paid_user_no_date_data[col] = [0] * final.shape[0]
        del col
    del final
    if len([k for k in temp_active_paid_user_no_date_data.keys() if len(temp_active_paid_user_no_date_data[k]) == 0]) != 0:
        raise ValueError("Need to check data for creating temp_active_paid_user_no_date_data..")
    temp_active_paid_user_no_date = pd.DataFrame(data=temp_active_paid_user_no_date_data)
    del temp_active_paid_user_no_date_data
    for col in ["WEEKDAY", "WEEKDAY_DIV"]:
        temp_active_paid_user_no_date[col] = temp_active_paid_user_no_date["DATE"].copy()
        temp_active_paid_user_no_date[col] = temp_active_paid_user_no_date[col].apply(lambda x: datetime(year=int(x.split("-")[0]), month=int(x.split("-")[1]),
                                                                                                         day=int(x.split("-")[2])).strftime("%A"))
        del col
    temp_active_paid_user_no_date["WEEKDAY_DIV"] = temp_active_paid_user_no_date["WEEKDAY_DIV"].map(
        {"Monday": "주중", "Tuesday": "주중", "Wednesday": "주중", "Thursday": "주중", "Friday": "주중",
         "Saturday": "주말", "Sunday": "주말"})
    temp_active_paid_user_no_date["WEEKDAY"] = temp_active_paid_user_no_date["WEEKDAY"].map(
        {"Monday": "월요일", "Tuesday": "화요일", "Wednesday": "수요일", "Thursday": "목요일", "Friday": "금요일",
         "Saturday": "토요일", "Sunday": "일요일"})
    for dt in temp_active_paid_user_no_date["DATE"].unique().tolist():
        result = df_active_user[df_active_user["DIV"] == "일별"].copy()
        ind_result = result[result["DATE"] == dt].index.tolist()
        ind_df_paid_user_total_daily_no_date = temp_active_paid_user_no_date[temp_active_paid_user_no_date["DATE"] == dt].index.tolist()
        series = df_active_user.loc[ind_result, "ACTIVEUSER_TOTAL"]
        if len(series) != 0:
            df_paid_user_total_daily_no_date.loc[ind_df_paid_user_total_daily_no_date, "ACTIVEUSER_TOTAL"] = df_active_user.loc[ind_result, "ACTIVEUSER_TOTAL"].unique()[0]
        del dt, result, ind_result, ind_df_paid_user_total_daily_no_date, series
    temp_active_paid_user = pd.concat([temp_active_paid_user, temp_active_paid_user_no_date], axis=0, ignore_index=True).sort_values(by=["DATE"])
    del temp_active_paid_user_no_date

    df_paid_user_total_daily = pd.concat([df_paid_user_total_daily, temp_active_paid_user], axis=0, ignore_index=True)
    del temp_active_paid_user
    for col in ["DIV_MEMBER", "LEVELIDX", "GUBUN"]:
        if df_paid_user_total_daily[df_paid_user_total_daily[col].isnull()]["FILTER"].nunique(dropna=False) != 1 or df_paid_user_total_daily[df_paid_user_total_daily[col].isnull()]["FILTER"].unique()[0] != "전체":
            raise ValueError("Need to check {} column after concatenating df_paid_user_total_daily and temp_active_paid_user..")
        else:
            df_paid_user_total_daily[col].fillna(df_paid_user_total_daily[df_paid_user_total_daily[col].isnull()]["FILTER"].unique()[0], inplace=True)
        del col
    df_paid_user_total_daily.drop(["PAIDUSERRATE", "ARPPU", "ARPDAU"], axis=1, inplace=True)

    for col in ["ACTIVEUSER_TOTAL", "PAIDUSER_TOTAL", "SETTLEPRICE", "PAYMENT", "PAYMENTCONFIRM", "CANCELPAYMENT"]:
        result_total = df_paid_user_total_daily[df_paid_user_total_daily["FILTER"] == "전체"].copy()
        result_div = df_paid_user_total_daily[df_paid_user_total_daily["FILTER"] == "회원 유형별"].copy()
        if result_total[col].sum() != result_div[col].sum():
            raise ValueError("{} column is not equal for FILTER == '전체' and FILTER == '회원 유형별' during final check....".format(col))
        del col, result_total, result_div

    file_nm_confirmation_and_cancellation_list = "{}df_confirmation_and_cancellation_list_{}.csv".format(folder_homelearnfriendsmall_database, dbname)
    print("Exporting {}".format(file_nm_confirmation_and_cancellation_list))
    df_confirmation_and_cancellation_list.to_csv(file_nm_confirmation_and_cancellation_list, index=False, encoding="utf-8-sig")
    del file_nm_confirmation_and_cancellation_list, df_confirmation_and_cancellation_list

    file_nm_retention_rate = "{}df_retention_rate_{}.csv".format(folder_homelearnfriendsmall_database, dbname)
    print("Exporting {}".format(file_nm_retention_rate))
    df_retention_rate.to_csv(file_nm_retention_rate, index=False, encoding="utf-8-sig")
    del file_nm_retention_rate, df_retention_rate

    file_nm_first_to_second_purchase = "{}df_first_to_second_purchase_{}.csv".format(folder_homelearnfriendsmall_database, dbname)
    print("Exporting {}".format(file_nm_first_to_second_purchase))
    df_first_to_second_purchase.to_csv(file_nm_first_to_second_purchase, index=False, encoding="utf-8-sig")
    del file_nm_first_to_second_purchase, df_first_to_second_purchase

    file_nm_active_user = "{}df_active_user_{}.csv".format(folder_homelearnfriendsmall_database, dbname)
    print("Exporting {}".format(file_nm_active_user))
    df_active_user.to_csv(file_nm_active_user, index=False, encoding="utf-8-sig")
    del file_nm_active_user, df_active_user

    file_nm_active_user_total_daily_main_board = "{}df_active_user_total_main_board_{}.csv".format(folder_homelearnfriendsmall_database, dbname)
    print("Exporting {}".format(file_nm_active_user_total_daily_main_board))
    df_active_user_total_daily_main_board.to_csv(file_nm_active_user_total_daily_main_board, index=False, encoding="utf-8-sig")
    del file_nm_active_user_total_daily_main_board, df_active_user_total_daily_main_board

    file_nm_engagement = "{}df_engagement_{}.csv".format(folder_homelearnfriendsmall_database, dbname)
    print("Exporting {}".format(file_nm_engagement))
    df_engagement.to_csv(file_nm_engagement, index=False, encoding="utf-8-sig")
    del file_nm_engagement, df_engagement

    file_nm_paid_user_total_daily = "{}df_paid_user_total_daily_{}.csv".format(folder_homelearnfriendsmall_database, dbname)
    print("Exporting {}".format(file_nm_paid_user_total_daily))
    df_paid_user_total_daily.to_csv(file_nm_paid_user_total_daily, index=False, encoding="utf-8-sig")
    del file_nm_paid_user_total_daily, df_paid_user_total_daily

    file_nm_paid_user_div = "{}df_paid_user_div_with_platform_{}.csv".format(folder_homelearnfriendsmall_database, dbname)
    print("Exporting {}".format(file_nm_paid_user_div))
    df_paid_user_div.to_csv(file_nm_paid_user_div, index=False, encoding="utf-8-sig")
    del file_nm_paid_user_div, df_paid_user_div

    file_nm_paid_user_total_daily_main_board = "{}df_paid_user_total_daily_main_board_{}.csv".format(folder_homelearnfriendsmall_database, dbname)
    print("Exporting {}".format(file_nm_paid_user_total_daily_main_board))
    df_paid_user_total_daily_main_board.to_csv(file_nm_paid_user_total_daily_main_board, index=False, encoding="utf-8-sig")
    del file_nm_paid_user_total_daily_main_board, df_paid_user_total_daily_main_board


def upload_whole_database(dbname=[]):
    import pandas as pd
    import numpy as np
    from datetime import timedelta
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    # path_export = file_directory

    df_register_date = pd.DataFrame()

    for db_name in dbname:
        temp_register_date = conestore_total_t_user(dbname=db_name)
        temp_register_date.insert(0, "DBNAME", db_name)
        df_register_date = df_register_date.append(temp_register_date, sort=False, ignore_index=True)
        del temp_register_date, db_name
    df_register_date = df_register_date[(df_register_date["USERKIND"] == "회원") | (df_register_date["USERKIND"] == "비회원")].reset_index(drop=True)
    df_register_date["JOINDATE"] = df_register_date["JOINDATE"].apply(lambda x: x.strftime("%Y-%m-%d"))
    df_register_date = df_register_date.groupby(["DBNAME", "USERKIND", "JOINDATE"])["USERNO"].nunique().reset_index()

    df_register_date_prev_day = df_register_date.copy().rename(columns={"JOINDATE": "JOINDATE_PREV_DAY", "USERNO": "USERNO_PREV_DAY"})
    df_register_date_prev_week = df_register_date.copy().rename(columns={"JOINDATE": "JOINDATE_PREV_WEEK", "USERNO": "USERNO_PREV_WEEK"})
    df_register_date_prev_month = df_register_date.copy().rename(columns={"JOINDATE": "JOINDATE_PREV_MONTH", "USERNO": "USERNO_PREV_MONTH"})

    df_register_date["JOINDATE_PREV_DAY"] = df_register_date["JOINDATE"].apply(lambda x: (pd.to_datetime(x) - timedelta(days=1)).strftime("%Y-%m-%d"))
    df_register_date["JOINDATE_PREV_WEEK"] = df_register_date["JOINDATE"].apply(lambda x: (pd.to_datetime(x) - timedelta(days=7)).strftime("%Y-%m-%d"))
    df_register_date["JOINDATE_PREV_MONTH"] = df_register_date["JOINDATE"].apply(lambda x: (pd.to_datetime(x) - timedelta(days=30)).strftime("%Y-%m-%d"))

    df_register_date = pd.merge(df_register_date, df_register_date_prev_day,
                                on=["DBNAME", "USERKIND", "JOINDATE_PREV_DAY"], how="left")
    df_register_date = pd.merge(df_register_date, df_register_date_prev_week,
                                on=["DBNAME", "USERKIND", "JOINDATE_PREV_WEEK"], how="left")
    df_register_date = pd.merge(df_register_date, df_register_date_prev_month,
                                on=["DBNAME", "USERKIND", "JOINDATE_PREV_MONTH"], how="left")
    del df_register_date_prev_day, df_register_date_prev_week, df_register_date_prev_month
    df_register_date.drop(["JOINDATE_PREV_DAY", "JOINDATE_PREV_WEEK", "JOINDATE_PREV_MONTH"], axis=1, inplace=True)

    for col in ["PREV_DAY", "PREV_WEEK", "PREV_MONTH"]:
        df_register_date["USERNO_CHANGES_RATE_{}".format(col)] = (df_register_date["USERNO"] - df_register_date["USERNO_{}".format(col)]) / df_register_date["USERNO_{}".format(col)]
        del col

    for col in ["PREV_DAY", "PREV_WEEK", "PREV_MONTH"]:
        ind_uniqueuserno_pos_changes = df_register_date[
            (df_register_date["USERNO_CHANGES_RATE_{}".format(col)].notnull()) &
            (df_register_date["USERNO_CHANGES_RATE_{}".format(col)] > 0)].index.tolist()
        ind_uniqueuserno_no_changes = df_register_date[
            (df_register_date["USERNO_CHANGES_RATE_{}".format(col)].notnull()) &
            (df_register_date["USERNO_CHANGES_RATE_{}".format(col)] == 0)].index.tolist()
        ind_uniqueuserno_neg_changes = df_register_date[
            (df_register_date["USERNO_CHANGES_RATE_{}".format(col)].notnull()) &
            (df_register_date["USERNO_CHANGES_RATE_{}".format(col)] < 0)].index.tolist()

        df_register_date.loc[ind_uniqueuserno_neg_changes, "USERNO_CHANGES_RATE_{}".format(col)] = -1 * df_register_date.loc[
            ind_uniqueuserno_neg_changes, "USERNO_CHANGES_RATE_{}".format(col)]

        df_register_date.loc[ind_uniqueuserno_no_changes, "USERNO_CHANGES_RATE_{}".format(col)] = np.nan
        df_register_date["USERNO_CHANGES_RATE_{}_STR".format(col)] = np.nan
        df_register_date.loc[ind_uniqueuserno_pos_changes, "USERNO_CHANGES_RATE_{}_STR".format(col)] = "상승"
        df_register_date.loc[ind_uniqueuserno_no_changes, "USERNO_CHANGES_RATE_{}_STR".format(col)] = "변동 없음"
        df_register_date.loc[ind_uniqueuserno_neg_changes, "USERNO_CHANGES_RATE_{}_STR".format(col)] = "하락"
        del col, ind_uniqueuserno_pos_changes, ind_uniqueuserno_no_changes, ind_uniqueuserno_neg_changes

    f_nm = "{}df_join_date_with_comparison.csv".format(path_export)
    print("Exporting {}".format(f_nm))
    df_register_date.to_csv(f_nm, index=False)
    del f_nm, df_register_date

    df_total_order_sequence = pd.DataFrame()
    df_order_region = pd.DataFrame()
    df_by_join_date = pd.DataFrame()
    df_main = pd.DataFrame()
    df_main_total = pd.DataFrame()
    df_main_total_temp = pd.DataFrame()
    df_cart_info = pd.DataFrame()
    df_first_to_second_purchase_g = pd.DataFrame()
    df_time_to_purchase = pd.DataFrame()
    df_retention_rate = pd.DataFrame()
    df_weekday_engagement_rate = pd.DataFrame()

    for db_name in dbname:
        # file_export_df_by_join_date = file_directory
        # file_export_df_main = file_directory
        # file_export_df_main_total = file_directory
        # file_export_df_main_total_temp = file_directory
        # file_export_df_cart_info = file_directory
        # file_export_df_first_to_second_purchase = file_directory
        # file_export_df_retention_rate = file_directory
        # file_export_df_time_to_purchase = file_directory
        # file_export_df_weekday_engagement_rate = file_directory
        # file_export_df_order_region = file_directory
        # file_export_df_total_order_sequence = file_directory

        temp_df_total_order_sequence = import_pickle(file_name=file_export_df_total_order_sequence)
        temp_df_total_order_sequence.insert(0, "DBNAME", db_name)
        del file_export_df_total_order_sequence
        df_total_order_sequence = df_total_order_sequence.append(temp_df_total_order_sequence, sort=False, ignore_index=True)
        del temp_df_total_order_sequence

        temp_df_order_region = import_pickle(file_name=file_export_df_order_region)
        temp_df_order_region.insert(0, "DBNAME", db_name)
        del file_export_df_order_region
        df_order_region = df_order_region.append(temp_df_order_region, sort=False, ignore_index=True)
        del temp_df_order_region

        temp_df_by_join_date = import_pickle(file_name=file_export_df_by_join_date)
        temp_df_by_join_date.insert(0, "DBNAME", db_name)
        del file_export_df_by_join_date
        df_by_join_date = df_by_join_date.append(temp_df_by_join_date, sort=False, ignore_index=True)
        del temp_df_by_join_date

        temp_df_main_total = import_pickle(file_name=file_export_df_main_total)
        temp_df_main_total.insert(0, "DBNAME", db_name)
        del file_export_df_main_total
        df_main_total = df_main_total.append(temp_df_main_total, sort=False, ignore_index=True)
        del temp_df_main_total

        temp_df_main_total_temp = import_pickle(file_name=file_export_df_main_total_temp)
        temp_df_main_total_temp.insert(0, "DBNAME", db_name)
        del file_export_df_main_total_temp
        df_main_total_temp = df_main_total_temp.append(temp_df_main_total_temp, sort=False, ignore_index=True)
        del temp_df_main_total_temp

        temp_df_main = import_pickle(file_name=file_export_df_main)
        temp_df_main.insert(0, "DBNAME", db_name)
        del file_export_df_main
        df_main = df_main.append(temp_df_main, sort=False, ignore_index=True)
        del temp_df_main

        temp_df_cart_info = import_pickle(file_name=file_export_df_cart_info)
        temp_df_cart_info.insert(0, "DBNAME", db_name)
        del file_export_df_cart_info
        df_cart_info = df_cart_info.append(temp_df_cart_info, sort=False, ignore_index=True)
        del temp_df_cart_info

        temp_df_first_to_second_purchase_g = import_pickle(file_name=file_export_df_first_to_second_purchase)
        temp_df_first_to_second_purchase_g.insert(0, "DBNAME", db_name)
        del file_export_df_first_to_second_purchase
        df_first_to_second_purchase_g = df_first_to_second_purchase_g.append(temp_df_first_to_second_purchase_g, sort=False, ignore_index=True)
        del temp_df_first_to_second_purchase_g

        temp_df_time_to_purchase = import_pickle(file_name=file_export_df_time_to_purchase)
        temp_df_time_to_purchase.insert(0, "DBNAME", db_name)
        del file_export_df_time_to_purchase
        df_time_to_purchase = df_time_to_purchase.append(temp_df_time_to_purchase, sort=False, ignore_index=True)
        del temp_df_time_to_purchase

        temp_df_retention_rate = import_pickle(file_name=file_export_df_retention_rate)
        temp_df_retention_rate.insert(0, "DBNAME", db_name)
        del file_export_df_retention_rate
        df_retention_rate = df_retention_rate.append(temp_df_retention_rate, sort=False, ignore_index=True)
        del temp_df_retention_rate

        temp_df_weekday_engagement_rate = import_pickle(file_name=file_export_df_weekday_engagement_rate)
        temp_df_weekday_engagement_rate.insert(0, "DBNAME", db_name)
        del file_export_df_weekday_engagement_rate
        df_weekday_engagement_rate = df_weekday_engagement_rate.append(temp_df_weekday_engagement_rate, sort=False, ignore_index=True)
        del temp_df_weekday_engagement_rate

        del db_name

    for c in ["ACTIVEUSER", "TOTALORDERPRICE", "PAYMENT", "CANCELPAYMENT"]:
        for col in ["PREV_DAY", "PREV_WEEK", "PREV_MONTH"]:
            ind_pos_changes = df_main_total_temp[
                (df_main_total_temp["{}_CHANGES_RATE_{}".format(c, col)].notnull()) &
                (df_main_total_temp["{}_CHANGES_RATE_{}".format(c, col)] > 0)].index.tolist()
            ind_no_changes = df_main_total_temp[
                (df_main_total_temp["{}_CHANGES_RATE_{}".format(c, col)].notnull()) &
                (df_main_total_temp["{}_CHANGES_RATE_{}".format(c, col)] == 0)].index.tolist()
            ind_neg_changes = df_main_total_temp[
                (df_main_total_temp["{}_CHANGES_RATE_{}".format(c, col)].notnull()) &
                (df_main_total_temp["{}_CHANGES_RATE_{}".format(c, col)] < 0)].index.tolist()

            df_main_total_temp.loc[ind_neg_changes, "{}_CHANGES_RATE_{}".format(c, col)] = -1 * df_main_total_temp.loc[
                ind_neg_changes, "{}_CHANGES_RATE_{}".format(c, col)]

            df_main_total_temp["{}_CHANGES_RATE_{}_STR".format(c, col)] = np.nan
            df_main_total_temp.loc[ind_pos_changes, "{}_CHANGES_RATE_{}_STR".format(c, col)] = "상승"
            df_main_total_temp.loc[ind_no_changes, "{}_CHANGES_RATE_{}_STR".format(c, col)] = "변동 없음"
            df_main_total_temp.loc[ind_neg_changes, "{}_CHANGES_RATE_{}_STR".format(c, col)] = "하락"
            del col, ind_pos_changes, ind_no_changes, ind_neg_changes
        del c

    df_cart_info = df_cart_info[df_cart_info["DIV"] == "일별"].reset_index(drop=True)
    ind_rank_uniqueuserno_pos_changes = df_cart_info[(df_cart_info["RANK_UNIQUEUSERNO_CHANGES"].notnull()) &
                                                     (df_cart_info["RANK_UNIQUEUSERNO_CHANGES"] > 0)].index.tolist()
    ind_rank_uniqueuserno_no_changes = df_cart_info[(df_cart_info["RANK_UNIQUEUSERNO_CHANGES"].notnull()) &
                                                    (df_cart_info["RANK_UNIQUEUSERNO_CHANGES"] == 0)].index.tolist()
    ind_rank_uniqueuserno_neg_changes = df_cart_info[(df_cart_info["RANK_UNIQUEUSERNO_CHANGES"].notnull()) &
                                                     (df_cart_info["RANK_UNIQUEUSERNO_CHANGES"] < 0)].index.tolist()
    ind_rank_ea_pos_changes = df_cart_info[(df_cart_info["RANK_EA_CHANGES"].notnull()) &
                                           (df_cart_info["RANK_EA_CHANGES"] > 0)].index.tolist()
    ind_rank_ea_no_changes = df_cart_info[(df_cart_info["RANK_EA_CHANGES"].notnull()) &
                                          (df_cart_info["RANK_EA_CHANGES"] == 0)].index.tolist()
    ind_rank_ea_neg_changes = df_cart_info[(df_cart_info["RANK_EA_CHANGES"].notnull()) &
                                           (df_cart_info["RANK_EA_CHANGES"] < 0)].index.tolist()

    df_cart_info.loc[ind_rank_uniqueuserno_neg_changes, "RANK_UNIQUEUSERNO_CHANGES"] = -1 * df_cart_info.loc[
        ind_rank_uniqueuserno_neg_changes, "RANK_UNIQUEUSERNO_CHANGES"]
    df_cart_info["RANK_UNIQUEUSERNO_CHANGES_STR"] = np.nan
    df_cart_info.loc[ind_rank_uniqueuserno_pos_changes, "RANK_UNIQUEUSERNO_CHANGES_STR"] = "상승"
    df_cart_info.loc[ind_rank_uniqueuserno_no_changes, "RANK_UNIQUEUSERNO_CHANGES_STR"] = "변동 없음"
    df_cart_info.loc[ind_rank_uniqueuserno_neg_changes, "RANK_UNIQUEUSERNO_CHANGES_STR"] = "하락"
    del ind_rank_uniqueuserno_pos_changes, ind_rank_uniqueuserno_no_changes, ind_rank_uniqueuserno_neg_changes

    df_cart_info.loc[ind_rank_ea_no_changes, "RANK_EA_CHANGES"] = np.nan
    df_cart_info.loc[ind_rank_ea_neg_changes, "RANK_EA_CHANGES"] = -1 * df_cart_info.loc[
        ind_rank_ea_neg_changes, "RANK_EA_CHANGES"]
    df_cart_info["RANK_EA_CHANGES_STR"] = np.nan
    df_cart_info.loc[ind_rank_ea_pos_changes, "RANK_EA_CHANGES_STR"] = "상승"
    df_cart_info.loc[ind_rank_ea_no_changes, "RANK_EA_CHANGES_STR"] = "변동 없음"
    df_cart_info.loc[ind_rank_ea_neg_changes, "RANK_EA_CHANGES_STR"] = "하락"
    del ind_rank_ea_pos_changes, ind_rank_ea_no_changes, ind_rank_ea_neg_changes

    f_nm = "{}df_total_order_sequence.csv".format(path_export)
    print("Exporting {}".format(f_nm))
    df_total_order_sequence.to_csv(f_nm, index=False)
    del f_nm, df_total_order_sequence

    f_nm = "{}df_order_region.csv".format(path_export)
    print("Exporting {}".format(f_nm))
    df_order_region.to_csv(f_nm, index=False)
    del f_nm, df_order_region

    f_nm = "{}df_by_join_date.csv".format(path_export)
    print("Exporting {}".format(f_nm))
    df_by_join_date.to_csv(f_nm, index=False)
    del f_nm, df_by_join_date

    f_nm = "{}df_main.csv".format(path_export)
    print("Exporting {}".format(f_nm))
    df_main.to_csv(f_nm, index=False)
    del f_nm, df_main

    f_nm = "{}df_main_total.csv".format(path_export)
    print("Exporting {}".format(f_nm))
    df_main_total.to_csv(f_nm, index=False)
    del f_nm, df_main_total

    f_nm = "{}df_main_total_with_comparison.csv".format(path_export)
    print("Exporting {}".format(f_nm))
    df_main_total_temp.to_csv(f_nm, index=False)
    del f_nm, df_main_total_temp

    f_nm = "{}df_cart_info.csv".format(path_export)
    print("Exporting {}".format(f_nm))
    df_cart_info.to_csv(f_nm, index=False)
    del f_nm, df_cart_info

    f_nm = "{}df_first_to_second_purchase_g.csv".format(path_export)
    print("Exporting {}".format(f_nm))
    df_first_to_second_purchase_g.to_csv(f_nm, index=False)
    del f_nm, df_first_to_second_purchase_g

    f_nm = "{}df_time_to_purchase.csv".format(path_export)
    print("Exporting {}".format(f_nm))
    df_time_to_purchase.to_csv(f_nm, index=False)
    del f_nm, df_time_to_purchase

    f_nm = "{}df_retention_rate.csv".format(path_export)
    print("Exporting {}".format(f_nm))
    df_retention_rate.to_csv(f_nm, index=False)
    del f_nm, df_retention_rate

    f_nm = "{}df_weekday_engagement_rate.csv".format(path_export)
    print("Exporting {}".format(f_nm))
    df_weekday_engagement_rate.to_csv(f_nm, index=False)
    del f_nm, df_weekday_engagement_rate

    del dbname

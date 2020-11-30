def personal_check_income_base_delivery(path_import=None, path_export=None):
    from glob import glob
    import pandas as pd
    import numpy as np
    import gzip
    import pickle
    from datetime import datetime

    map_folder = {}
    for f in sorted(glob("{}*".format(path_import))):
        f_key = f.split("/")[-1]
        f_val = datetime(year=int(f_key.split("_")[0]), month=int(f_key.split("_")[1]), day=1).strftime("%Y-%m")
        map_folder.update({f_key: f_val})
        del f
    map_folder = {map_folder[k]: k for k in map_folder.keys()}

    map_final_folder = {}
    for i in sorted(map_folder):
        map_final_folder.update({i: map_folder[i]})
        del i
    del map_folder

    for folder_date in map_final_folder.keys():
        for file in glob("{}*".format(path_import)):
            if map_final_folder[folder_date] in file:
                for f in glob("{}/*".format(file)):
                    f_temp = f.split("/")[-1][18:][9:]
                    if "8.xlsx" in f_temp:
                        print("Reading {}".format(f))
                        df = pd.read_excel(f)
                        df = df.groupby(["정제 여부", "정제 구분", "인입 구분"])["비회원ID"].count().reset_index(name="건수")
                        df.insert(0, "기준", np.nan)
                        df.loc[0, "기준"] = "사용 데이터 : {}".format(f.split("/")[-1])
                        df.loc[1, "기준"] = "업데이트 날짜 : {}, 업데이트 시간 : {}시".format(f.split("/")[-1][18:26],
                                                                               f_temp.split(".")[0][-1])
                        df.loc[2, "기준"] = "비회원 인입자 (날짜) : {}년 {}월".format(f_temp.split(".")[0][:-2].split("_")[0],
                                                                                     f_temp.split(".")[0][:-2].split("_")[1])

                        file_nm_lead = datetime(year=int(f_temp.split(".")[0][:-2].split("_")[0]),
                                                month=int(f_temp.split(".")[0][:-2].split("_")[1]),
                                                day=1).strftime("%Y-%m")
                        file_nm = "{}incomebase_lead_{}_updatedate_{}.pickle".format(path_export,
                                                                                     file_nm_lead,
                                                                                     f.split("/")[-1][18:26])
                        del file_nm_lead
                        print("Exporting {}".format(file_nm))
                        with gzip.open(file_nm, "wb") as f:
                            pickle.dump(df, f)
                            del f
                        del file_nm, df
            del file
        del folder_date


# [상담구분] mapping
def mapping_table_for_consulting_div():
    import numpy as np
    map_consult_div = {"통화완료": "통화완료", "제외대상": "제외대상", "부재": "부재", "재통화": "재통화",
                       np.nan: "상담 내역 없음", "NODATA": "상담 내역 없음"}
    return map_consult_div


# [상담상태] mapping
def mapping_table_for_consulting_status():
    import numpy as np
    map_consult_status_agg = {"체험 미신청": "체험 미신청", "체험 신청": "체험 신청", "상담 거부(삭제 요청)": "상담 거부",
                              "유료 결제": "유료 결제", "대상자 없음(자녀 없음,6세 이하,고등 이상)": "대상자 없음",
                              "중복": "중복", "3차 부재": "3차 부재 이상", "오번": "결번/오번", "6차 부재 이상": "3차 부재 이상",
                              "중등": "중등", "재통화 요청": "재통화 에약/요청", "재통화 예약": "재통화 에약/요청", "결번": "결번/오번",
                              "1차 부재": "1차 부재", "2차 부재": "2차 부재", "4차 부재": "3차 부재 이상", "5차 부재": "3차 부재 이상",
                              "예비 초등(7세)": "예비초등", "배송 대기": "배송 대기", "재통화 부재": "재통화 부재", np.nan: "상담 내역 없음",
                              "NODATA": "상담 내역 없음"}
    return map_consult_status_agg


# 집계 기준 #
def count_marketing(df=None):
    lst_col = ["정제구분", "인입구분", "출하현황", "로그인여부(임시계정발급일이후)", "상품유무료구분", "주문상태", "첫결제상태"]
    if "인입후결제" in df.columns:
        lst_col.append("인입후결제")
    for col in lst_col:
        df[col] = df[col].str.replace(" ", "")
        del col

    # 1) 마케팅실 > 정제필요 제외 / 2) 고객창출실 > 신규DB & 신규배정 집계"
    print("Setting 인입, 상담, 배송, 로그인, 결제 indices for marketing..")
    # 인입 : 전체 데이터
    set_ind_leading = set(df[df["정제구분"] != "정제필요"].index.tolist())
    # 상담 : 상담 부분에 고객창출실이 아닌곳도 존재하므로, 상담부분은 최근배정교사조직 == 고객창출실 추가 (고객창출실과 집계 기준 동일)
    set_ind_consult = set(df[(df["출하현황"] == "임시아이디발급요청") |
                             (df["출하현황"] == "임시아이디발급완료") |
                             (df["출하현황"] == "기존아이디매핑") |
                             (df["출하현황"] == "배송결제완료(직가입)")].index.tolist())
    set_ind_consult = set_ind_consult.intersection(set_ind_leading)
    # 배송
    set_ind_delivery = set(df[(df["출하현황"] == "임시아이디발급요청") |
                              (df["출하현황"] == "임시아이디발급완료") |
                              (df["출하현황"] == "기존아이디매핑") |
                              (df["출하현황"] == "배송결제완료(직가입)")].index.tolist())
    set_ind_delivery = set_ind_delivery.intersection(set_ind_leading)
    # 출하현황 & 로그인여부
    set_ind_login = set(df[(df["로그인여부(임시계정발급일이후)"] == "Y") |
                           (df["출하현황"] == "배송결제완료(직가입)")].index.tolist())
    set_ind_login = set_ind_login.intersection(set_ind_leading)
    # 상품유무료구분 & 주문상태 & 첫결제상태 & 인입후결제
    set_ind_payment = set()
    if "인입후결제" in lst_col:
        set_ind_payment.update(set(df[df["상품유무료구분"] == "유료상품"].index.tolist()).intersection(
            set(df[(df["주문상태"] == "주문완료") |
                   (df["주문상태"] == "환불완료") |
                   (df["주문상태"] == "환불요청")].index.tolist()),
            set(df[df["첫결제상태"] == "입금완료"].index.tolist()),
            set(df[df["인입후결제"] == "Y"].index.tolist())))
    if "인입후결제" not in lst_col:
        set_ind_payment.update(set(df[df["상품유무료구분"] == "유료상품"].index.tolist()).intersection(
            set(df[(df["주문상태"] == "주문완료") |
                   (df["주문상태"] == "환불완료") |
                   (df["주문상태"] == "환불요청")].index.tolist()),
            set(df[df["첫결제상태"] == "입금완료"].index.tolist())))

    set_ind_payment = set_ind_payment.intersection(set_ind_leading)

    return set_ind_leading, set_ind_consult, set_ind_delivery, set_ind_login, set_ind_payment


def read_category_table(path=None):
    import pandas as pd

    file_nm = "{}category_table.xlsx".format(path)
    print("Reading {}".format(file_nm))
    df_category_table = pd.read_excel("{}category_table.xlsx".format(path))
    del file_nm
    df_category_table["채널"] = df_category_table["채널"].str.replace(" ", "")
    df_category_table["프로모션"] = df_category_table["프로모션"].str.replace(" ", "")

    return df_category_table


def read_ad_cost(date=None, path=None):
    import pandas as pd

    file_nm = "{}marketing_adCost.xlsx".format(path)
    print("Reading {}\nSetting adCost Date == {}".format(file_nm, date))
    df_ad_cost = pd.read_excel(file_nm)
    del file_nm
    df_ad_cost["채널"] = df_ad_cost["채널"].str.replace(" ", "")
    if date != None:
        df_ad_cost_monthly = df_ad_cost[df_ad_cost["Date"] == date].copy().reset_index(drop=True)
        return df_ad_cost_monthly
    else:
        return df_ad_cost


def read_leading_income_base(path_leading=None, standard_date_leading=None, update_date_leading=None):
    import pandas as pd
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    file_nm = "{}income_base_login_{}_{}.xlsx".format(path_leading, standard_date_leading, update_date_leading)
    print("Reading {}".format(file_nm))
    df_leading = pd.read_excel(file_nm)
    df_leading.columns = [col.replace(" ", "") for col in df_leading.columns]
    df_leading["프로모션"] = df_leading["프로모션"].str.replace(" ", "")
    df_leading.drop(["채널"], axis=1, inplace=True)

    return df_leading


def read_delivery_income_base(path_delivery=None, standard_date_delivery=None, update_date_delivery=None):
    import pandas as pd
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    file_nm = "{}income_base_login_{}_{}.xlsx".format(path_delivery, standard_date_delivery, update_date_delivery)
    print("Reading {}".format(file_nm))
    df_delivery = pd.read_excel(file_nm)
    df_delivery.columns = [col.replace(" ", "") for col in df_delivery.columns]
    df_delivery["프로모션"] = df_delivery["프로모션"].str.replace(" ", "")
    df_delivery.drop(["채널"], axis=1, inplace=True)

    return df_delivery


# 고객창출실용 대시보드 데이터 추출
def dataset_to_aggregate(standard_date_delivery=None, update_date_delivery=None,
                         standard_date_leading=None, update_date_leading=None,
                         path_output=None, path_leading=None, path_delivery=None):
    import pandas as pd
    from datetime import datetime

    date_update = datetime(year=int(update_date_leading.split("_")[0]),
                           month=int(update_date_leading.split("_")[1]),
                           day=1).strftime("%Y-%m")

    map_consult_div = mapping_table_for_consulting_div()
    map_consult_status_agg = mapping_table_for_consulting_status()
    df_category_table = read_category_table(path=path_output)
    df_category_table = df_category_table[df_category_table["Year"] == int(date_update.split("-")[0])].reset_index(drop=True).drop(["Year"], axis=1)

    df_leading = read_leading_income_base(path_leading=path_leading, standard_date_leading=standard_date_leading,
                                          update_date_leading=update_date_leading)
    df_delivery = read_delivery_income_base(path_delivery=path_delivery, standard_date_delivery=standard_date_delivery,
                                            update_date_delivery=update_date_delivery)

    df_leading["인입일"] = df_leading["인입일"].apply(lambda x: str(x).split(" ")[0])
    df_leading.insert(0, "Date", df_leading["인입일"].apply(lambda x: x[:-3]))
    df_delivery["인입일"] = df_delivery["인입일"].apply(lambda x: str(x).split(" ")[0])
    df_delivery.insert(0, "Date", df_delivery["인입일"].apply(lambda x: x[:-3]))

    ####################################################################################################################
    for df_temp in [df_leading, df_delivery]:
        t = df_temp["상담구분"].unique().tolist()
        map_consult_div.update({k: v for k, v in dict(zip(t, t)).items() if type(k) is not float})
        del t
        df_temp["상담구분"] = df_temp["상담구분"].map(map_consult_div)
        ##############################################
        df_temp["상담상태agg"] = df_temp["상담상태"].copy()
        t = df_temp["상담상태agg"].unique().tolist()
        map_consult_status_agg.update({k: v for k, v in dict(zip(t, t)).items() if type(k) is not float})
        for val in ["3차 부재", "4차 부재", "5차 부재", "6차 부재 이상"]:
            map_consult_status_agg[val] = "3차 부재 이상"
            del val
        del t
        df_temp["상담상태agg"] = df_temp["상담상태agg"].map(map_consult_status_agg)
        del df_temp
    for col in ["상담구분", "상담상태agg"]:
        if df_leading[df_leading[col].isnull()].shape[0] != 0:
            raise ValueError("There are null values in {} column in df_leading..".format(col))
        if df_delivery[df_delivery[col].isnull()].shape[0] != 0:
            raise ValueError("There are null values in {} column in df_delivery..".format(col))
        del col
    ####################################################################################################################

    for col in ["정제구분", "인입구분", "출하현황", "로그인여부(임시계정발급일이후)", "상품유무료구분", "주문상태", "첫결제상태"]:
        df_delivery[col] = df_delivery[col].str.replace(" ", "")
        del col
    if "인입후결제" in df_delivery.columns:
        df_delivery["인입후결제"] = df_delivery["인입후결제"].str.replace(" ", "")

    df_leading = pd.merge(df_category_table, df_leading, on=["프로모션"], how="inner")
    df_delivery = pd.merge(df_category_table, df_delivery, on=["프로모션"], how="inner")

    del df_category_table
    if "2019" in update_date_leading and "중등" in df_leading["대상"].unique():
        print("Removing 대상==중등 in {} dataframe for df_leading..".format(update_date_leading))
        df_leading = df_leading[df_leading["대상"] == "초등"].reset_index(drop=True)
    if "2019" in update_date_leading and "중등" in df_delivery["대상"].unique():
        print("Removing 대상==중등 in {} dataframe for df_delivery..".format(update_date_leading))
        df_delivery = df_delivery[df_delivery["대상"] == "초등"].reset_index(drop=True)

    print("Dataset for {} is ready to parse!!!!!!!!\n >>> DO NOT CHNGE INDICES !!! <<<".format(date_update))

    return df_leading, df_delivery


# 인입, 배송, 로그인, 결제 - setting indices
def setting_indices(standard_date_delivery=None, update_date_delivery=None,
                    standard_date_leading=None, update_date_leading=None,
                    path_output=None, path_leading=None, path_delivery=None):
    import numpy as np

    temp = dataset_to_aggregate(standard_date_delivery=standard_date_delivery, update_date_delivery=update_date_delivery,
                                standard_date_leading=standard_date_leading, update_date_leading=update_date_leading,
                                path_output=path_output, path_leading=path_leading, path_delivery=path_delivery)
    df_leading = temp[0]
    df_delivery = temp[1]
    del temp
    temp_ind_marketing_leading = count_marketing(df=df_leading)
    temp_ind_marketing_leading_leading = temp_ind_marketing_leading[0]
    temp_ind_marketing_delivery = count_marketing(df=df_delivery)
    temp_ind_marketing_delivery_consult = temp_ind_marketing_delivery[1]
    temp_ind_marketing_delivery_delivery = temp_ind_marketing_delivery[2]
    temp_ind_marketing_delivery_login = temp_ind_marketing_delivery[3]
    temp_ind_marketing_delivery_payment = temp_ind_marketing_delivery[4]
    del temp_ind_marketing_leading, temp_ind_marketing_delivery

    df_leading["인입"] = np.nan
    df_leading.loc[temp_ind_marketing_leading_leading, "인입"] = 1
    df_leading["인입"] = df_leading["인입"].fillna(0).astype(int)

    for col in ["상담", "배송", "로그인", "결제"]:
        df_delivery[col] = np.nan
        del col
    df_delivery.loc[temp_ind_marketing_delivery_consult, "상담"] = 1
    df_delivery.loc[temp_ind_marketing_delivery_delivery, "배송"] = 1
    df_delivery.loc[temp_ind_marketing_delivery_login, "로그인"] = 1
    df_delivery.loc[temp_ind_marketing_delivery_payment, "결제"] = 1
    for col in ["상담", "배송", "로그인", "결제"]:
        df_delivery[col] = df_delivery[col].fillna(0).astype(int)
        del col

    return df_leading, df_delivery

# 광고비 추가
def merge_with_other_data(standard_date_delivery=None, update_date_delivery=None,
                          standard_date_leading=None, update_date_leading=None,
                          path_output=None, path_leading=None, path_delivery=None):
    import pandas as pd
    import numpy as np
    from datetime import datetime
    import gzip
    import pickle

    ####################################################################################################################
    map_leading_sum = {"인입": "sum"}
    map_delivery_sum = {"상담": "sum", "배송": "sum", "로그인": "sum", "결제": "sum"}
    map_leading_delivery_sum = map_leading_sum.copy()
    map_leading_delivery_sum.update(map_delivery_sum)
    # 내부대시보드용 column
    col_group_main = ["Date", "대상", "대분류", "소분류", "채널", "프로모션", "프로모션 (일)", "비고", "인입일", "최근 배정 교사명",
                      "상담 구분", "상담 상태 agg"]
    # 집행현황분석용 column
    col_group_submain = ["Date", "대상", "대분류", "소분류", "채널"]
    # 내부+외부 columns
    col_group_int_ext = ["필터", "Date", "대분류", "소분류", "채널", "대상"]
    ####################################################################################################################

    date = datetime(year=int(update_date_leading.split("_")[0]), month=int(update_date_leading.split("_")[1]),
                    day=1).strftime("%Y-%m")

    df_category_table = read_category_table(path=path_output)
    df_category_table = df_category_table[df_category_table["Year"] == int(update_date_leading.split("_")[0])].reset_index(drop=True).drop(
        ["Year"], axis=1)
    df_ad_cost_monthly = read_ad_cost(date=date, path=path_output)

    df = setting_indices(standard_date_delivery=standard_date_delivery, update_date_delivery=update_date_delivery,
                         standard_date_leading=standard_date_leading, update_date_leading=update_date_leading,
                         path_output=path_output, path_leading=path_leading, path_delivery=path_delivery)
    df_leading = df[0].copy()
    df_delivery = df[1].copy()
    del df

    temp_leading_g = df_leading.groupby(col_group_submain).agg(map_leading_sum).reset_index()
    temp_delivery_g = df_delivery.groupby(col_group_submain).agg(
        map_delivery_sum).reset_index()
    df_marketing = pd.merge(temp_leading_g, temp_delivery_g, on=col_group_submain, how="outer")
    del temp_leading_g, temp_delivery_g
    df_marketing["Date"] = df_marketing["Date"].apply(lambda x: x[:-3])
    for col in ["인입", "상담", "배송", "로그인", "결제"]:
        df_marketing[col] = df_marketing[col].fillna(0).astype(int)
        del col
    df_marketing = df_marketing.groupby(col_group_submain).agg(map_leading_delivery_sum).reset_index()

    file_nm_raw = "{}after_202011/raw/df_check_marketing_data_{}.pickle".format(path_output, date)
    with gzip.open(file_nm_raw, "wb") as f:
        print("Exporting {}".format(file_nm_raw))
        pickle.dump(df_marketing, f)
        del f, df_marketing, file_nm_raw
    df_leading = df_leading.fillna("데이터없음")
    df_leading = df_leading.rename(columns={"프로모션(일)": "프로모션 (일)", "최근배정교사명": "최근 배정 교사명", "상담구분": "상담 구분",
                                              "상담상태agg": "상담 상태 agg"})
    df_delivery = df_delivery.fillna("데이터없음")
    df_delivery = df_delivery.rename(columns={"프로모션(일)": "프로모션 (일)", "최근배정교사명": "최근 배정 교사명", "상담구분": "상담 구분",
                                              "상담상태agg": "상담 상태 agg"})

    ####################################################################################################################
    df_main_leading_g = df_leading.groupby(col_group_main).agg(map_leading_sum).reset_index()
    df_main_leading_g.insert(0, "필터", "전체DB")
    df_submain_leading_g = df_main_leading_g.groupby(col_group_submain).agg(map_leading_sum).reset_index()
    df_submain_leading_g.insert(0, "필터", "전체DB")

    df_main_delivery_g = df_delivery.groupby(col_group_main).agg(map_delivery_sum).reset_index()
    df_main_delivery_g.insert(0, "필터", "전체DB")
    df_submain_delivery_g = df_main_delivery_g.groupby(col_group_submain).agg(map_delivery_sum).reset_index()
    df_submain_delivery_g.insert(0, "필터", "전체DB")

    if df_submain_leading_g["채널"].nunique(dropna=False) != df_submain_delivery_g["채널"].nunique(dropna=False):
        r = list(set(df_submain_leading_g["채널"].unique().tolist()).difference(df_submain_delivery_g["채널"].unique().tolist()))
        print("The following channel name is in leading dataframe but not in delivery dataframe..\n{}\n값 산출할때는 다 포함 (상담, 배송, 로그인, 결제 == 0)".format(r))
        del r
    if df_main_leading_g["채널"].nunique(dropna=False) != df_main_delivery_g["채널"].nunique(dropna=False):
        r = list(set(df_main_leading_g["채널"].unique().tolist()).difference(df_main_delivery_g["채널"].unique().tolist()))
        print("The following channel name is in leading dataframe but not in delivery dataframe..\n{}\n값 산출할때는 다 포함 (상담, 배송, 로그인, 결제 == 0)".format(r))
        del r
    if df_main_leading_g["프로모션"].nunique(dropna=False) != df_main_delivery_g["프로모션"].nunique(dropna=False):
        r = list(set(df_main_leading_g["프로모션"].unique().tolist()).difference(df_main_delivery_g["프로모션"].unique().tolist()))
        print("The following promotion name is in leading dataframe but not in delivery dataframe..\n{}\n값 산출할때는 다 포함 (상담, 배송, 로그인, 결제 == 0)".format(r))
        del r

    # 돈은 사용했으나, 1.인입,  2.배송,  3.로그인,  4.결제 갸 없는 경우, 0 으로 변경
    print("돈은 사용했으나, 1.인입,  2.배송,  3.로그인,  4.결제 갸 없는 경우, 0 으로 변경..")
    temp_df_ad_cost_monthly = df_ad_cost_monthly[df_ad_cost_monthly["adCost"] != 0].copy()
    lst_missing_channel = list(set(temp_df_ad_cost_monthly["채널"].unique().tolist()).difference(set(df_main_leading_g["채널"].unique().tolist())))
    temp_df_category_table = pd.DataFrame()
    for v in lst_missing_channel:
        temp_df_category_table = temp_df_category_table.append(
            df_category_table[df_category_table["채널"] == v].copy().reset_index(drop=True), sort=False, ignore_index=True)
        del v
    del lst_missing_channel, temp_df_ad_cost_monthly
    if temp_df_category_table.shape[0] != 0:
        temp_df_category_table["필터"] = "전체DB"
        temp_df_category_table["Date"] = date
        temp_df_category_table["비고"].fillna("데이터없음", inplace=True)

        temp_df_category_table_leading = temp_df_category_table.copy()
        temp_df_category_table_delivery = temp_df_category_table.copy()
        del temp_df_category_table
        for k in map_leading_delivery_sum.keys():
            if k in map_leading_sum.keys():
                temp_df_category_table_leading[k] = 0
            if k in map_delivery_sum.keys():
                temp_df_category_table_delivery[k] = 0
            del k

        df_main_leading_g = df_main_leading_g.append(temp_df_category_table_leading, sort=False, ignore_index=True)
        df_main_delivery_g = df_main_delivery_g.append(temp_df_category_table_delivery, sort=False, ignore_index=True)

        temp_df_category_table_leading.drop(["프로모션", "프로모션 (일)", "비고"], axis=1, inplace=True)
        temp_df_category_table_delivery.drop(["프로모션", "프로모션 (일)", "비고"], axis=1, inplace=True)
        # 여러개의 프로모션이 한개의 채널에 들어가기 때문에 프로모션명 제거시, unique 한 채널명으로 변경
        temp_df_category_table_leading.drop_duplicates(keep="first", inplace=True)
        temp_df_category_table_delivery.drop_duplicates(keep="first", inplace=True)

        df_submain_leading_g = df_submain_leading_g.append(temp_df_category_table_leading, sort=False,
                                                           ignore_index=True)
        df_submain_delivery_g = df_submain_delivery_g.append(temp_df_category_table_delivery, sort=False,
                                                             ignore_index=True)

        for col in ["인입일", "최근 배정 교사명", "상담 구분", "상담 상태 agg"]:
            df_main_leading_g[col].fillna("데이터없음", inplace=True)
            df_main_delivery_g[col].fillna("데이터없음", inplace=True)
            del col

    # status_내부대시보드용 ################################################################################################
    print("Creating status data for {}..".format(date))
    df_status_leading_g = df_main_leading_g.melt(id_vars=["필터", "Date", "대분류", "소분류", "채널", "인입일", "대상"],
                                                 value_vars=["인입"],
                                                 var_name="status", value_name="dbNum")
    df_status_leading_g["인입일"] = df_status_leading_g["인입일"].astype(str)
    df_status_leading_g["인입일"] = df_status_leading_g["인입일"].str.split(" ", expand=True)[0]
    df_status_leading_g = df_status_leading_g.groupby(["필터", "Date", "대상", "대분류", "소분류", "채널", "인입일", "status"]).agg({"dbNum": "sum"}).reset_index()

    df_status_delivery_g = df_main_delivery_g.melt(id_vars=["필터", "Date", "대분류", "소분류", "채널", "인입일", "대상"],
                                                   value_vars=["배송", "로그인", "결제"],
                                                   var_name="status", value_name="dbNum")
    df_status_delivery_g["인입일"] = df_status_delivery_g["인입일"].astype(str)
    df_status_delivery_g["인입일"] = df_status_delivery_g["인입일"].str.split(" ", expand=True)[0]
    df_status_delivery_g = df_status_delivery_g.groupby(["필터", "Date", "대상", "대분류", "소분류", "채널", "인입일", "status"]).agg({"dbNum": "sum"}).reset_index()

    df_status_g = pd.concat([df_status_leading_g, df_status_delivery_g], axis=0, ignore_index=True).sort_values(
        by=["대상", "Date", "채널"], ignore_index=True)
    del df_status_leading_g, df_status_delivery_g
    lst_ind_no_data = df_status_g[df_status_g["인입일"] == "데이터없음"].index.tolist()
    df_status_g.loc[lst_ind_no_data, "인입일"] = np.nan
    print("Status data for {} is created!".format(date))
    file_nm_status = "{}after_202011/monthly/df_marketing_{}_status.pickle".format(path_output, date)
    with gzip.open(file_nm_status, "wb") as f:
        print("Exporting {}".format(file_nm_status, date))
        pickle.dump(df_status_g, f)
        del df_status_g, f
    del file_nm_status

    # 집행현황분석용 #######################################################################################################
    df_submain = pd.merge(df_submain_leading_g, df_submain_delivery_g, on=["필터"] + col_group_submain, how="outer")
    del df_submain_leading_g, df_submain_delivery_g

    df_submain = pd.merge(df_submain, df_ad_cost_monthly, on=["Date", "대상", "대분류", "소분류", "채널"], how="outer")
    if df_ad_cost_monthly["adCost"].sum() != 0:
        df_submain_notnull = df_submain[df_submain["필터"].notnull()].copy()
        df_submain_isnull = df_submain[df_submain["필터"].isnull()].copy()
        df_submain_isnull = df_submain_isnull[df_submain_isnull["adCost"] != 0]
        df_submain_isnull["필터"].fillna("전체DB", inplace=True)
        del df_submain
        df_submain = pd.concat([df_submain_notnull, df_submain_isnull], axis=0, ignore_index=True)
        del df_submain_notnull, df_submain_isnull
    if df_ad_cost_monthly["adCost"].sum() == 0:
        df_submain = df_submain[df_submain["필터"].notnull()].reset_index(drop=True)

    if int(df_submain["adCost"].sum()) != int(df_ad_cost_monthly["adCost"].sum()):
        temp = df_ad_cost_monthly[df_ad_cost_monthly["adCost"] != 0]["채널"].unique().tolist()
        print("The following channels are in df_ad_cost_monthly but not in df_submain..\n{}\nMaybe consider updating adCost again..?".format(
            set(temp).difference(set(df_submain["채널"].unique().tolist()))))
        raise ValueError("Some values for adCost is not appended..\n{} Total adCost: {}\nadCost in merged dataframe: {}".format(
            date, df_ad_cost_monthly['adCost'].sum(), df_submain['adCost'].sum()))
    else:
        df_submain["adCost"].fillna(0, inplace=True)
        for col in map_leading_delivery_sum.keys():
            df_submain[col] = df_submain[col].fillna(0).astype(int)
            del col

    file_nm_submain = "{}after_202011/monthly/df_marketing_{}_집행현황분석용.pickle".format(path_output, date)
    with gzip.open(file_nm_submain, "wb") as f:
        print("Exporting {}".format(file_nm_submain))
        pickle.dump(df_submain, f)
        del df_submain, f
    del file_nm_submain

    # 내부대시보드용 #######################################################################################################
    df_main = pd.merge(df_main_leading_g, df_main_delivery_g, on=["필터"] + col_group_main, how="outer")
    del df_main_leading_g, df_main_delivery_g
    for col in map_leading_delivery_sum.keys():
        df_main[col] = df_main[col].fillna(0).astype(int)
        del col

    lst_col_no_data = []
    for col in df_main.columns:
        if "데이터없음" in df_main[col].unique().tolist():
            lst_col_no_data.append(col)
        del col
    if len(lst_col_no_data) != 0:
        for col in lst_col_no_data:
            ind = df_main[df_main[col] == "데이터없음"].index.tolist()
            df_main.loc[ind, col] = np.nan
            del col, ind
        del lst_col_no_data

    file_nm_main = "{}after_202011/monthly/df_marketing_{}_내부대시보드용.pickle".format(path_output, date)
    with gzip.open(file_nm_main, "wb") as f:
        print("Exporting {}".format(file_nm_main))
        pickle.dump(df_main, f)
        del f
    del file_nm_main
    df_main_int_ext = df_main.copy()
    del df_main

    # 내부+외부 ##########################################################################################################
    print("Reading {}agency_data.xlsx".format(path_output))
    agency_data = pd.read_excel("{}agency_data.xlsx".format(path_output))
    print("Reading {}agency_data_mapping_table.xlsx".format(path_output))
    agency_data_mapping_table = pd.read_excel("{}agency_data_mapping_table.xlsx".format(path_output))

    ###################################################
    # 채널명 통일 부분 #
    agency_data_mapping_table = dict(zip(agency_data_mapping_table["매체_마케팅실파일"].tolist(), agency_data_mapping_table["채널_분류TABLE"].tolist()))

    map_tableau_marketing = dict(zip(df_main_int_ext["채널"].unique().tolist(), df_main_int_ext["채널"].unique().tolist()))
    map_tableau_marketing["링크플랜에스_1"] = agency_data_mapping_table["텐핑 TM"]
    map_tableau_marketing["링크플랜에스_3"] = agency_data_mapping_table["텐핑 TM"]
    map_tableau_marketing["링크플랜에스_2"] = agency_data_mapping_table["핀크럭스 TM"]
    map_tableau_marketing["링크플랜에스_4"] = agency_data_mapping_table["핀크럭스 TM"]

    map_marketing_ad_cost = dict(zip(df_main_int_ext["채널"].unique().tolist(), df_main_int_ext["채널"].unique().tolist()))
    map_marketing_ad_cost["링크플랜에스_1"] = agency_data_mapping_table["텐핑 TM"]
    map_marketing_ad_cost["링크플랜에스_3"] = agency_data_mapping_table["텐핑 TM"]
    map_marketing_ad_cost["링크플랜에스_2"] = agency_data_mapping_table["핀크럭스 TM"]
    map_marketing_ad_cost["링크플랜에스_4"] = agency_data_mapping_table["핀크럭스 TM"]
    ###################################################

    df_main_int_ext["채널"] = df_main_int_ext["채널"].map(map_tableau_marketing)
    if df_main_int_ext[df_main_int_ext["채널"].isnull()].shape[0] != 0:
        raise ValueError("There are null values in 채널 columns after mapping (df_main_int_ext)..")
    else:
        del map_tableau_marketing

    del map_leading_delivery_sum["상담"]
    df_main_int_ext = df_main_int_ext.groupby(col_group_int_ext).agg(map_leading_delivery_sum).reset_index()

    df_ad_cost_monthly["채널"] = df_ad_cost_monthly["채널"].map(map_marketing_ad_cost)
    del map_marketing_ad_cost
    if df_ad_cost_monthly[df_ad_cost_monthly["채널"].isnull()]["adCost"].sum() != 0:
        raise ValueError("There are null values in 채널 columns after mapping (df_ad_cost_monthly)..")
    else:
        df_ad_cost_monthly = df_ad_cost_monthly[df_ad_cost_monthly["채널"].notnull()].reset_index(drop=True)

    if df_ad_cost_monthly.shape[0] != df_ad_cost_monthly["채널"].nunique():
        df_ad_cost_monthly["adCost_비고"].fillna("데이터없음", inplace=True)
        df_ad_cost_monthly = df_ad_cost_monthly.groupby(["Date", "대상", "대분류", "소분류", "채널", "adCost_비고"])["adCost"].sum().reset_index()
        ind = df_ad_cost_monthly[df_ad_cost_monthly["adCost_비고"] == "데이터없음"].index.tolist()
        df_ad_cost_monthly.loc[ind, "adCost_비고"] = np.nan
        df_ad_cost_monthly = df_ad_cost_monthly[['Date', '대분류', '소분류', '채널', 'adCost', 'adCost_비고', '대상']]
        del ind
    df_ad_cost_monthly = df_ad_cost_monthly[df_ad_cost_monthly["adCost"] != 0].reset_index(drop=True)
    df_ad_cost_monthly["채널"] = df_ad_cost_monthly["채널"].str.replace(" ", "")
    df_main_int_ext["채널"] = df_main_int_ext["채널"].str.replace(" ", "")

    df_main_int_ext = pd.merge(df_main_int_ext, df_ad_cost_monthly, on=col_group_int_ext[1:], how="outer")
    # 돈을 사용해야 퍼널 앞단이 생기기 때문 광고비가 null 값이거나 0 은 제외
    df_main_int_ext = df_main_int_ext[df_main_int_ext["adCost"].notnull()].copy()
    df_main_int_ext = df_main_int_ext[df_main_int_ext["adCost"] != 0].copy()
    df_main_int_ext_notnull = df_main_int_ext[df_main_int_ext["필터"].notnull()].copy()
    df_main_int_ext_isnull = df_main_int_ext[df_main_int_ext["필터"].isnull()].copy()
    df_main_int_ext_isnull["필터"].fillna("전체DB", inplace=True)
    del df_main_int_ext
    df_main_int_ext = pd.concat([df_main_int_ext_notnull, df_main_int_ext_isnull], axis=0)
    del df_main_int_ext_notnull, df_main_int_ext_isnull

    for col in map_leading_delivery_sum.keys():
        df_main_int_ext[col] = df_main_int_ext[col].fillna(0).astype(int)
        del col
    del df_ad_cost_monthly

    agency_data = agency_data.rename(columns={"매체": "채널"}).drop(["구분"], axis=1)
    agency_data["채널"] = agency_data["채널"].str.replace(" ", "")

    # This part is for checking
    if len(agency_data[agency_data["노출수"] < agency_data["클릭수"]].index) != 0:
        raise ValueError("클릭수가 노출수보다 큰 값 존재..")
    if len(agency_data[agency_data["클릭수"] < agency_data["체험신청(합계)"]].index) != 0:
        raise ValueError("체험신청수가 클릭수보다 큰 값 존재..")

    # 노출은 존재하나 해당월에 광고비를 사용하지 않았으면, 인입부터 결제까지 인입이 불가능하므로 inner join 실시
    rows = df_main_int_ext.shape[0]
    df_final = pd.merge(df_main_int_ext, agency_data, on=["Date", "채널", "대상"], sort=True, how="left")
    if rows != df_final.shape[0]:
        raise ValueError("Need to check data after merging df_main_int_ext with agency_data..")
    else:
        del df_main_int_ext, agency_data
    df_final = df_final[df_final["체험신청(합계)"].notnull()].reset_index(drop=True)
    for col in ["노출수", "클릭수"]:
        if df_final[df_final[col].isnull()].shape[0] != 0:
            raise ValueError("There are null values in {} column for df_final..".format(col))
        else:
            df_final[col] = df_final[col].astype(int)
        del col
    df_final["체험신청(합계)"] = df_final["체험신청(합계)"].astype(int)
    df_final = df_final[df_final["노출수"].notnull()].reset_index(drop=True)
    df_final = df_final[["필터", "Date", "대분류", "소분류", "채널", "대상", "노출수", "클릭수", "체험신청(합계)",
                         "인입", "배송", "로그인", "결제", "adCost", "adCost_비고"]]
    rows = df_final.shape[0]
    print("Rows of dataframe before dropping where 체험신청(합계) < 인입 여부 : {}".format(rows))
    lst_ind_drop = df_final[df_final["체험신청(합계)"] < df_final["인입"]].index.tolist()
    df_final = df_final.drop(lst_ind_drop, axis=0).reset_index(drop=True)
    rows_after_dropping = df_final.shape[0]
    print("Rows of dataframe after dropping where 체험신청(합계) < 인입 여부 : {}".format(rows_after_dropping))
    df_final = df_final.rename(columns={"adCost": "광고비"})

    file_nm_final = "{}after_202011/monthly/df_marketing_{}_내부+외부.pickle".format(path_output, date)
    with gzip.open(file_nm_final, "wb") as f:
        print("Exporting {}".format(file_nm_final))
        pickle.dump(df_final, f)
        del df_final, f
    del file_nm_final


def concat_for_tableau(path_import=None, path_export=None):
    from glob import glob
    import gzip
    import pickle
    import pandas as pd

    df_internal = pd.DataFrame()
    df_status = pd.DataFrame()
    df_internal_with_ad_cost = pd.DataFrame()
    df_internal_external = pd.DataFrame()
    col_map = {"인입": "인입 여부", "상담": "상담 상태_count", "배송": "배송 구분", "로그인": "로그인 구분", "결제": "결제 여부"}

    print("Concatenating 내부대시보드용 files..")
    for file in [f for f in sorted(glob("{}*.pickle".format(path_import))) if "내부대시보드용" in f]:
        with gzip.open(file, "rb") as f:
            df_internal = df_internal.append(pickle.load(f), ignore_index=True, sort=False)
            del f
        del file
    df_internal.rename(columns=col_map, inplace=True)

    print("Concatenating status files..")
    for file in [f for f in sorted(glob("{}*.pickle".format(path_import))) if "status" in f]:
        with gzip.open(file, "rb") as f:
            df_status = df_status.append(pickle.load(f), ignore_index=True, sort=False)
            del f
        del file

    print("Concatenating 집행현황분석용 files..")
    for file in [f for f in sorted(glob("{}*.pickle".format(path_import))) if "집행현황분석용" in f]:
        with gzip.open(file, "rb") as f:
            df_internal_with_ad_cost = df_internal_with_ad_cost.append(pickle.load(f), ignore_index=True, sort=False)
            del f
        del file
    df_internal_with_ad_cost.rename(columns=col_map, inplace=True)
    df_internal_with_ad_cost.rename(columns={"adCost_비고": "비고"}, inplace=True)

    print("Concatenating 내부+외부 files..")
    for file in [f for f in sorted(glob("{}*.pickle".format(path_import))) if "내부+외부" in f]:
        with gzip.open(file, "rb") as f:
            df_internal_external = df_internal_external.append(pickle.load(f), ignore_index=True, sort=False)
            del f
        del file
    df_internal_external.rename(columns=col_map, inplace=True)

    print("Reading {}tableau_marketing_내부대시보드용.csv".format(path_export))
    df_internal.to_csv("{}tableau_marketing_내부대시보드용.csv".format(path_export), index=False)
    print("Reading {}tableau_marketing_status_내부대시보드용.csv".format(path_export))
    df_status.to_csv("{}tableau_marketing_status_내부대시보드용.csv".format(path_export), index=False)
    print("Reading {}tableau_marketing_집행현황분석용_내부대시보드용.csv".format(path_export))
    df_internal_with_ad_cost.to_csv("{}tableau_marketing_집행현황분석용_내부대시보드용.csv".format(path_export), index=False)
    print("Reading {}tableau_marketing_내부+외부.xlsx".format(path_export))
    df_internal_external.to_excel("{}tableau_marketing_내부+외부.xlsx".format(path_export), index=False)

    del df_internal, df_status, df_internal_with_ad_cost, df_internal_external


def compare_channels(standard_year=None, standard_month=None, path_output=None, path_import=None):
    if path_output is None:
        raise FileNotFoundError("Enter the path_output..")
    if path_import is None:
        raise FileNotFoundError("Enter the path_import..")
    if standard_year is None or standard_month is None:
        raise ValueError("Please enter year and month (기준일)..")

    from datetime import date
    import pandas as pd
    import numpy as np


    standard_date = pd.date_range(start="2020-01-01", end=date(year=standard_year, month=11, day=1).strftime("%Y-%m-%d"), freq="D")
    standard_date = sorted(list(set([x.strftime("%Y-%m") for x in standard_date])))
    standard_date = "|".join(standard_date)

    print("Creating dataframe monthly and yearly comparison!")
    df = pd.read_csv("{}tableau_marketing_집행현황분석용_내부대시보드용.csv".format(path_import))

    ####################################################################################################################

    lst_dt = sorted(df["Date"].unique().tolist())
    if date(year=standard_year, month=standard_month, day=1).strftime("%Y-%m") != lst_dt[-1]:
        rm = lst_dt.pop()
        print("Removing Date == {} from dataframe..".format(rm))
        lst_dt = "|".join(lst_dt)
        df = df[df["Date"].str.contains(lst_dt)].copy()
        del rm
    del lst_dt

    map_category_table = df.groupby(["대상", "대분류", "소분류"])["채널"].nunique().reset_index().rename(columns={"채널": "채널수"})

    date = pd.DataFrame(data=sorted(df["Date"].unique().tolist()), columns=["Date"])
    date["Date_prev_mth"] = date["Date"].shift(1)
    date["Date_prev_yr"] = date["Date"].shift(12)

    df = pd.merge(df, date, on=["Date"], how="left")
    del date
    df = df[['필터', '대상', 'Date', 'Date_prev_mth', 'Date_prev_yr', '대분류', '소분류', '채널',
             '인입 여부', '상담 상태_count', '배송 구분', '로그인 구분', '결제 여부', 'adCost']]

    ####################################################################################################################
    # 산출 방법: 기존, 기준점 변경(전체), 기준점 변경(신규) ####################################################################
    lst_df = []

    for filt in df["필터"].unique():

        dff = df[df["필터"] == filt].copy().reset_index(drop=True)

        # 전월 비교
        dff_prev_mth = dff[['Date', '대분류', '소분류', '채널', '인입 여부', '배송 구분', '로그인 구분', '결제 여부', 'adCost']].copy()
        for col in ['Date', '인입 여부', '배송 구분', '로그인 구분', '결제 여부', 'adCost']:
            dff_prev_mth = dff_prev_mth.rename(columns={"{}".format(col): "{}_prev_mth".format(col)})
            del col

        # 동월 비교
        dff_prev_yr = dff[['Date', '대분류', '소분류', '채널', '인입 여부', '배송 구분', '로그인 구분', '결제 여부', 'adCost']].copy()
        for col in ['Date', '인입 여부', '배송 구분', '로그인 구분', '결제 여부', 'adCost']:
            dff_prev_yr = dff_prev_yr.rename(columns={"{}".format(col): "{}_prev_yr".format(col)})
            del col

        for col in ['prev_mth', 'prev_yr']:
            if col == 'prev_mth':
                dff = pd.merge(dff, dff_prev_mth,
                                       on=["Date_{}".format(col), "대분류", "소분류", "채널"], how="left")
                for c in ['인입 여부', '배송 구분', '로그인 구분', '결제 여부']:
                    if c in ['결제 여부']:
                        dff["{}_전월".format(c)] = dff["{}_{}".format(c, col)]
                    if c == 'adCost':
                        continue
                    dff["{}_전월_차이".format(c)] = dff["{}".format(c)] - dff[
                        "{}_{}".format(c, col)]
                    dff.drop(["{}_{}".format(c, col)], axis=1, inplace=True)
            if col == 'prev_yr':
                dff = pd.merge(dff, dff_prev_yr,
                                       on=["Date_{}".format(col), "대분류", "소분류", "채널"], how="left")
                for c in ['인입 여부', '배송 구분', '로그인 구분', '결제 여부']:
                    if c in ['결제 여부']:
                        dff["{}_동월".format(c)] = dff["{}_{}".format(c, col)]
                    if c == 'adCost':
                        continue
                    dff["{}_동월_차이".format(c)] = dff["{}".format(c)] - dff["{}_{}".format(c, col)]
                    dff.drop(["{}_{}".format(c, col)], axis=1, inplace=True)
        del dff_prev_mth, dff_prev_yr

        dff.drop(["Date_prev_mth", "Date_prev_yr"], axis=1, inplace=True)
        dff = dff.rename(columns={"인입 여부": "인입", "배송 구분": "배송", "로그인 구분": "로그인", "결제 여부": "결제",
                                                  "결제 여부_전월": "결제_전월", "결제 여부_동월": "결제_동월",
                                                  "인입 여부_전월_차이": "인입_전월_차이", "결제 여부_전월_차이": "결제_전월_차이",
                                                  "인입 여부_동월_차이": "인입_동월_차이", "결제 여부_동월_차이": "결제_동월_차이"})
        lst_df.append(dff)
        del dff, filt

    df = pd.concat(lst_df, sort=False, axis=0).sort_values(by=["필터", "Date"]).reset_index(drop=True)
    del lst_df
    # DB 목표수 #########################################################################################################
    df_db_target = pd.read_excel("{}db_target_2020.xlsx".format(path_output))
    df_db_target.drop(["학년"], axis=1, inplace=True)

    map_category_table["소분류_매체"] = np.nan
    map_category_table.loc[map_category_table[map_category_table["대분류"] == "내부"].index, "소분류_매체"] = "내부"
    map_category_table.loc[map_category_table[map_category_table["대분류"] == "내부(SA)"].index, "소분류_매체"] = "내부(SA)"
    map_category_table.loc[map_category_table[map_category_table["대분류"] == "외부"].index, "소분류_매체"] = "외부(DA)"
    map_category_table.loc[map_category_table[map_category_table["소분류"] == "홈쇼핑"].index, "소분류_매체"] = "홈쇼핑"
    if map_category_table[map_category_table["소분류_매체"].isnull()].shape[0] != 0:
        raise ValueError("Need to check data..")
    else:
        map_category_table = map_category_table.groupby(["대상", "대분류", "소분류_매체", "소분류"])["채널수"].nunique().reset_index().drop(
            ["채널수"], axis=1)

    df_db_target["소분류"] = df_db_target["소분류"].map({"내부": "내부", "SA": "내부(SA)", "DA": "외부(DA)", "홈쇼핑": "홈쇼핑"})
    df_db_target.rename(columns={"기간": "Date"}, inplace=True)

    # merge filter on tableau
    df_db_target["소분류_merge_filter"] = df_db_target["소분류"].copy()
    df["소분류_merge_filter"] = np.nan
    df.loc[df[df["대분류"] == "내부"].index, "소분류_merge_filter"] = "내부"
    df.loc[df[df["대분류"] == "내부(SA)"].index, "소분류_merge_filter"] = "내부(SA)"
    df.loc[df[df["대분류"] == "외부"].index, "소분류_merge_filter"] = "외부(DA)"
    df.loc[df[df["소분류"] == "홈쇼핑"].index, "소분류_merge_filter"] = "홈쇼핑"
    ####################################################################################################################
    # 연도별 전체 인입, 배송, 로그인, 결제 ##################################################################################
    df_total = df.copy()
    df_total["year"] = df_total["Date"].str.split("-", expand=True)[0]
    df_total = df_total.groupby(["필터", "year", "대상"]).agg({"인입": "sum", "배송": "sum", "로그인": "sum", "결제": "sum"}).reset_index()

    df_target = df[df["Date"].str.contains(standard_date)].copy().reset_index(drop=True)
    del standard_date
    df_target = df_target.groupby(["필터", "Date", "대상", "대분류", "소분류"]).agg({"인입": "sum"}).reset_index()
    df_target["인입"] = df_target["인입"].astype(int)
    df_target = pd.merge(map_category_table, df_target, on=["대분류", "소분류", "대상"], how="inner")
    del map_category_table

    df_target = df_target.groupby(["필터", "Date", "소분류_매체"]).agg(
        {"인입": "sum"}).reset_index().rename(columns={"소분류_매체": "소분류"})
    df_target = pd.merge(df_target, df_db_target, on=["Date", "소분류"], how="inner")
    df_target = df_target.sort_values(by=["필터", "Date"]).reset_index(drop=True)
    del df_db_target
    ####################################################################################################################

    # Export dataframe
    print("Exporting {}month_comparison.csv".format(path_import))
    df.to_csv("{}month_comparison.csv".format(path_import), index=False)

    print("Exporting {}db_target.csv".format(path_import))
    df_target.to_csv("{}db_target.csv".format(path_import), index=False)

    print("Exporting {}total_value_for_month_comparison.xlsx".format(path_import))
    df_total.to_excel("{}total_value_for_month_comparison.xlsx".format(path_import), index=False)
    del df, df_target, df_total


def marketing_channels(path_output=None, path_tableau_data=None, lst_date=None, encoding="utf-8-sig"):
    if path_output is None:
        raise ValueError("Need to enter output folder path..")
    if path_tableau_data is None:
        raise ValueError("Need to enter tableau_data folder path..")
    if lst_date is None:
        raise ValueError("Need to enter dates you want to get..")

    import pandas as pd
    import numpy as np
    from functools import reduce

    filter_name = "전체DB"

    df_mkt_ad_cost = read_ad_cost(path=path_output)
    df_mkt_ad_cost = df_mkt_ad_cost[(df_mkt_ad_cost["Date"].str.contains("|".join(lst_date))) &
                                    (df_mkt_ad_cost["대분류"] != "내부")].reset_index(drop=True)
    df_mkt_ad_cost["채널"] = df_mkt_ad_cost["채널"].str.replace(" ", "")

    print("Reading {}tableau_marketing_집행현황분석용_내부대시보드용.csv".format(path_tableau_data))
    df_ad_cost = pd.read_csv("{}tableau_marketing_집행현황분석용_내부대시보드용.csv".format(path_tableau_data))
    # 준서님 전달용 ###
    df_ad_cost_with_metric = pd.read_excel("{}tableau_marketing_내부+외부.xlsx".format(path_tableau_data))
    df_ad_cost_with_metric = df_ad_cost_with_metric[df_ad_cost_with_metric["필터"] == filter_name].reset_index(
        drop=True).drop(["필터"], axis=1)
    df_ad_cost_with_metric = df_ad_cost_with_metric[(df_ad_cost_with_metric["Date"].str.contains("|".join(lst_date))) &
                                                    (df_ad_cost_with_metric["대분류"] != "내부")].reset_index(drop=True)
    df_channels_with_metric = df_ad_cost_with_metric.groupby(["대상", "대분류", "소분류", "채널"])["Date"].nunique().reset_index().drop(["Date"], axis=1)
    df_channels_with_metric["include_digital_metric"] = "y"
    ################

    lst_ad_cost_check = []
    for f in df_ad_cost["필터"].unique():
        lst_ad_cost_check.append(df_ad_cost[df_ad_cost["필터"] == f]["adCost"].sum())
        del f
    if len(lst_ad_cost_check) > 1:
        if not reduce(lambda x, y: x == y, lst_ad_cost_check):
            raise ValueError("Need to check adCost data for different filters..\n{}".format(lst_ad_cost_check))
    del lst_ad_cost_check

    df_ad_cost = df_ad_cost[df_ad_cost["Date"].str.contains("|".join(lst_date))].reset_index(drop=True)
    df_ad_cost = df_ad_cost[df_ad_cost["대분류"] != "내부"].reset_index(drop=True)
    df_ad_cost = df_ad_cost[df_ad_cost["필터"] == filter_name].reset_index(drop=True)
    df_ad_cost = df_ad_cost.rename(columns={"인입 여부": "인입", "배송 구분": "배송",
                                            "로그인 구분": "로그인", "결제 여부": "결제"}).drop(["비고"], axis=1).drop(
        ["필터", "상담 상태_count"], axis=1)

    for col in df_ad_cost.columns:
        if df_ad_cost[df_ad_cost[col].isnull()].shape[0] > 0:
            raise ValueError("{} has null values..".format(col))
        del col

    df_ad_cost["인입 단가"] = df_ad_cost["adCost"] / df_ad_cost["인입"]
    df_ad_cost["결제 단가"] = df_ad_cost["adCost"] / df_ad_cost["결제"]
    df_ad_cost["전환율(결제_기준)"] = df_ad_cost["결제"] / df_ad_cost["인입"]
    df_ad_cost = df_ad_cost.replace(np.inf, np.nan)

    if int(df_mkt_ad_cost["adCost"].sum()) != int(df_ad_cost["adCost"].sum()):
        df_mkt_ad_cost = df_mkt_ad_cost.rename(columns={"adCost": "adCost_mktAdCost"})
        df_ad_cost = df_ad_cost.rename(columns={"adCost": "adCost_dash"})
        aa = pd.merge(df_mkt_ad_cost, df_ad_cost, on=["Date", "대분류", "소분류", "대상", "채널"], how="outer")
        aa = aa[["Date", "대분류", "소분류", "채널", "대상", "adCost_mktAdCost", "adCost_dash"]]
        for col in ["adCost_mktAdCost", "adCost_dash"]:
            aa[col].fillna(0, inplace=True)
            aa[col] = aa[col].astype(int)
            del col
        aa = aa[aa["adCost_mktAdCost"] != aa["adCost_dash"]].copy()

        raise ValueError("Need to check adCost data...{}".format(aa))
    else:
        del df_mkt_ad_cost

        # 준서님 전달용 #####
        df_ad_cost_with_metric = pd.merge(df_ad_cost, df_ad_cost_with_metric, on=["Date", "대상", "대분류", "소분류", "채널"], how="outer")
        df_ad_cost_with_metric = pd.merge(df_ad_cost_with_metric, df_channels_with_metric, on=["대상", "대분류", "소분류", "채널"], how="left")
        del df_channels_with_metric
        df_ad_cost_with_metric = df_ad_cost_with_metric[df_ad_cost_with_metric["include_digital_metric"] == "y"].reset_index(
            drop=True).drop(["include_digital_metric"], axis=1).sort_values(by=["Date", "대상", "대분류", "소분류", "채널"])
        df_ad_cost_with_metric = df_ad_cost_with_metric[["Date", "대분류", "소분류", "채널", "노출수", "클릭수", "체험신청(합계)", "광고비"]].rename(
            columns={"체험신청(합계)": "체험신청"})
        df_ad_cost_with_metric["클릭률"] = df_ad_cost_with_metric["클릭수"] / df_ad_cost_with_metric["노출수"]
        df_ad_cost_with_metric["CPA"] = df_ad_cost_with_metric["광고비"] / df_ad_cost_with_metric["체험신청"]
        df_ad_cost_with_metric["CPC"] = df_ad_cost_with_metric["광고비"] / df_ad_cost_with_metric["클릭수"]
        df_ad_cost_with_metric["CVR"] = df_ad_cost_with_metric["체험신청"] / df_ad_cost_with_metric["클릭수"]
        ##################
        df_ad_cost = df_ad_cost.rename(columns={"adCost": "광고비"})
        df_ad_cost = df_ad_cost[['Date', '대분류', '소분류', '채널', '광고비', '인입', '배송', '로그인', '결제', '인입 단가',
                                 '결제 단가', '전환율(결제_기준)']]

    print("Exporting {}dataframe_marketing_channels_{}_present.xlsx".format(path_tableau_data, lst_date[0]))
    df_ad_cost.to_excel("{}dataframe_marketing_channels_{}_present.xlsx".format(path_tableau_data, lst_date[0]),
                        index=False, encoding=encoding)
    print("Exporting {}junseoyoon_performance_marketing.csv".format(path_tableau_data))
    df_ad_cost_with_metric.to_csv("{}junseoyoon_performance_marketing.csv".format(path_tableau_data), index=False)
    del df_ad_cost


def check_wth_marketing_data(path_input=None, input_file_nm=None, path_df_tableau=None, export_file_nm=None):
    import pandas as pd
    from sys import platform
    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    df = pd.read_excel("{}{}".format(path_input, input_file_nm), sheet_name="2020년 현황(채널상세)")
    df.dropna(axis=1, how="all", inplace=True)

    df_copy = df.copy()
    df_copy = df_copy.replace(r'\n', '', regex=True)
    for i in range(1, 5):
        val = df_copy.loc[0, "Unnamed: {}".format(i)]
        df_copy.rename(columns={"Unnamed: {}".format(i): val}, inplace=True)
        del i, val
    df_copy = df_copy.T
    df_copy[1] = df_copy[1].fillna(method="ffill")
    df_copy = df_copy.T
    for i in range(5, df_copy.shape[1]+1):
        val = df_copy.loc[1, "Unnamed: {}".format(i)] + df_copy.loc[2, "Unnamed: {}".format(i)]
        df_copy.rename(columns={"Unnamed: {}".format(i): "{}".format(val)}, inplace=True)
        del i, val
    df_copy = df_copy[[x for x in df_copy.columns.tolist() if "유효" not in x and "율" not in x and "률" not in x and
                       "2020년" not in x and "비용" not in x and "CPA" not in x and "12월" not in x and "11월" not in x]]
    df_copy = df_copy.loc[4:, :]
    df_copy["대상"] = df_copy["대상"].fillna(method="ffill")
    df_copy["대분류"] = df_copy["대분류"].fillna(method="ffill")
    df_copy["소분류"] = df_copy["소분류"].fillna(method="ffill")
    ########################################################################################################################
    df_marketing = pd.DataFrame()
    for i in range(1, 11):
        m = "{}".format(i)
        if len(m) == 1:
            m = "0"+m
        temp = df_copy[["대상", "대분류", "소분류", "채널명"] + [x for x in df_copy.columns.tolist() if "{}월".format(m) in x]].copy()
        temp.insert(0, "Date", "2020-{}".format(m))
        temp.rename(columns={"{}월인입(전체)".format(m): "인입_인입결제통계",
                             "{}월배송(전체 인입 기준)".format(m): "배송_인입결제통계",
                             "{}월로그인".format(m): "로그인_인입결제통계",
                             "{}월결제(전체 인입 기준)".format(m): "결제_인입결제통계",
                             "채널명": "채널"}, inplace=True)
        df_marketing = df_marketing.append(temp, sort=False, ignore_index=True)
        del i, m, temp
    df_marketing["채널"] = df_marketing["채널"].str.replace(" ", "")

    df_tableau = pd.read_csv(path_df_tableau)
    df_tableau = df_tableau[df_tableau["Date"].str.contains("2020")].reset_index(drop=True)
    df_tableau = df_tableau[(df_tableau["Date"] != "2020-11") & (df_tableau["Date"] != "2020-12")]
    df_tableau = df_tableau[["Date", "대상", "대분류", "소분류", "채널", "인입 여부", "배송 구분", "로그인 구분", "결제 여부"]].rename(
        columns={"인입 여부": "인입", "배송 구분": "배송", "로그인 구분": "로그인", "결제 여부": "결제"})

    df_merge = pd.merge(df_tableau, df_marketing, on=["Date", "대상", "대분류", "소분류", "채널"], how="outer")
    df_merge_notnull = df_merge[df_merge["인입"].notnull()].copy()
    df_merge_isnull = df_merge[df_merge["인입"].isnull()].copy()
    df_merge_isnull = df_merge_isnull[(df_merge_isnull["인입_인입결제통계"] != 0) |
                                      (df_merge_isnull["배송_인입결제통계"] != 0) |
                                      (df_merge_isnull["로그인_인입결제통계"] != 0) |
                                      (df_merge_isnull["결제_인입결제통계"] != 0)]
    for col in ["인입", "배송", "로그인", "결제"]:
        df_merge_isnull[col] = df_merge_isnull[col].fillna(0)
        del col
    df_merge = pd.concat([df_merge_notnull, df_merge_isnull], axis=0).sort_values(by=["Date"], ignore_index=True)
    for col in ["인입", "배송", "로그인", "결제"]:
        df_merge[col] = df_merge[col].astype(int)
        del col
    del df_merge_notnull, df_merge_isnull

    aa = df_merge[(df_merge["인입"] == 0) &
                  (df_merge["배송"] == 0) &
                  (df_merge["로그인"] == 0) &
                  (df_merge["결제"] == 0)].copy()
    aa = aa[(aa["인입_인입결제통계"].isnull()) &
            (aa["배송_인입결제통계"].isnull()) &
            (aa["로그인_인입결제통계"].isnull()) &
            (aa["결제_인입결제통계"].isnull())].copy()
    df_merge.drop(aa.index.tolist(), axis=0, inplace=True)
    del aa

    for col in ["인입", "배송", "로그인", "결제"]:
        df_merge["{}_차이".format(col)] = df_merge[col] - df_merge["{}_인입결제통계".format(col)]
        del col

    df_merge = df_merge[['Date', '대상', '대분류', '소분류', '채널',
                         '인입', '인입_인입결제통계', '인입_차이',
                         '배송', '배송_인입결제통계', '배송_차이',
                         '로그인', '로그인_인입결제통계', '로그인_차이',
                         '결제', '결제_인입결제통계', '결제_차이']]

    # df_merge.to_excel(, index=False)
    df_summary = pd.DataFrame()
    for dt in df_merge["Date"].unique():
        temp = df_merge[df_merge["Date"] == dt].copy()
        t = pd.DataFrame()
        for col in [x for x in temp.columns.tolist() if "차이" in x]:
            t = pd.concat([t, pd.DataFrame({col: [temp[col].sum()]})], axis=1)
            del col
        t.insert(0, "Date", dt)
        df_summary = df_summary.append(t, sort=False, ignore_index=True)
        del dt, temp, t

    with pd.ExcelWriter("{}".format(export_file_nm)) as writer:
        df_summary.to_excel(writer, sheet_name='incomebase 와 인입결제통계 데이터 차이', index=False)
        df_merge.to_excel(writer, sheet_name='데이터', index=False)


def prediction_income_base(folder_start_date=None, folder_end_date=None, path_import=None, path_export=None, current_date=None):
    if folder_start_date is None or folder_end_date is None:
        raise ValueError("Need to enter start and date folder name..")
    import pandas as pd
    from glob import glob
    from functools import reduce
    from calendar import monthrange
    from datetime import timedelta
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    col_g_channel = ["Date", "대상", "대분류", "소분류", "채널"]
    col_g_promotion = ["Date", "대상", "대분류", "소분류", "채널", "프로모션"]
    agg_col_leading = {'인입': 'sum'}
    agg_col_delivery = {'배송': 'sum', '로그인': 'sum', '결제': 'sum'}

    for dt in pd.date_range(start=folder_start_date, end=folder_end_date, freq="M"):
        # making date for income base folder and file ##################################################################
        dt_leading = dt.strftime("%Y-%m") # 인입한 회원 날짜 > dataframe column에 추가
        file_leading = dt_leading

        folder_first_update = (dt + timedelta(days=1)).strftime("%Y_%m") # 익월 폴더
        folder_second_update = (dt + timedelta(days=40)).strftime("%Y_%m") # 익악월 폴더
        file_first_update = folder_first_update.replace("_", "")
        file_second_update = folder_second_update.replace("_", "")
        if file_leading.split("-")[1] in ["01", "02", "03", "04", "05", "06", "07", "08", "09"]:
            file_leading = file_leading.split("-")[0] + "_" + file_leading.split("-")[1].replace("0", "")
        else:
            file_leading = file_leading.split("-")[0] + "_" + file_leading.split("-")[1]
        if folder_first_update.split("_")[1] in ["01", "02", "03", "04", "05", "06", "07", "08", "09"]:
            folder_first_update = folder_first_update.split("_")[0] + "_" + folder_first_update.split("_")[1].replace("0", "")
        else:
            folder_first_update = folder_first_update.split("_")[0] + "_" + folder_first_update.split("_")[1]
        if folder_second_update.split("_")[1] in ["01", "02", "03", "04", "05", "06", "07", "08", "09"]:
            folder_second_update = folder_second_update.split("_")[0] + "_" + folder_second_update.split("_")[1].replace("0", "")
        else:
            folder_second_update = folder_second_update.split("_")[0] + "_" + folder_second_update.split("_")[1]
        ################################################################################################################
        f_leading = "{}01_{}".format(file_first_update, file_leading)
        f_first_update = "{}01_{}_8".format(file_first_update, file_leading)
        f_second_update = "{}01_{}_8".format(file_second_update, file_leading)
        f_third_update = "{}{}_{}_8".format(file_second_update,
                                            monthrange(year=int(file_second_update[:4]), month=int(file_second_update[4:]))[1],
                                            file_leading)

        if folder_first_update == "2019_12":
            f_leading = "{}04_{}_8".format(file_first_update, file_leading)
        if folder_first_update == "2019_12":
            f_first_update = "{}04_{}_8".format(file_first_update, file_leading)
        if folder_second_update == "2019_12":
            f_second_update = "{}04_{}_8".format(file_second_update, file_leading)
        elif folder_second_update == "2019_11":
            f_third_update = "{}{}_{}_8".format(file_second_update, 29, file_leading)

        del file_leading, file_first_update, file_second_update
        del dt


        df_first = setting_indices(standard_date_delivery=f_first_update[:8], update_date_delivery=f_first_update[9:],
                                   standard_date_leading=f_leading[:8], update_date_leading=f_leading[9:],
                                   path_output="{}/file_directory/".format(root),
                                   path_leading="{}/".format(path_import+folder_first_update),
                                   path_delivery="{}/".format(path_import+folder_first_update))
        df_leading = df_first[0]
        df_delivery_first = df_first[1]
        del df_first
        df_leading_g_channel = df_leading.groupby(col_g_channel).agg(agg_col_leading).reset_index()
        df_delivery_first_g_channel = df_delivery_first.groupby(col_g_channel).agg(agg_col_delivery).reset_index()
        for col in list(agg_col_delivery.keys()):
            df_delivery_first_g_channel.rename(columns={col: "{}_1차".format(col)}, inplace=True)
            del col

        df_leading_g_promotion = df_leading.groupby(col_g_promotion).agg(agg_col_leading).reset_index()
        df_delivery_first_g_promotion = df_delivery_first.groupby(col_g_promotion).agg(agg_col_delivery).reset_index()
        for col in list(agg_col_delivery.keys()):
            df_delivery_first_g_promotion.rename(columns={col: "{}_1차".format(col)}, inplace=True)
            del col
        del df_leading, df_delivery_first


        df_delivery_second = pd.DataFrame()
        for file in glob("{}/*".format(path_import+folder_second_update)):
            if f_second_update in file:
                df_delivery_second = df_delivery_second.append(setting_indices(standard_date_delivery=f_second_update[:8],
                                                                               update_date_delivery=f_second_update[9:],
                                                                               standard_date_leading=f_leading[:8],
                                                                               update_date_leading=f_leading[9:],
                                                                               path_output="{}/file_directory/".format(root),
                                                                               path_leading="{}/".format(path_import+folder_first_update),
                                                                               path_delivery="{}/".format(path_import+folder_second_update))[1], sort=False)
            del file

        df_delivery_second_g_channel = pd.DataFrame()
        df_delivery_second_g_promotion = pd.DataFrame()
        if df_delivery_second.shape[0] != 0:
            df_delivery_second_g_channel_append = df_delivery_second.groupby(col_g_channel).agg(agg_col_delivery).reset_index()
            for col in list(agg_col_delivery.keys()):
                df_delivery_second_g_channel_append.rename(columns={col: "{}_2차".format(col)}, inplace=True)
                del col
            df_delivery_second_g_channel = df_delivery_second_g_channel.append(df_delivery_second_g_channel_append, sort=False, ignore_index=True)
            del df_delivery_second_g_channel_append

            df_delivery_second_g_promotion_append = df_delivery_second.groupby(col_g_promotion).agg(agg_col_delivery).reset_index()
            for col in list(agg_col_delivery.keys()):
                df_delivery_second_g_promotion_append.rename(columns={col: "{}_2차".format(col)}, inplace=True)
                del col
            df_delivery_second_g_promotion = df_delivery_second_g_promotion.append(df_delivery_second_g_promotion_append, sort=False, ignore_index=True)
            del df_delivery_second_g_promotion_append
        del df_delivery_second

        df_delivery_third = pd.DataFrame()
        for file in glob("{}/*".format(path_import + folder_second_update)):
            if f_third_update in file:
                df_delivery_third = df_delivery_third.append(setting_indices(standard_date_delivery=f_third_update[:8],
                                                                             update_date_delivery=f_third_update[9:],
                                                                             standard_date_leading=f_leading[:8],
                                                                             update_date_leading=f_leading[9:],
                                                                             path_output="{}/file_directory/".format(root),
                                                                             path_leading="{}/".format(path_import + folder_first_update),
                                                                             path_delivery="{}/".format(path_import + folder_second_update))[1], sort=False)
            del file

        df_delivery_third_g_channel = pd.DataFrame()
        df_delivery_third_g_promotion = pd.DataFrame()
        if df_delivery_third.shape[0] != 0:
            df_delivery_third_g_channel_append = df_delivery_third.groupby(col_g_channel).agg(agg_col_delivery).reset_index()
            for col in list(agg_col_delivery.keys()):
                df_delivery_third_g_channel_append.rename(columns={col: "{}_3차".format(col)}, inplace=True)
                del col
            df_delivery_third_g_channel = df_delivery_third_g_channel.append(df_delivery_third_g_channel_append, sort=False, ignore_index=True)
            del df_delivery_third_g_channel_append

            df_delivery_third_g_promotion_append = df_delivery_third.groupby(col_g_promotion).agg(agg_col_delivery).reset_index()
            for col in list(agg_col_delivery.keys()):
                df_delivery_third_g_promotion_append.rename(columns={col: "{}_3차".format(col)}, inplace=True)
                del col
            df_delivery_third_g_promotion = df_delivery_third_g_promotion.append(df_delivery_third_g_promotion_append,sort=False, ignore_index=True)
            del df_delivery_third_g_promotion_append
        del df_delivery_third

        lst_merge_df_channel = []
        lst_merge_df_promotion = []
        lst_merge_df_channel.append(df_leading_g_channel)
        lst_merge_df_promotion.append(df_leading_g_promotion)
        del df_leading_g_channel, df_leading_g_promotion

        for df in [df_delivery_first_g_channel, df_delivery_second_g_channel, df_delivery_third_g_channel]:
            if df.shape[0] != 0:
                lst_merge_df_channel.append(df)
            del df
        del df_delivery_first_g_channel, df_delivery_second_g_channel, df_delivery_third_g_channel
        for df in [df_delivery_first_g_promotion, df_delivery_second_g_promotion, df_delivery_third_g_promotion]:
            if df.shape[0] != 0:
                lst_merge_df_promotion.append(df)
            del df
        del df_delivery_first_g_promotion, df_delivery_second_g_promotion, df_delivery_third_g_promotion
        ################################################################################################################
        df_merge_channel = reduce(lambda left, right: pd.merge(left, right, on=col_g_channel, how="outer"),
                                  lst_merge_df_channel)
        df_merge_promotion = reduce(lambda left, right: pd.merge(left, right, on=col_g_promotion, how="outer"),
                                    lst_merge_df_promotion)
        for col in df_merge_promotion.columns.tolist()[df_merge_promotion.columns.tolist().index("인입"):]:
            df_merge_promotion[col] = df_merge_promotion[col].fillna(0).astype(int)
            del col
        ################################################################################################################
        file_nm_channel = "{}income_base_g_channel_{}_전체DB.xlsx".format(path_export, dt_leading)
        file_nm_promotion = "{}income_base_g_promotion_{}_전체DB.xlsx".format(path_export, dt_leading)

        print("Exporting {}".format(file_nm_channel))
        df_merge_channel.to_excel(file_nm_channel, index=False)

        print("Exporting {}".format(file_nm_promotion))
        df_merge_promotion.to_excel(file_nm_promotion, index=False)

        del df_merge_channel, df_merge_promotion, lst_merge_df_channel, lst_merge_df_promotion

    # 퍼포먼스파트가 알려주는 날짜 8시 데이터 사용 기준 (당일 00시 ~ 8시까지 인입자는 제외)
    df_current = setting_indices(standard_date_delivery=current_date.replace("-", ""),
                                 update_date_delivery="{}_8".format(current_date.replace("-", "_")[:-3]),
                                 standard_date_leading=current_date.replace("-", ""),
                                 update_date_leading="{}_8".format(current_date.replace("-", "_")[:-3]),
                                 path_output="{}/file_directory/".format(root),
                                 path_leading="{}/".format(path_import + current_date.replace("-", "_")[:-3]),
                                 path_delivery="{}/".format(path_import + current_date.replace("-", "_")[:-3]))
    df_current_leading_channel = df_current[0].copy()
    df_current_leading_promotion = df_current[0].copy()
    df_current_delivery_channel = df_current[1].copy()
    df_current_delivery_promotion = df_current[1].copy()
    del df_current

    print("Dropping 비회원ID where 인입일 == {}".format(current_date))
    rows_df_current_leading_channel = df_current_leading_channel.shape[0]
    rows_df_current_leading_promotion = df_current_leading_promotion.shape[0]
    rows_df_current_delivery_channel = df_current_delivery_channel.shape[0]
    rows_df_current_delivery_promotion = df_current_delivery_promotion.shape[0]

    df_current_leading_channel = df_current_leading_channel[df_current_leading_channel["인입일"] != current_date].copy().reset_index(drop=True)
    df_current_leading_promotion = df_current_leading_promotion[df_current_leading_promotion["인입일"] != current_date].copy().reset_index(drop=True)
    df_current_delivery_channel = df_current_delivery_channel[df_current_delivery_channel["인입일"] != current_date].copy().reset_index(drop=True)
    df_current_delivery_promotion = df_current_delivery_promotion[df_current_delivery_promotion["인입일"] != current_date].copy().reset_index(drop=True)

    print("Rows after dropping..\ndf_current_leading_channel: {} > {}\ndf_current_leading_promotion: {} > {}\ndf_current_delivery_channel: {} > {}\ndf_current_delivery_promotion: {} > {}".format(
        rows_df_current_leading_channel, df_current_leading_channel.shape[0],
        rows_df_current_leading_promotion, df_current_leading_promotion.shape[0],
        rows_df_current_delivery_channel, df_current_delivery_channel.shape[0],
        rows_df_current_delivery_promotion, df_current_delivery_promotion.shape[0]))
    del rows_df_current_leading_channel, rows_df_current_leading_promotion, rows_df_current_delivery_channel, rows_df_current_delivery_promotion

    df_current_leading_channel = df_current_leading_channel.groupby(col_g_channel).agg(agg_col_leading).reset_index()
    df_current_delivery_channel = df_current_delivery_channel.groupby(col_g_channel).agg(agg_col_delivery).reset_index()

    df_current_leading_promotion = df_current_leading_promotion.groupby(col_g_promotion).agg(agg_col_leading).reset_index()
    df_current_delivery_promotion = df_current_delivery_promotion.groupby(col_g_promotion).agg(agg_col_delivery).reset_index()

    for col in list(agg_col_delivery.keys()):
        df_current_delivery_channel.rename(columns={col: "{}_1차".format(col)}, inplace=True)
        df_current_delivery_promotion.rename(columns={col: "{}_1차".format(col)}, inplace=True)
        del col

    df_current_channel = reduce(lambda left, right: pd.merge(left, right, on=col_g_channel, how="outer"),
                                [df_current_leading_channel, df_current_delivery_channel])
    del df_current_leading_channel, df_current_delivery_channel
    df_current_promotion = reduce(lambda left, right: pd.merge(left, right, on=col_g_promotion, how="outer"),
                                [df_current_leading_promotion, df_current_delivery_promotion])
    del df_current_leading_promotion, df_current_delivery_promotion

    file_nm_current_channel = "{}income_base_g_channel_{}_전체DB.xlsx".format(path_export, current_date[:-3])
    file_nm_current_promotion = "{}income_base_g_promotion_{}_전체DB.xlsx".format(path_export, current_date[:-3])

    print("Exporting {}".format(file_nm_current_channel))
    df_current_channel.to_excel(file_nm_current_channel, index=False)

    print("Exporting {}".format(file_nm_current_promotion))
    df_current_promotion.to_excel(file_nm_current_promotion, index=False)


def prediction_export_excel(path_import=None, path_export=None):
    from glob import glob
    import pandas as pd
    from sys import platform

    # Choosing platform
    root = ""
    if platform == "darwin":
        root = "/Users/jy"
    elif platform == "win32":
        root = "D:"

    df_channel_total = pd.DataFrame()
    df_promotion_total = pd.DataFrame()

    print("Creating dataframe > channel, promotion")
    for file in sorted(glob("{}*".format(path_import))):
        if "channel" in file and "전체DB" in file:
            temp = pd.read_excel(file)
            if temp.shape[0] == 0:
                raise ValueError("The following file is not read\n{}".format(file))
            df_channel_total = df_channel_total.append(temp, sort=False, ignore_index=True)
            del temp
        elif "promotion" in file and "전체DB" in file:
            temp = pd.read_excel(file)
            if temp.shape[0] == 0:
                raise ValueError("The following file is not read\n{}".format(file))
            df_promotion_total = df_promotion_total.append(temp, sort=False, ignore_index=True)
            del temp

    df_channel_total = df_channel_total[['Date', '대상', '대분류', '소분류', '채널', '인입', '배송_1차', '배송_2차', '배송_3차',
                                         '로그인_1차', '로그인_2차', '로그인_3차', '결제_1차', '결제_2차', '결제_3차']]
    df_promotion_total = df_promotion_total[['Date', '대상', '대분류', '소분류', '채널', '프로모션', '인입', '배송_1차', '배송_2차', '배송_3차',
                                             '로그인_1차', '로그인_2차', '로그인_3차', '결제_1차', '결제_2차', '결제_3차']]

    for col in df_channel_total.columns.tolist()[df_channel_total.columns.tolist().index("인입"):]:
        df_channel_total[col] = df_channel_total[col].fillna(0).astype(int)
        del col
    for col in df_promotion_total.columns.tolist()[df_promotion_total.columns.tolist().index("인입"):]:
        df_promotion_total[col] = df_promotion_total[col].fillna(0).astype(int)
        del col

    df_channel_total.rename(columns={"인입": "인입수"}, inplace=True)
    df_promotion_total.rename(columns={"인입": "인입수"}, inplace=True)

    agg_col = {'인입수': "sum",
               '배송_1차': 'sum', '배송_2차': 'sum', '배송_3차': 'sum',
               '로그인_1차': 'sum', '로그인_2차': 'sum', '로그인_3차': 'sum',
               '결제_1차': 'sum', '결제_2차': 'sum', '결제_3차': 'sum'}
    df_channel_total_all = df_channel_total.groupby(['대상', '대분류', '소분류', '채널']).agg(agg_col).reset_index()
    df_promotion_total_all = df_promotion_total.groupby(['대상', '대분류', '소분류', '채널', '프로모션']).agg(agg_col).reset_index()
    del agg_col
    ###################################################
    file_nm_channel_total = "{}income_base_by_channel_전체DB.xlsx".format(path_export)
    print("Exporting {}".format(file_nm_channel_total))
    with pd.ExcelWriter(file_nm_channel_total) as writer:
        df_channel_total_all.to_excel(writer, sheet_name='총 합계', index=False)
        for dt in df_channel_total["Date"].unique():
            temp = df_channel_total[df_channel_total["Date"] == dt].copy().reset_index(drop=True)
            temp.to_excel(writer, sheet_name='{}'.format(dt), index=False)
            del dt,
    del file_nm_channel_total, df_channel_total_all, df_channel_total

    file_nm_promotion_total = "{}income_base_by_promotion_전체DB.xlsx".format(path_export)
    print("Exporting {}".format(file_nm_promotion_total))
    with pd.ExcelWriter(file_nm_promotion_total) as writer:
        df_promotion_total_all.to_excel(writer, sheet_name='총 합계', index=False)
        for dt in df_promotion_total["Date"].unique():
            temp = df_promotion_total[df_promotion_total["Date"] == dt].copy().reset_index(drop=True)
            temp.to_excel(writer, sheet_name='{}'.format(dt), index=False)
            del dt, temp
    del file_nm_promotion_total, df_promotion_total_all, df_promotion_total

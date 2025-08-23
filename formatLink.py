import json
import os
import threading
import time
from enum import Enum
import pandas as pd
import psycopg
import requests
from dotenv import load_dotenv
import json

load_dotenv()


class cs2ObjectType(Enum):
    case = 0
    gun = 1
    knife = 2
    sticker = 3


# Readies links for API retrieval
def formatLink(
    type: cs2ObjectType,
    weaponName: str,
    skinName: str,
    quality: str,
    isSouvenir: bool,
    isStatTrak: bool,
):
    urlBase = (
        "https://steamcommunity.com/market/pricehistory/?appid=730&market_hash_name="
    )
    statTrakText = "StatTrak™"
    knifeText = "★"

    # Cases/stickers return a simple link
    if type == cs2ObjectType.case or type == cs2ObjectType.sticker:
        return urlBase + weaponName
    # Weapons get more detailed
    if type == cs2ObjectType.knife:
        urlBase += knifeText + " "
    if isSouvenir:
        urlBase += "Souvenir "
    if isStatTrak:
        urlBase += statTrakText + " "
    return urlBase + weaponName + " | " + skinName + " (" + quality + ")"


# Split weapon + skin name into separate columns
def seperateNames(rowElement: str):
    weaponList = [
        "Glock-18",
        "P2000",
        "USP-S",
        "Dual Berettas",
        "P250",
        "Tec-9",
        "CZ75-Auto",
        "Five-SeveN",
        "Desert Eagle",
        "R8 Revolver",
        "MAC-10",
        "MP9",
        "MP7",
        "MP5-SD",
        "UMP-45",
        "P90",
        "PP-Bizon",
        "Nova",
        "XM1014",
        "Sawed-Off",
        "MAG-7",
        "M249",
        "Negev",
        "Galil AR",
        "FAMAS",
        "AK-47",
        "M4A4",
        "M4A1-S",
        "SSG 08",
        "SG 553",
        "AUG",
        "AWP",
        "G3SG1",
        "SCAR-20",
        "Zeus x27",
    ]
    splitText = rowElement.split(" ")
    if splitText[0] in weaponList:
        weaponName = splitText[0]
        skinName = " ".join(splitText[1:])
        return (weaponName, skinName)
    elif splitText[0] + " " + splitText[1] in weaponList:
        weaponName = splitText[0] + " " + splitText[1]
        skinName = " ".join(splitText[2:])
        return (weaponName, skinName)


# Creates a spreadsheet of all weapon skins in cs2
def fixSpreadSheet():
    sheet = "weaponSkins.csv"
    skinList = pd.read_csv(sheet)
    for index, row in skinList.iterrows():
        separatedNames = seperateNames(skinList.iloc[index, 0])
        skinList.iloc[index, 0] = separatedNames[0]  # type: ignore
        skinList.iloc[index, 1] = separatedNames[1]  # type: ignore
    return skinList.to_csv(sheet, index=False)


def collectionIsSouvenir(collection: str):
    souvenirKeywords = [
        "Dust 2",
        "Mirage",
        "Inferno",
        "Nuke",
        "Train",
        "Cobblestone",
        "Overpass",
        "Cache",
        "Safehouse",
        "Lake",
        "Italy",
        "Anubis",
        "Ancient",
        "Vertigo",
    ]
    for word in souvenirKeywords:
        if word in collection:
            return True
    return False


# Make an array for psycopg3 executemany requests
def make_batches(cur: psycopg.Cursor, data_to_insert: list):
    cur.executemany("INSERT INTO weapons (name) VALUES (%s)", data_to_insert[0])
    cur.executemany(
        "INSERT INTO collections (name, release_date) VALUES (%s, %s);",
        data_to_insert[1],
    )
    cur.executemany(
        "INSERT INTO skin (name, rarity, weapon_name) VALUES (%s, %s, %s) RETURNING name;",
        data_to_insert[2],
    )
    cur.executemany(
        "INSERT INTO skin_collections (skin_name, collection_name) VALUES (%s, %s);",
        data_to_insert[3],
    )
    cur.executemany(
        "INSERT INTO skin_instance (weapon_name, skin_name, exterior, stattrak, souvenir) VALUES (%s, %s, %s, %s, %s) RETURNING id;",
        data_to_insert[4],
    )
    cur.executemany(
        "INSERT INTO price_history (skin_instance_id, date, price, sold_volume_count) VALUES (%s, %s, %s, %s);",
        data_to_insert[5],
    )


def makeSteamRequests():
    class columns(Enum):
        weaponName = 0
        skinName = 1
        rarity = 2
        collection = 3
        release = 4

    qualities = [
        "Battle-Scarred",
        "Field-Tested",
        "Well-Worn",
        "Minimal Wear",
        "Factory New",
    ]

    sheet = "weaponSkins.csv"
    skinList = pd.read_csv(sheet)
    does_not_exist_arr, rate_limited_arr = [], []

    with open("skins.json", "w") as json_file:

        num_processed = 0

        for index, row in skinList.head(20).iterrows():
            weaponName = str(skinList.iloc[index, columns.weaponName.value])
            skinName = str(skinList.iloc[index, columns.skinName.value])
            rarity = str(skinList.iloc[index, columns.rarity.value])
            collection = str(skinList.iloc[index, columns.collection.value])
            raw_release_date = str(skinList.iloc[index, columns.release.value])
            release_date_obj = time.strptime(raw_release_date, "%d-%b-%y")
            formatted_release_date = f"{release_date_obj.tm_year}-{release_date_obj.tm_mon}-{release_date_obj.tm_mday}"
            base_start_time = time.perf_counter()
            for quality_index in range(len(qualities)):
                combination = {
                    "skin_instance_info": (
                        weaponName,
                        skinName,
                        rarity,
                        collection,
                        formatted_release_date,
                        qualities[quality_index],
                        quality_index,
                    )
                }
                if num_processed <= 69:
                    base_prices_data = get_skin_data(
                        weaponName,
                        skinName,
                        qualities,
                        quality_index,
                        does_not_exist_arr,
                        rate_limited_arr,
                    )
                    num_processed += 1
                    combination["price_data"] = base_prices_data
                    json.dump(combination, json_file, indent=4)

                else:
                    num_processed = 0
                    time.sleep(61)

    return


def populateDatabase():
    class columns(Enum):
        weaponName = 0
        skinName = 1
        rarity = 2
        collection = 3
        release = 4

    qualities = [
        "Battle-Scarred",
        "Field-Tested",
        "Well-Worn",
        "Minimal Wear",
        "Factory New",
    ]

    # 1. Access the skin list
    sheet = "weaponSkins.csv"
    skinList = pd.read_csv(sheet)

    db_user = os.getenv("username")
    db_password = os.getenv("password")
    db_host = os.getenv("host")
    db_port = os.getenv("port")
    db_name = os.getenv("database")
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode=require"
    print("opening database connection...")

    with psycopg.connect(connection_string) as conn:
        print("database connection created.")
        with conn.cursor() as cur:
            print("database cursor created.\n")

            does_not_exist_arr, rate_limited_arr = [], []
            weapon_name_set = set()
            skin_set = set()
            collection_set = set()
            skin_collections_set = set()
            skin_instances_tuples = []
            price_hist_tuples = []

            # 2. Select a row, grab type, weaponName, and skinName
            for index, row in skinList.head(1).iterrows():
                weaponName = str(skinList.iloc[index, columns.weaponName.value])
                skinName = str(skinList.iloc[index, columns.skinName.value])
                rarity = str(skinList.iloc[index, columns.rarity.value])
                collection = str(skinList.iloc[index, columns.collection.value])
                raw_release_date = str(skinList.iloc[index, columns.release.value])
                release_date_obj = time.strptime(raw_release_date, "%d-%b-%y")
                formatted_release_date = f"{release_date_obj.tm_year}-{release_date_obj.tm_mon}-{release_date_obj.tm_mday}"
                base_start_time = time.perf_counter()
                for quality_index in range(len(qualities)):
                    base_prices_data = get_skin_data(
                        weaponName,
                        skinName,
                        qualities,
                        quality_index,
                        does_not_exist_arr,
                        rate_limited_arr,
                    )
                    if base_prices_data:
                        weapon_name_set.add((weaponName,))
                        collection_set.add((collection, formatted_release_date))
                        skin_set.add((skinName, rarity, weaponName))
                        skin_collections_set.add((skinName, collection))
                        skin_instances_tuples.append(
                            (
                                weaponName,
                                skinName,
                                qualities[quality_index],
                                False,
                                False,
                            )
                        )
                        create_price_tuples(
                            base_prices_data,
                            price_hist_tuples,
                            len(skin_instances_tuples),
                        )
                        weapon_name_tuples = tuple(weapon_name_set)
                        collection_tuples = tuple(collection_set)
                        skin_tuples = tuple(skin_set)
                        skin_collections_tuples = tuple(skin_collections_set)
                        data_to_insert = [
                            weapon_name_tuples,
                            collection_tuples,
                            skin_tuples,
                            skin_collections_tuples,
                            skin_instances_tuples,
                            price_hist_tuples,
                        ]

            # convert weapon and collection sets to tuples

            make_batches(cur, data_to_insert)

            base_end_time = time.perf_counter()
            base_time_delta = base_end_time - base_start_time
            print(f"base skins completed! This took {base_time_delta} seconds\n")
            return

            if collectionIsSouvenir(collection):
                souvenir_link = formatLink(
                    cs2ObjectType.gun,
                    weaponName,
                    skinName,
                    qualities[quality_index],
                    True,
                    False,
                )

                # TESTING
                # print(baseLinks)
                # print(souvenirLinks)
                return
            else:
                stattrak_link = formatLink(
                    cs2ObjectType.gun,
                    weaponName,
                    skinName,
                    qualities[quality_index],
                    False,
                    True,
                )

        if len(does_not_exist_arr) > 0:
            print(
                f"The following URLs are items that dont exist:\n{does_not_exist_arr}\n"
            )
        if len(rate_limited_arr) > 0:
            print(f"The following URLs were rate limited:\n{rate_limited_arr}\n")
        return


def get_skin_data(
    weaponName: str,
    skinName: str,
    qualities: list,
    quality_index: int,
    does_not_exist_arr: list,
    rate_limited_arr: list,
):
    base_link = formatLink(
        cs2ObjectType.gun,
        weaponName,
        skinName,
        qualities[quality_index],
        False,
        False,
    )

    sessionID = os.getenv("sessionid", "")
    steamLoginSecure = os.getenv("steamloginsecure", "")
    cookies = {"sessionId": sessionID, "steamLoginSecure": steamLoginSecure}

    raw_data = requests.get(base_link, cookies=cookies)
    status_code = raw_data.status_code
    payload = json.loads(raw_data.content)
    base_prices_data = payload["prices"]
    print(f"status: {raw_data.status_code}    url: {base_link}")
    if base_prices_data is []:
        print(
            "Prices data is an empty array. Check your cookie credentials to ensure that they are correct."
        )
    # print(f"base prices data: {base_prices_data}")
    if status_code == 500:
        does_not_exist_arr.append(base_link)
    elif status_code == 429:
        rate_limited_arr.append(base_link)
    request_end_time = time.perf_counter
    return base_prices_data


def create_price_tuples(
    base_prices_data: list, price_hist_tuples: list, skin_instance_index: int
):
    for record in base_prices_data:
        # if instance_exists then get the most recent price_history record and start adding records from that day
        raw_date = record[0]
        median_sale_price = record[1]
        sale_volume_count = record[2]
        date_obj = time.strptime(raw_date[:-4], "%b %d %Y %H")
        date_string = f"{date_obj.tm_year}-{date_obj.tm_mon}-{date_obj.tm_mday}"
        price_hist_tuples.append(
            (
                str(skin_instance_index),
                str(date_string),
                str(median_sale_price),
                str(sale_volume_count),
            )
        )
        # print(f"{skin_instance_index, date_string, median_sale_price, sale_volume_count}")


# fixSpreadSheet()
makeSteamRequests()
# populateDatabase()
# print(make_batches("weaponSkins.csv", 0, 0, 100))

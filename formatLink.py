import json
import os
import time
from enum import Enum

import pandas as pd  # type: ignore
import psycopg
import requests
from dotenv import load_dotenv

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
def seperateNames(rowElement):
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


def writeItemEntry():
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

            # 2. Select a row, grab type, weaponName, and skinName
            for index, row in skinList.head(1).iterrows():
                weaponName = str(skinList.iloc[index, columns.weaponName.value])
                skinName = str(skinList.iloc[index, columns.skinName.value])
                rarity = str(skinList.iloc[index, columns.rarity.value])
                collection = str(skinList.iloc[index, columns.collection.value])
                raw_release_date = str(skinList.iloc[index, columns.release.value])
                release_date_obj = time.strptime(raw_release_date, "%d-%b-%y")
                formatted_release_date = f"{release_date_obj.tm_year}-{release_date_obj.tm_mon}-{release_date_obj.tm_mday}"
                # 3. generate a list of formattedLinks by by rarity. Bear in mind you have to distinguish between souvenir and statTrak

                # Changes to make
                # x1. add timer for keeping track of rate limiting
                # x2. no appends instead we will do the database query at time of link generation (completed)
                # 3. add exception to return the
                # x4. keep track of 500 (does not exist), 429 (rate limited), and any other errors in a csv
                # 5. track the number of cocurrent requests

                # BASE KNIFES ARE CALLED VANILLA

                baseLinks, souvenirLinks, statTrakLinks = [], [], []
                does_not_exist_arr, rate_limited_arr = [], []
                # open a connection to the database
                for quality_index in range(len(qualities)):
                    base_start_time = time.perf_counter()
                    base_prices_data = get_skin_data(
                        weaponName,
                        skinName,
                        qualities,
                        quality_index,
                        does_not_exist_arr,
                        rate_limited_arr,
                    )
                    if base_prices_data:
                        update_weapons(cur, weaponName)
                        update_collections(cur, collection, formatted_release_date)
                        update_skins(cur, skinName, rarity, weaponName)
                        update_skin_collections(cur, skinName, collection)
                        update_skin_instance(
                            cur,
                            weaponName,
                            skinName,
                            qualities,
                            quality_index,
                            False,
                            False,
                            base_prices_data,
                        )
                    # create the try except logic for saving the last index pairing and list of 500/429 urls
                    base_end_time = time.perf_counter()
                    base_time_delta = base_end_time - base_start_time
                    print(
                        f"base skins completed! This took {base_time_delta} seconds\n"
                    )
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

                    print(f"\n\n#########\n\n")

                if len(does_not_exist_arr) > 0:
                    print(
                        f"The following URLs are items that dont exist:\n{does_not_exist_arr}\n"
                    )
                if len(rate_limited_arr) > 0:
                    print(
                        f"The following URLs were rate limited:\n{rate_limited_arr}\n"
                    )
                # TESTING
                # print(baseLinks)
                # print(souvenirLinks)
                # print(statTrakLinks)
                print(index)
                return

            # 4. Iteratively make an API request for each formattedLink, a lot of these will not work

            # IDEAS
            # Suppose we get a set number of faulty requests. When a limit is reached, check the end points of each array to see
            # where we left off. Get that index/location of the last good request
            #  and return it. Use as input of function as starting point for link
            # generation.

            # 5. Access the dictonary and contain the data in a spreadsheet for further development
            return


def get_skin_data(
    weaponName, skinName, qualities, quality_index, does_not_exist_arr, rate_limited_arr
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
    return base_prices_data


def update_weapons(cur, weaponName):
    weapon_check = "SELECT name FROM weapons WHERE name = %s;"
    cur.execute(weapon_check, (weaponName,))
    weapon_exists = cur.fetchone()
    print(f"weapon check: {weapon_exists}")
    if weapon_exists:
        print(f"weapon name: {weapon_exists}")
    else:
        print(f"Need to create record for the weapon: {weaponName}...")
        create_weapon = "INSERT INTO weapons (name) VALUES (%s)"
        cur.execute(create_weapon, (weaponName,))
        print("...weapon created")


def update_collections(cur, collection, formatted_release_date):
    collections_check = "SELECT name FROM collections WHERE name = %s;"
    cur.execute(collections_check, (collection,))
    collection_name = cur.fetchone()
    if collection_name:
        print(f"collection name: {collection_name}")
    else:
        print(f"Need to create record for the collection: {collection}...")
        create_collection = (
            "INSERT INTO collections (name, release_date) VALUES (%s, %s);"
        )
        cur.execute(
            create_collection,
            (
                collection,
                formatted_release_date,
            ),
        )
        print("...collection created")


def update_skins(cur, skinName, rarity, weaponName):
    skin_check = "SELECT name FROM skin WHERE name = %s;"
    cur.execute(skin_check, (skinName,))
    skin_id = cur.fetchone()
    if skin_id:
        print(f"skin name: {skin_id}")
    else:
        print(f"Need to create record for the skin: {skinName}...")
        create_skin = "INSERT INTO skin (name, rarity, weapon_name) VALUES (%s, %s, %s) RETURNING name;"
        cur.execute(
            create_skin,
            (skinName, rarity, weaponName),
        )
        print("...skin created")


def update_skin_collections(cur, skinName, collection):
    skin_collections_check = "SELECT skin_name, collection_name FROM skin_collections WHERE skin_name = %s AND collection_name = %s;"
    cur.execute(
        skin_collections_check,
        (
            skinName,
            collection,
        ),
    )
    item_exists = cur.fetchone()
    if item_exists:
        print(item_exists)
    else:
        print(
            f"Need to create the colleciton record for {skinName} and {collection}..."
        )
        create_skin_collection = (
            "INSERT INTO skin_collections (skin_name, collection_name) VALUES (%s, %s);"
        )
        cur.execute(
            create_skin_collection,
            (
                skinName,
                collection,
            ),
        )
        print(f"...skin+collection record created")


def update_skin_instance(
    cur,
    weaponName,
    skinName,
    qualities,
    quality_index,
    isStatTrak,
    isSouvenir,
    base_prices_data,
):
    skin_instance_check = "SELECT weapon_name, skin_name, exterior, stattrak, souvenir FROM skin_instance WHERE weapon_name = %s AND skin_name = %s AND exterior = %s AND stattrak = %s AND souvenir = %s"
    cur.execute(
        skin_instance_check,
        (weaponName, skinName, qualities[quality_index], isStatTrak, isSouvenir),
    )
    instance_exists = cur.fetchone()
    if instance_exists:
        print(instance_exists)
    create_skin_instance = "INSERT INTO skin_instance (weapon_name, skin_name, exterior, stattrak, souvenir) VALUES (%s, %s, %s, %s, %s) RETURNING id;"
    cur.execute(
        create_skin_instance,
        (
            weaponName,
            skinName,
            qualities[quality_index],
            isStatTrak,
            isSouvenir,
        ),
    )
    skin_instance_id = cur.fetchone()[0]
    print(f"skin_instance_id: {skin_instance_id}\n")

    # iterate over price history and create a batch of price_history objects to send to the database
    # once the operations are complete, end the timer and see if it falls outside of the 3.5 second window, if not then sleep until it does

    for record in base_prices_data:
        # if instance_exists then get the most recent price_history record and start adding records from that day
        raw_date = record[0]
        median_sale_price = record[1]
        sale_volume_count = record[2]
        date_obj = time.strptime(raw_date[:-4], "%b %d %Y %H")
        date_string = f"{date_obj.tm_year}-{date_obj.tm_mon}-{date_obj.tm_mday}"
        print(
            f"date: {raw_date}, median: {median_sale_price}, volume: {sale_volume_count}"
        )
        print(f"{date_string}")
        create_price_history = f"INSERT INTO price_history (skin_instance_id, date, price, sold_volume_count) VALUES (%s, %s, %s, %s);"
        cur.execute(
            create_price_history,
            (
                skin_instance_id,
                date_string,
                median_sale_price,
                sale_volume_count,
            ),
        )


# fixSpreadSheet()
writeItemEntry()

import time
from enum import Enum

import pandas as pd  # type: ignore
import requests


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


cookies = {
    "sessionId": "a1d0927e052666febaac2074",
    "steamLoginSecure": "76561198123389177%7C%7CeyAidHlwIjogIkpXVCIsICJhbGciOiAiRWREU0EiIH0.eyAiaXNzIjogInI6MDAwQ18yNkIyRUM1RF80MzI1RCIsICJzdWIiOiAiNzY1NjExOTgxMjMzODkxNzciLCAiYXVkIjogWyAid2ViOmNvbW11bml0eSIgXSwgImV4cCI6IDE3NTQ2MTgwMzAsICJuYmYiOiAxNzQ1ODkxNTIyLCAiaWF0IjogMTc1NDUzMTUyMiwgImp0aSI6ICIwMDE5XzI2QkMyN0RGXzNFOUZFIiwgIm9hdCI6IDE3NTM4MzY3NzksICJydF9leHAiOiAxNzcxNjk2ODgzLCAicGVyIjogMCwgImlwX3N1YmplY3QiOiAiMTY2LjE5Ni42MS4xMTAiLCAiaXBfY29uZmlybWVyIjogIjE3Mi41OC41MS42NCIgfQ.d0HMGnzZ7YGQ6a2eqyGj7__iT1K0EgY6iryQtg_gFhVKDYia5XgAKjruQ4mhwToPxEk__kiYl2cTMCohobJJAg",
}


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
    # 2. Select a row, grab type, weaponName, and skinName
    for index, row in skinList.iterrows():
        weaponName = str(skinList.iloc[index, columns.weaponName.value])
        skinName = str(skinList.iloc[index, columns.skinName.value])
        collection = str(skinList.iloc[index, columns.collection.value])
        # 3. generate a list of formattedLinks by by rarity. Bear in mind you have to distinguish between souvenir and statTrak

        # Changes to make
        # 1. add timer for keeping track of rate limiting
        # 2. no appends instead we will do the database query at time of link generation (completed)
        # 3. add exception to return the
        # 4. keep track of 500 (does not exist), 429 (rate limited), and any other errors in a csv
        # 5. track the number of cocurrent requests

        baseLinks, souvenirLinks, statTrakLinks = [], [], []
        does_not_exist_arr, rate_limited_arr = [], []
        # open a connection to the database
        for i in range(len(qualities)):
            base_start_time = time.perf_counter()
            base_link = formatLink(
                cs2ObjectType.gun, weaponName, skinName, qualities[i], False, False
            )
            print(base_link)
            base_data = requests.get(base_link, cookies=cookies)
            print(base_data.status_code)
            print(base_data.content)
            if base_data.status_code == 500:
                does_not_exist_arr.append(base_link)
            elif base_data.status_code == 429:
                rate_limited_arr.append(base_link)
            else:
                # make reqeust to database
                # check if weapon, collection, and skin exist
                # if they dont create them
                # create a skin instance with the skinName, weaponName, exterior quality, and stattrack/souvenir
                # iterate over price history and create a batch of price_history objects to send to the database
                # once the operations are complete, end the timer and see if it falls outside of the 3.5 second window, if not then sleep until it does
                print()

                # create the try except logic for saving the last index pairing and list of 500/429 urls

            print(f"\n\n#########\n\n")

            if collectionIsSouvenir(collection):
                souvenir_link = formatLink(
                    cs2ObjectType.gun,
                    weaponName,
                    skinName,
                    qualities[i],
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
                    qualities[i],
                    False,
                    True,
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


# fixSpreadSheet()
writeItemEntry()

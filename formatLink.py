from enum import Enum
import pandas as pd

class cs2ObjectType(Enum):
    case = 0
    gun = 1
    knife = 2
    sticker = 3

# Readies links for API retrieval
def formatLink(type: cs2ObjectType, weaponName: str, skinName: str, quality: str,  isSouvenir: bool, isStatTrak: bool):
    urlBase = "https://steamcommunity.com/market/pricehistory/?appid=730&market_hash_name="
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
    weaponList = ["Glock-18", "P2000", "USP-S", "Dual Berettas", "P250", "Tec-9", "CZ75-Auto", "Five-SeveN", "Desert Eagle", "R8 Revolver",
                 "MAC-10", "MP9", "MP7", "MP5-SD", "UMP-45", "P90", "PP-Bizon", "Nova", "XM1014", "Sawed-Off", "MAG-7", "M249", "Negev", "Galil AR",
                 "FAMAS" , "AK-47", "M4A4", "M4A1-S", "SSG 08", "SG 553", "AUG", "AWP", "G3SG1", "SCAR-20", "Zeus x27"]
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
        skinList.iloc[index, 0] = separatedNames[0] # type: ignore
        skinList.iloc[index, 1] = separatedNames[1] # type: ignore
    return skinList.to_csv(sheet, index = False)

def writeItemEntry():
    # 1. Access the skin list
    # 2. Select a row, grab type, weaponName, and skinName 
    # 3. generate a list of formattedLinks by by rarity. Bear in mind you have to distinguish between souvenir and statTrak
    # 4. Iteratively make an API request for each formattedLink, (non-existent floats are an edge case here)
    # 5. Access the dictonary and contain the data in a spreadsheet for further development
    return

fixSpreadSheet()


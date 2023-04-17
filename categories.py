import json
import lzma
import re
from json import JSONDecodeError

import requests

ADDITIONAL_CATEGORIES = ['ArcaneHelmets', 'Imprints', 'VeiledRivenMods', 'Emotes',
                         'ArmorPieces', "CollectorItems"]

OVERRIDES = {
    'Prisma Dual Decurions': 'Prisma Dual Decurion'
}

MANUAL_CATEGORIES = {
    'Scan Aquatic Lifeforms': 'Mods',
    "Kavasa Prime Kubrow Collar Blueprint": "Misc",
    'Equilibrium (Steam Pinnacle Pack)': 'CollectorItems',
    'Corpus Void Key': 'CollectorItems',
    'Vay Hek Frequency Triangulator': 'CollectorItems',
    'Ancient Fusion Core': 'CollectorItems',
    'Delta Beacon': 'CollectorItems',
    'Gamma Beacon': 'CollectorItems',
    'Kappa Beacon': 'CollectorItems',
    'Omega Beacon': 'CollectorItems',
    'Damaged Necramech Set': 'Misc',
    'Damaged Necramech Weapon Set': 'Misc',
    'Suda Armor Set': 'ArmorPieces',
    'Lokan Armor Set': 'ArmorPieces',
    'Red Veil Armor Set': 'ArmorPieces',
    'Solaris Armor Set': 'ArmorPieces',
    'Meridian Armor Set': 'ArmorPieces',
    'Perrin Armor Set': 'ArmorPieces',
    'Ostron Armor Set': 'ArmorPieces',
    'Hexis Armor Set': 'ArmorPieces',
    'Kavasa Prime Kubrow Collar Set': 'Misc'
}

ARMOR_TYPES = [
    'Chest Plate', 'Arm Spurs', 'Arm Plates', 'Leg Spurs', 'Arm Guards', 'Leg Guards',
    'Chest Marker', 'Knee Guards', 'Chest Piece', 'Spurs', 'Chest Guard', "Arm Insignia"
]

CATEGORY_MAPPINGS = [
    (re.compile(r"Arcane.*Helmet"), 'ArcaneHelmets'),
    (re.compile(r".*Imprint"), 'Imprints'),
    (re.compile(r".*Augment Mod"), 'Mods'),
    (re.compile(r".*(Veiled)"), 'VeiledRivenMods'),
    (re.compile(r".*Emote"), 'Emotes'),
    (re.compile("|".join(ARMOR_TYPES)), 'ArmorPieces'),
    (re.compile(r"(.*) Set"), lambda match, d: d.get(match.group(1))),
]

def find_category(item, flattened_type_dict):
    # Check if the item is in MANUAL_CATEGORIES
    if item in MANUAL_CATEGORIES:
        return MANUAL_CATEGORIES[item]

    # Returns the next item in the iterable that evaluates to True
    # If no item evaluates to True, returns 'Misc'
    # Uses CATEGORY_MAPPINGS to determine the category
    # If the item matches a regex pattern, the category is returned
    # If item is a set, find category the base item belongs to
    # Otherwise, returns 'Misc'.
    match, category = next(((pattern.match(item), category)
                            for pattern, category
                            in CATEGORY_MAPPINGS
                            if pattern.match(item)),
                           (None, 'Misc'))
    return category(match, flattened_type_dict) if callable(category) else category


def process_item(item, item_type, wfm_item_data, wfm_item_data_lower, wfm_items_categorized, all_items, overrides, manual_categories):
    if item in overrides:
        item = overrides[item]

    if item in manual_categories:
        return

    original_item_name = wfm_item_data_lower.get(item.lower())

    if original_item_name:
        all_items.add(original_item_name)
        wfm_items_categorized[item_type][original_item_name] = wfm_item_data[original_item_name]
    else:
        all_items.add(item)


def get_wfm_item_categorized(wfm_item_data, manifest_list, wf_parser):
    type_dict = gen_type_dict(manifest_list, wf_parser)
    wfm_item_data_lower = {k.lower(): k for k, v in wfm_item_data.items()}
    all_items = set()

    wfm_items_categorized = {k: {} for k in list(type_dict) + ADDITIONAL_CATEGORIES}

    for item_type, items in type_dict.items():
        for item in items:
            process_item(item, item_type, wfm_item_data, wfm_item_data_lower, wfm_items_categorized, all_items, OVERRIDES, MANUAL_CATEGORIES)

    flattened_type_dict = {item: item_type for item_type, items in type_dict.items() for item in items}
    uncategorized_items = set(wfm_item_data.keys()) - all_items

    for item in uncategorized_items:
        category = find_category(item, flattened_type_dict)
        wfm_items_categorized[category][item] = wfm_item_data[item]

    wfm_items_categorized = {item_type: items for item_type, items in sorted(wfm_items_categorized.items()) if items}

    return wfm_items_categorized


def parse_rarity(relic_name):
    rarities = {
        "Bronze": "Intact",
        "Silver": "Exceptional",
        "Gold": "Flawless",
        "Platinum": "Radiant"
    }
    for key in rarities:
        if relic_name.endswith(key):
            return rarities[key]
    return ""


def fix():
    response = requests.get('https://content.warframe.com/PublicExport/index_en.txt.lzma')
    data = response.content
    byt = bytes(data)
    length = len(data)
    stay = True
    while stay:
        stay = False
        try:
            decompress_lzma(byt[0:length])
        except lzma.LZMAError:
            length -= 1
            stay = True

    return decompress_lzma(byt[0:length]).decode("utf-8")


# FROM: https://stackoverflow.com/a/37400585/15041587
def decompress_lzma(data):
    results = []
    while True:
        decomp = lzma.LZMADecompressor(lzma.FORMAT_AUTO, None, None)
        try:
            res = decomp.decompress(data)
        except lzma.LZMAError:
            if results:
                break  # Leftover data is not a valid LZMA/XZ stream; ignore it.
            else:
                raise  # Error on the first iteration; bail out.
        results.append(res)
        data = decomp.unused_data
        if not data:
            break
        if not decomp.eof:
            raise lzma.LZMAError("Compressed data ended before the end-of-stream marker was reached")
    return b"".join(results)


def get_node_list():
    nodes = requests.get('https://relics.run/json/solNodes.json').json()

    return nodes


def parse_name(name, parser):
    if name in parser:
        if isinstance(parser[name], dict):
            mission_node = parser[name]['node']
            if 'planet' in parser[name]:
                mission_node += f" - {parser[name]['planet']}"
            return mission_node
        else:
            if parser[name] == '':
                print(name)

            if parser[name] in parser:
                return parser[parser[name]]
            else:
                return parser[name]
    else:
        return name


def build_parser(manifest_list):
    parser_base = {'AP_POWER': 'Zenurik',
                   'AP_TACTIC': 'Naramon',
                   'AP_DEFENSE': 'Vazarin',
                   'AP_PRECEPT': 'Penjaga',
                   'AP_ATTACK': 'Madurai',
                   'AP_UMBRA': 'Umbra',
                   'AP_WARD': 'Unairu',
                   'AP_ANY': 'Aura',
                   'AP_UNIVERSAL': 'Universal',
                   '/Lotus/Types/Items/Research/DojoColors/GenericDojoColorPigment': 'Dojo Color Pigment',
                   '/Lotus/Types/Sentinels/SentinelPrecepts/SwiftDeth': 'Swift Deth',
                   '/Lotus/Types/Sentinels/SentinelPrecepts/BoomStick': 'Striker',
                   '/Lotus/Types/Sentinels/SentinelPrecepts/Warrior': 'Warrior',
                   '/Lotus/Upgrades/Mods/Warframe/AvatarPickupBonusMod': 'Equilibrium',
                   '/Lotus/Upgrades/CosmeticEnhancers/Offensive/PrimaryDamageOnKill': 'Primary Merciless',
                   '/Lotus/Upgrades/CosmeticEnhancers/Offensive/SecondaryDamageOnKill': 'Secondary Merciless',
                   '/Lotus/Upgrades/CosmeticEnhancers/Offensive/PrimaryDamageOnMeleeKill': 'Primary Dexterity',
                   '/Lotus/Upgrades/CosmeticEnhancers/Offensive/SecondaryDamageOnNoMelee': 'Secondary Deadhead',
                   '/Lotus/Upgrades/CosmeticEnhancers/Offensive/PrimaryDamageOnNoMelee': 'Primary Deadhead',
                   '/Lotus/Upgrades/CosmeticEnhancers/Zariman/SecondaryOnRollCritChanceOnHeadshot': 'Cascadia Accuracy',
                   '/Lotus/Upgrades/CosmeticEnhancers/Zariman/SecondaryOnStatusProcBonusDamage': 'Cascadia Empowered',
                   '/Lotus/Types/Items/Events/TennoConRelay2019EarlyAccess': 'Tennocon 2019 Ticket',
                   '/Lotus/Types/Game/ShipScenes/ChristmasScene': 'Christmas Orbiter Decorations',
                   '/Lotus/Types/Items/Events/TennoConRelay2020EarlyAccess': 'Tennocon 2020 Ticket',
                   '/Lotus/Types/StoreItems/AvatarImages/FanChannel/AvatarImageRebelDustyPinky': 'RebelDustyPinky Glyph',
                   '/Lotus/Types/Recipes/Components/VorBoltRemoverFakeItem': 'Ascaris',
                   '/Lotus/Types/Sentinels/SentinelPrecepts/TnCrossAttack': 'Retarget',
                   '/Lotus/Types/Sentinels/SentinelPrecepts/LocateCreatures': 'Scan Lifeforms',
                   '/Lotus/Weapons/SolarisUnited/Primary/LotusModularPrimaryBeam': 'Primary Gaze',
                   '/Lotus/Types/Recipes/CosmeticUnenhancerItem': 'CosmeticUnenhancerItem',
                   '/Lotus/Types/Items/MiscItems/BasicMiscItem': 'BasicMiscItem',
                   '/Lotus/Weapons/SolarisUnited/Secondary/LotusModularSecondaryShotgun': 'Kitgun',
                   '/Lotus/Weapons/SolarisUnited/Secondary/LotusModularSecondary': 'Kitgun',
                   '/Lotus/Weapons/Ostron/Melee/LotusModularWeapon': 'Zaw',
                   '/Lotus/Weapons/SolarisUnited/Secondary/LotusModularSecondaryBeam': 'Kitgun',
                   '/Lotus/Powersuits/Wraith/Reaper': 'Shadow Claws',
                   '/Lotus/Powersuits/Yareli/BoardSuit': 'Merulina',
                   '/Lotus/Types/Friendly/Pets/MoaPets/MoaPetPowerSuit': 'Moa',
                   '/Lotus/Types/Game/CrewShip/RailJack/DefaultHarness': 'Railjack Harness',
                   '/Lotus/Types/Gameplay/InfestedMicroplanet/EncounterObjects/TestPartItem': 'TestPartItem',
                   '/Lotus/Upgrades/CosmeticEnhancers/Offensive/SecondaryDamageOnMeleeKill': 'Secondary Dexterity',
                   '/Lotus/Upgrades/CosmeticEnhancers/Zariman/PrimaryOnAbilityReloadSpeed': 'Fractalized Reset',
                   '/Lotus/Upgrades/CosmeticEnhancers/Zariman/SecondaryOnOvershieldCritChance': 'Cascadia Overcharge',
                   '/Lotus/Upgrades/CosmeticEnhancers/Offensive/SecondaryDamageOnHeatProc': 'Cascadia Flare',
                   '/Lotus/Types/Gameplay/InfestedMicroplanet/EncounterObjects/TestPartItem': 'TestPartItem',
                   '/Lotus/Types/Game/ShipScenes/PrimeLisetFiligreeScene': 'Filigree Prime',
                   '/Lotus/Weapons/SolarisUnited/Primary/LotusModularPrimary': 'Kitgun',
                   '/Lotus/Types/Friendly/Pets/ZanukaPets/ZanukaPetCPowerSuit': 'Hound',
                   '/Lotus/Types/Friendly/Pets/ZanukaPets/ZanukaPetBPowerSuit': 'Hound',
                   '/Lotus/Upgrades/Mods/Fusers/LegendaryModFuser': 'Legendary Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyUncommonLvl1ModFuser': 'U1 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyRareLvl2ModFuser': 'R2 Ancient Fusion Core',
                   '/Lotus/Types/Items/Events/TennoConRelay2021EarlyAccess': 'Tennocon 2021 Ticket',
                   '/Lotus/Weapons/SolarisUnited/Primary/LotusModularPrimaryShotgun': 'Kitgun',
                   '/Lotus/Types/Friendly/Pets/ZanukaPets/ZanukaPetAPowerSuit': 'Hound',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyCommonLvl10ModFuser': 'C10 Ancient Fusion Core',
                   '/Lotus/Types/StoreItems/AvatarImages/AvatarImageCreatorDogManDan': 'DogManDan Glyph',
                   '/Lotus/Types/Items/Events/RelayReconstructionEarlyAccess': 'Relay Reconstruction Early Access',
                   '/Lotus/Types/StoreItems/AvatarImages/FanChannel/AvatarImageEltioProd': 'EltioProd Glyph',
                   '/Lotus/Types/StoreItems/AvatarImages/AvatarImageDayJoBo': 'DayJoBo Glyph',
                   '/Lotus/Types/StoreItems/AvatarImages/AvatarImageAHR': 'AHR Glyph',
                   '/Lotus/Types/StoreItems/AvatarImages/AvatarImageSenastra': 'Senastra Glyph',
                   '/Lotus/Types/StoreItems/AvatarImages/AvatarImageSilentMashiko': 'Silent Mashiko Glyph',
                   '/Lotus/Types/Items/Events/TennoConRelay2018EarlyAccess': 'Tennocon 2018 Ticket',
                   '/Lotus/Types/StoreItems/AvatarImageItem/GuardianCon2018Glyph': 'GuardianCon 2018 Glyph',
                   '/Lotus/Types/StoreItems/AvatarImages/GlyphErisTennocon2020Mech': 'Tenncon 2020 Glyph',
                   '/Lotus/Upgrades/Mods/Warframe/Intermediate/AvatarPickupBonusModIntermediate': 'Equilibrium (Steam Pinnacle Pack)',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyCommonLvl3ModFuser': 'C3 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyUncommonLvl5ModFuser': 'U5 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyCommonLvl8ModFuser': 'C8 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyUncommonLvl8ModFuser': 'U8 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyUncommonLvl10ModFuser': 'U10 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyCommonLvl5ModFuser': 'C5 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyRareLvl3ModFuser': 'R3 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyRareLvl10ModFuser': 'R10 Ancient Fusion Core',
                   '/Lotus/Types/Game/ShipScenes/NidusPrimeScene': 'Nidus Prime Scene',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyCommonLvl6ModFuser': 'C6 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyCommonLvl7ModFuser': 'C7 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyCommonLvl9ModFuser': 'C9 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyCommonLvl1ModFuser': 'C1 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyCommonLvl2ModFuser': 'C2 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyCommonLvl4ModFuser': 'C4 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyUncommonLvl9ModFuser': 'U9 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyUncommonLvl3ModFuser': 'U3 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyUncommonLvl4ModFuser': 'U4 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyUncommonLvl6ModFuser': 'U6 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyUncommonLvl7ModFuser': 'U7 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyRareLvl6ModFuser': 'R6 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyRareLvl7ModFuser': 'R7 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyRareLvl9ModFuser': 'R9 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyUncommonLvl2ModFuser': 'U2 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyRareLvl1ModFuser': 'R1 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyRareLvl4ModFuser': 'R4 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyRareLvl8ModFuser': 'R8 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/LegacyRareLvl5ModFuser': 'R5 Ancient Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/RareLvl5ModFuser': 'R5 Fusion Core',
                   '/Lotus/Upgrades/Mods/Fusers/UncommonModFuser': 'U0 Fusion Core',
                   'LPS_COMMAND': 'Command',
                   'LPP_SPACE': 'Points',
                   'LPS_GUNNERY': 'Gunnery',
                   'LPS_ENGINEERING': 'Engineering',
                   'LPS_TACTICAL': 'Tactical',
                   'LPS_PILOTING': 'Piloting',
                   '/Lotus/Types/Vehicles/Hoverboard/HoverboardSuit': 'K-Drive',
                   '/Lotus/Weapons/Tenno/HackingDevices/TnHackingDevice/TnHackingDeviceWeapon': 'Parazon',
                   '/Lotus/Types/Keys/VeyHekKeyBlueprint': 'Vay Hek Key Blueprint',
                   '/Lotus/Types/Boosters/ResourceDropChanceBooster': 'Resource Drop Chance',
                   '/Lotus/Types/Boosters/CreditBooster': 'Credit',
                   '/Lotus/Types/Boosters/ResourceAmountBooster': 'Resource',
                   '/Lotus/Types/Boosters/AffinityBooster': 'Affinity',
                   '/Lotus/Types/Boosters/ModDropChanceBooster': 'Mod Drop Chance',
                   '/Lotus/Types/Boosters/AffinityBlessing': 'Affinity Blessing',
                   '/Lotus/Types/Boosters/ResourceDropChanceBlessing': 'Resource Drop Chance Blessing',
                   '/Lotus/Types/Boosters/HealthBlessing': 'Health Blessing',
                   '/Lotus/Types/Boosters/CreditBlessing': 'Credit Blessing',
                   '/Lotus/Types/Boosters/DamageBlessing': 'Damage Blessing',
                   '/Lotus/Types/Boosters/ShieldBlessing': 'Shield Blessing',
                   'JupiterToEuropaJunction': 'Europa Junction',
                   'EarthToMarsJunction': 'Mars Junction',
                   'EarthToVenusJunction': 'Venus Junction',
                   'SaturnToUranusJunction': 'Uranus Junction',
                   'CetusHub4': 'Cetus',
                   'SolNode801': 'Sanctuary Onslaught',
                   'MarsToCeresJunction': 'Ceres Junction',
                   'CeresToJupiterJunction': 'Jupiter Junction',
                   'JupiterToSaturnJunction': 'Saturn Junction',
                   'UranusToNeptuneJunction': 'Neptune Junction',
                   'NeptuneToPlutoJunction': 'Pluto Junction',
                   'SolNode761': 'The Index (Low Risk)',
                   'SolNode762': 'The Index (Medium Risk)',
                   'PlutoToSednaJunction': 'Sedna Junction',
                   'SolarisUnitedHub1': 'Fortuna',
                   'PlutoToErisJunction': 'Eris Junction',
                   'SolNode802': 'Elite Sanctuary Onslaught',
                   'SolNode763': 'The Index (High Risk)',
                   'MarsToPhobosJunction': 'Phobos Junction',
                   'VenusToMercuryJunction': 'Mercury Junction',
                   'SolNode764': 'The Index (Quest)',
                   'CrewBattleNode506': 'Unknown Railjack - CrewBattleNode506',
                   'CrewBattleNode520': 'Unknown Railjack - CrewBattleNode520',
                   'CrewBattleNode508': 'Unknown Railjack - CrewBattleNode508',
                   'CrewBattleNode507': 'Unknown Railjack - CrewBattleNode507',
                   'CrewBattleNode517': 'Unknown Railjack - CrewBattleNode517',
                   'CrewBattleNode532': 'Unknown Railjack - CrewBattleNode532',
                   'DeimosHub': 'Necralisk',
                   'CrewBattleNode557': 'Unknown Railjack - CrewBattleNode557',
                   'CrewBattleNode558': 'Unknown Railjack - CrewBattleNode558',
                   'SolNode705': 'Unknown - SolNode705',
                   'NightwaveDerelictDefensePluto': 'Emissary Derelict Defense - Pluto',
                   'NightwaveDerelictSabotageNeptune': 'Emissary Derelict Sabotage - Neptune',
                   'NightwaveDerelictAssassinateEuropa': 'Emissary Derelict Sabotage - Neptune',
                   'NightwaveDerelictAssassinatePhobos': 'Emissary Derelict Assassinate - Phobos',
                   'NightwaveDerelictAssassinatePluto': 'Emissary Derelict Assassinate - Pluto',
                   'NightwaveDerelictAssassinateVenus': 'Emissary Derelict Assassinate - Venus',
                   'NightwaveDerelictAssassinateCeres': 'Emissary Derelict Assassinate - Ceres',
                   'NightwaveDerelictAssassinateSaturn': 'Emissary Derelict Assassinate - Saturn',
                   'SolNode701': 'Unknown - SolNode701',
                   '/Lotus/Types/Keys/WeeklyMissions/BaroWeeklyMission': 'Void Raider',
                   'NightwaveDerelictAssassinateNeptune': 'Emissary Derelict Assassinate - Neptune',
                   'CrewShipGenericTunnel': 'Railjack Tunnel',
                   '/Lotus/Types/Keys/RaidKeys/Raid01Stage01NightmareKeyItem': 'Law of Retribution: Nightmare - Stage 1',
                   '/Lotus/Types/Keys/RaidKeys/Raid01Stage01KeyItem': 'Law of Retribution - Stage 1',
                   '/Lotus/Types/Keys/RaidKeys/Raid01Stage02KeyItem': 'Law of Retribution - Stage 2',
                   '/Lotus/Types/Keys/RaidKeys/Raid01Stage03KeyItem': 'Law of Retribution - Stage 3',
                   '/Lotus/Types/Keys/RaidKeys/RaidGolemStage01KeyItem': 'Jordas Verdict - Stage 1',
                   '/Lotus/Types/Keys/RaidKeys/RaidGolemStage02KeyItem': 'Jordas Verdict - Stage 2',
                   '/Lotus/Types/Keys/RaidKeys/Raid01Stage02NightmareKeyItem': 'Law of Retribution: Nightmare - Stage 2',
                   '/Lotus/Types/Keys/RaidKeys/Raid01Stage03NightmareKeyItem': 'Law of Retribution: Nightmare - Stage 3',
                   '/Lotus/Types/Keys/RaidKeys/RaidGolemStage03KeyItem': 'Jordas Verdict - Stage 3',
                   'NightwaveDerelictSurvivalCeres': 'Emissary Derelict Survival - Ceres',
                   'NightwaveDerelictSabotageJupiter': 'Emissary Derelict Sabotage - Jupiter',
                   'NightwaveDerelictMobDefMars': 'Emissary Derelict Mobile Defense - Mars',
                   'NightwaveDerelictMobDefEuropa': 'Emissary Derelict Mobile Defense - Europa',
                   'NightwaveDerelictExterminatePhobos': 'Emissary Derelict Exterminate - Phobos',
                   'NightwaveDerelictDefenseSaturn': 'Emissary Derelict Defense - Saturn',
                   'NightwaveDerelictExterminateVenus': 'Emissary Derelict Exterminate - Venus',
                   'NightwaveDerelictSurvivalSedna': 'Emissary Derelict Survival - Sedna',
                   'EventNode37': 'Gifts of the Lotus - Stolen!',
                   '/Lotus/Language/Game/Jupiter': 'Jupiter',
                   'NightwaveDerelictAssassinateMars': 'Emissary Derelict Assasinate - Mars',
                   '/Lotus/Types/Keys/TestKeyRailjackSentientExterminatePoi1SuperWep': 'Railjack Sentient Exterminate Super Weapon',
                   '/Lotus/Types/Keys/TestKeyRailjackSentientExterminatePoi1Radar': 'Railjack Sentient Exterminate Poi1 Radar',
                   '/Lotus/Types/Keys/OrokinKeyB': 'Tower I Survival',
                   '/Lotus/Types/Keys/SpyQuestKeyChain/SpyQuestIntroKey': 'Capture Maroo',
                   '/Lotus/Types/Keys/SpyQuestKeyChain/SpyQuestKeyA': 'Take An Arcane Codex',
                   '/Lotus/Types/Keys/SpyQuestKeyChain/SpyQuestKeyB': 'Take The Grineer Arcane Codices',
                   '/Lotus/Types/Keys/SpyQuestKeyChain/SpyQuestKeyC': 'Take The Corpus Arcane Codices',
                   '/Lotus/Types/Keys/SpyQuestKeyChain/SpyQuestFinalKey': 'Find The Arcane Machine',
                   '/Lotus/Types/Keys/DragonQuest/DragonQuestMissionOne': 'Find Cephalon Simaris\' Missing Sentinels',
                   'EventNode21': 'Unknown - EventNode21',
                   'EventNode23': 'Unknown - EventNode23',
                   'NightwaveDerelictAssassinateSedna': 'Emissary Derelict Assassinate - Sedna',
                   '/Lotus/Language/Locations/Jupiter': 'Jupiter',
                   '/Lotus/Types/StoreItems/AvatarImages/FanChannel/AvatarImageKiwa': 'Kiwa Glyph',
                   '/Lotus/Types/Restoratives/Consumable/PrismaArrowBundle': 'Prisma Arrows',
                   '/Lotus/Types/Recipes/Kubrow/Collars/PrimeKubrowCollarABlueprint': 'Kavasa Prime Kubrow Collar Blueprint',
                   }
    parser = {}

    for manifest_file in manifest_list.values():
        for key in manifest_file:
            ingredient_list = []
            for data in manifest_file[key]:
                if 'uniqueName' in data:
                    if 'name' in data and data['name']:
                        if '/Lotus/Types/Game/Projections/' in data['uniqueName']:
                            parser[data['uniqueName']] = f"{data['name']} {parse_rarity(data['uniqueName'])}"
                        else:
                            name = data['name']
                            if '<ARCHWING>' in name:
                                name = name.split(maxsplit=1)[1]

                            if data['uniqueName'] not in parser:
                                parser[data['uniqueName']] = name
                    else:
                        if 'ingredients' in data:
                            for ingredient in data['ingredients']:
                                ingredient_list.append(ingredient['ItemType'])

                        if 'resultType' in data:
                            parser[data['uniqueName']] = data['resultType']

                    if 'abilities' in data:
                        for ability in data['abilities']:
                            parser[ability['abilityUniqueName']] = ability['abilityName']

                elif 'abilityUniqueName' in data:
                    parser[data['abilityUniqueName']] = data['abilityName']

    nodes = get_node_list()

    missions = []
    planets = []
    enemies = []
    node_list = []
    for node in nodes:
        parser[node] = nodes[node]
        mission_node = parser[node]['node']
        if 'planet' in parser[node]:
            mission_node += f" - {parser[node]['planet']}"

        parser[mission_node] = node

        if 'node' in nodes[node] and nodes[node]['node'] not in node_list:
            node_list.append(nodes[node]['node'])

        if 'planet' in nodes[node] and nodes[node]['planet'] not in planets:
            planets.append(nodes[node]['planet'])

        if 'enemy' in nodes[node] and nodes[node]['enemy'] not in enemies:
            enemies.append(nodes[node]['enemy'])

        if 'type' in nodes[node] and nodes[node]['type'] not in missions:
            missions.append(nodes[node]['type'])

    parser['/Lotus/Types/Items/Research/DojoColors/GenericDojoColorPigment'] = 'Dojo Color Pigment'

    parser.update(parser_base)

    return parser


def get_manifest():
    wf_manifest = fix().split('\r\n')
    manifest_list = {}
    for item in wf_manifest:
        try:
            json_file = requests.get(f"http://content.warframe.com/PublicExport/Manifest/{item}")
            json_file = json_file.text
            json_file = json.loads(json_file, strict=False)

            manifest_list[item.split("_en")[0]] = json_file
        except JSONDecodeError:
            pass

    return manifest_list


def gen_type_dict(manifest_list: dict, parser: dict):
    type_dict = {'Relics': set(),
                 'Arcanes': set(),
                 'Mods': set(),
                 'Avionics': set(),
                 'Warframes': set(),
                 'PrimeWarframes': set(),
                 'WarframeParts': set(),
                 'PrimeWarframeParts': set(),
                 'Weapons': set(),
                 'PrimeWeapons': set(),
                 'WeaponParts': set(),
                 'PrimeWeaponParts': set(),
                 'Sentinels': set(),
                 'PrimeSentinels': set(),
                 'SentinelParts': set(),
                 'PrimeSentinelParts': set(),
                 "Decorations": set(),
                 'CapturaScenes': set(),
                 'Fish': set(),
                 'SyndicateMedallions': set(),
                 'AyatanSculptures': set(),
                 'AyatanStars': set(),
                 'FocusLens': set(),
                 'Plants': set(),
                 'Gems': set(),
                 'ShipComponents': set(),
                 'Misc': set(),
                 }

    for data in manifest_list.values():
        for key in data:
            for item in data[key]:
                if "name" in item and "<ARCHWING> " in item['name']:
                    item['name'] = item['name'].replace("<ARCHWING> ", "")
                if key == "ExportRelicArcane":
                    if 'Relic' in item['name']:
                        type_dict['Relics'].add(item['name'])
                    else:
                        type_dict['Arcanes'].add(item['name'])
                elif key == "ExportUpgrades":
                    type_dict['Mods'].add(item['name'])
                elif key == "ExportAvionics":
                    type_dict['Avionics'].add(item['name'])
                elif key == "ExportWarframes":
                    if 'Prime' in item['name']:
                        type_dict['PrimeWarframes'].add(item['name'])
                    else:
                        type_dict['Warframes'].add(item['name'])
                elif key == "ExportWeapons":
                    if 'Prime' in item['name']:
                        type_dict['PrimeWeapons'].add(item['name'])
                    else:
                        type_dict['Weapons'].add(item['name'])
                elif key == "ExportSentinels":
                    if 'Prime' in item['name']:
                        type_dict['PrimeSentinels'].add(item['name'])
                    else:
                        type_dict['Sentinels'].add(item['name'])

    data = manifest_list['ExportResources']

    item_type = {'/Lotus/Types/Items/MiscItems/ResourceItem': 'Misc',
                 '/Lotus/Types/Gameplay/Zariman/Resources/ZarimanResourceItem': 'Misc',
                 '/Lotus/Types/Gameplay/Zariman/Resources/ZarimanDogTag': 'SyndicateMedallions',
                 '/Lotus/Types/Items/SyndicateDogTags/DogTag': 'SyndicateMedallions',
                 '/Lotus/Types/Items/SyndicateDogTags/SteelMeridianDogTag': 'SyndicateMedallions',
                 '/Lotus/Types/Items/SyndicateDogTags/SteelMeridianUncommonDogTag': 'SyndicateMedallions',
                 '/Lotus/Types/Items/SyndicateDogTags/RedVeilDogTag': 'SyndicateMedallions',
                 '/Lotus/Types/Items/SyndicateDogTags/RedVeilUncommonDogTag': 'SyndicateMedallions',
                 '/Lotus/Types/Items/SyndicateDogTags/PerrinDogTag': 'SyndicateMedallions',
                 '/Lotus/Types/Items/SyndicateDogTags/PerrinUncommonDogTag': 'SyndicateMedallions',
                 '/Lotus/Types/Items/SyndicateDogTags/NoraDogTag': 'SyndicateMedallions',
                 '/Lotus/Types/Items/SyndicateDogTags/NewLokaDogTag': 'SyndicateMedallions',
                 '/Lotus/Types/Items/SyndicateDogTags/NewLokaUncommonDogTag': 'SyndicateMedallions',
                 '/Lotus/Types/Items/SyndicateDogTags/CephalonDogTag': 'SyndicateMedallions',
                 '/Lotus/Types/Items/SyndicateDogTags/CephalonUncommonDogTag': 'SyndicateMedallions',
                 '/Lotus/Types/Items/SyndicateDogTags/ArbitersDogTag': 'SyndicateMedallions',
                 '/Lotus/Types/Items/SyndicateDogTags/ArbitersUncommonDogTag': 'SyndicateMedallions',
                 '/Lotus/Types/Items/Fish/Solaris/CommonFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Deimos/RareFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Deimos/HybridRareAFishItem': 'Fish',
                 '/Lotus/Types/Items/MiscItems/FocusLensOstron': 'FocusLens',
                 '/Lotus/Types/Items/MiscItems/FocusLensLua': 'FocusLens',
                 '/Lotus/Types/Items/MiscItems/FocusLensGreater': 'FocusLens',
                 '/Lotus/Types/Items/MiscItems/FocusLens': 'FocusLens',
                 '/Lotus/Types/Items/MiscItems/ShipComponentItem': 'ShipComponents',
                 '/Lotus/Types/Items/ShipDecos/ShipDecoItem': 'Decorations',
                 '/Lotus/Types/Items/ShipDecos/ChildDrawingBase': 'Decorations',
                 '/Lotus/Types/Items/ShipDecos/InstrumentDecoItem': 'Decorations',
                 '/Lotus/Types/Items/ShipDecos/BaseFishTrophy': 'Decorations',
                 '/Lotus/Types/Items/ShipDecos/LotusShawzinPlayableBase': 'Decorations',
                 '/Lotus/Types/Items/ShipDecos/BaseFishTrophy': 'Decorations',
                 '/Lotus/Types/Items/ShipDecos/Vignettes/Enemies/ShipDecoItem': 'Decorations',
                 '/Lotus/Types/Items/ShipDecos/Plushies/PlushyThumper': 'Decorations',
                 '/Lotus/Types/Items/Research/SampleBase': 'Misc',
                 '/Lotus/Types/Items/RailjackMiscItems/BaseRailjackItem': 'Misc',
                 '/Lotus/Types/Items/Plants/MiscItems/PlantItem': 'Plants',
                 '/Lotus/Types/Items/Gems/GemItem': 'Gems',
                 '/Lotus/Types/Items/FusionTreasures/FusionOrnament': 'AyatanStars',
                 '/Lotus/Types/Items/FusionTreasure': 'AyatanSculptures',
                 '/Lotus/Types/Items/Fish/Solaris/SolarisWarmRareFishAItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Solaris/RareFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Solaris/SolarisCoolUncommonFishAItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Solaris/UncommonFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Solaris/SolarisCoolCommonFishAItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Solaris/OrokinCoolRareFishAItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Solaris/RareFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Solaris/OrokinBothRareFishAItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Solaris/SolarisBothCommonFishAItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Solaris/OrokinBothLegendaryFishAItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Solaris/LegendaryFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Solaris/CorpusWarmUncommonFishAItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Solaris/CorpusWarmCommonFishBItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Solaris/CorpusWarmCommonFishAItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Solaris/CorpusCoolUncommonFishAItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Solaris/CorpusCoolCommonFishAItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Solaris/CorpusBothUncommonFishAItem': 'Fish',
                 '/Lotus/Types/Items/Fish/FishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/FishPartItem': 'Misc',
                 '/Lotus/Types/Items/Fish/Eidolon/NightRareFishBItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Eidolon/RareFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Eidolon/NightRareFishAItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Eidolon/RareFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Eidolon/NightLegendaryFishAItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Eidolon/LegendaryFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Eidolon/DayUncommonFishBItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Eidolon/UncommonFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Eidolon/DayUncommonFishAItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Eidolon/UncommonFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Eidolon/DayCommonFishCItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Eidolon/CommonFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Eidolon/DayCommonFishBItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Eidolon/CommonFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Eidolon/DayCommonFishAItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Eidolon/BothUncommonFishBItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Eidolon/BothUncommonFishAItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Eidolon/BothRareFishAItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Eidolon/BothCommonFishBItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Eidolon/BothCommonFishAItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Deimos/OrokinUncommonAFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Deimos/UncommonFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Deimos/OrokinRareAFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Deimos/OrokinLegendaryAFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Deimos/LegendaryFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Deimos/InfestedUncommonAFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Deimos/UncommonFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Deimos/InfestedRareAFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Deimos/CommonFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Deimos/InfestedCommonEFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Deimos/InfestedCommonDFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Deimos/InfestedCommonCFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Deimos/InfestedCommonBFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Deimos/InfestedCommonAFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Deimos/HybridUncommonBFishItem': 'Fish',
                 '/Lotus/Types/Items/Fish/Deimos/HybridUncommonAFishItem': 'Fish',
                 '/Lotus/Types/Items/MiscItems/PhotoboothTile': 'CapturaScenes',
                 '/Lotus/Types/Items/MiscItems/PhotoboothTileRequiresOwnership': 'CapturaScenes',
                 '/Lotus/Types/Items/MiscItems/PhotoboothTileRequiresQuestComplete': 'CapturaScenes'}

    override_dict = {'Decurion Barrel': 'WeaponParts',
                     'Decurion Receiver': 'WeaponParts',
                     'Morgha Stock': 'WeaponParts',
                     'Morgha Barrel': 'WeaponParts',
                     'Morgha Receiver': 'WeaponParts',
                     'Cortege Barrel': 'WeaponParts',
                     'Cortege Receiver': 'WeaponParts',
                     'Cortege Stock': 'WeaponParts', }

    for item in data['ExportResources']:
        if item['name'] in override_dict:
            type_dict[override_dict[item['name']]].add(item['name'])
        elif item['parentName'] in item_type:
            type_dict[item_type[item['parentName']]].add(item['name'])
        elif any(x in item['name'] for x in type_dict['PrimeWeapons']):
            type_dict['PrimeWeaponParts'].add(item['name'])
        elif any(x in item['name'] for x in type_dict['Weapons']):
            type_dict['WeaponParts'].add(item['name'])
        elif any(x in item['name'] for x in type_dict['PrimeWarframes']):
            type_dict['PrimeWarframeParts'].add(item['name'])
        elif any(x in item['name'] for x in type_dict['Warframes']):
            type_dict['WarframeParts'].add(item['name'])
        elif any(x in item['name'] for x in type_dict['PrimeSentinels']):
            type_dict['PrimeSentinelParts'].add(item['name'])
        elif any(x in item['name'] for x in type_dict['Sentinels']):
            type_dict['SentinelParts'].add(item['name'])
        else:
            type_dict['Misc'].add(item['name'])

    data = manifest_list['ExportRecipes']

    for item in data['ExportRecipes']:
        dict_keys = ['Warframes', 'PrimeWarframes', 'Weapons', 'PrimeWeapons', 'WeaponParts', 'WarframeParts',
                     'PrimeWeaponParts', 'PrimeWarframeParts', 'Sentinels', 'PrimeSentinels', 'SentinelParts',
                     'PrimeSentinelParts', 'ShipComponents', 'FocusLens']
        trans_dict = {'Warframes': 'WarframeParts', 'PrimeWarframes': 'PrimeWarframeParts',
                      'Weapons': 'WeaponParts', 'PrimeWeapons': 'PrimeWeaponParts',
                      'WeaponParts': 'WeaponParts', 'WarframeParts': 'WarframeParts',
                      'PrimeWeaponParts': 'PrimeWeaponParts', 'PrimeWarframeParts': 'PrimeWarframeParts',
                      'Sentinels': 'SentinelParts', 'PrimeSentinels': 'PrimeSentinelParts',
                      'SentinelParts': 'SentinelParts', 'PrimeSentinelParts': 'PrimeSentinelParts',
                      'ShipComponents': 'ShipComponents', 'FocusLens': 'FocusLens'}
        for key in dict_keys:
            parsed = parse_name(item['resultType'], parser)
            if parsed in type_dict[key]:
                type_dict[trans_dict[key]].add(f"{parsed} Blueprint")

    for key in type_dict:
        if isinstance(type_dict[key], set):
            type_dict[key] = list(type_dict[key])

    return type_dict

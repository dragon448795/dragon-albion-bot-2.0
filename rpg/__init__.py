"""
RPG 系統核心模組
提供 RPG 遊戲系統的核心功能
"""

from .player import RPGPlayer
from .commands import RPGCommands

__all__ = ['RPGPlayer', 'RPGCommands']
__version__ = '1.0.0'
__author__ = '小雲機械人團隊'

class RPGConfig:
    """RPG 系統配置"""
    
    # 屬性點系統
    STAT_POINTS_PER_LEVEL = 3
    
    # 基礎屬性
    BASE_STATS = {
        "vitality": 10,      # 體力
        "speed": 10,         # 速度
        "strength": 10,      # 力量
        "intelligence": 10,  # 智慧
        "carrying_capacity": 10  # 負重
    }
    
    # 經驗值系統
    EXP_CURVE = {
        "base_exp": 100,
        "growth_rate": 1.5,
        "max_level": 300
    }
    
    # 地圖設定
    MAPS = {
        "新手森林": {
            "layers": 10,
            "min_level": 1,
            "max_level": 30,
            "boss": "森林巨熊",
            "biome": "forest",
            "monster_count": 50
        },
        "沙漠遺跡": {
            "layers": 10,
            "min_level": 30,
            "max_level": 80,
            "boss": "沙暴領主",
            "biome": "desert",
            "monster_count": 50
        },
        "冰封山脈": {
            "layers": 10,
            "min_level": 80,
            "max_level": 150,
            "boss": "冰霜巨龍",
            "biome": "mountain",
            "monster_count": 50
        },
        "深淵地獄": {
            "layers": 10,
            "min_level": 150,
            "max_level": 300,
            "boss": "深淵魔王",
            "biome": "hell",
            "monster_count": 50
        }
    }
    
    # 裝備稀有度顏色
    RARITY_COLORS = {
        "green": 0x00FF00,      # 綠色
        "blue": 0x0000FF,       # 藍色
        "purple": 0x800080,     # 紫色
        "gold": 0xFFD700        # 金色
    }
    
    # 詞條庫
    SPECIAL_EFFECTS = {
        "明境止水": {"crit_rate": 0.15, "description": "爆擊率大增加"},
        "一心二用": {"crit_rate": 0.10, "description": "爆擊率中增加"},
        "靈光一觸": {"crit_rate": 0.05, "description": "爆擊率小增加"},
        "會心滅世": {"crit_damage": 0.50, "description": "爆擊傷害大增加"},
        "會心之魂": {"crit_damage": 0.30, "description": "爆擊傷害中增加"},
        "會心一擊": {"crit_damage": 0.15, "description": "爆擊傷害小增加"},
        "絕對領域": {"defense": 0.30, "description": "大幅增加防禦力"},
        "絕對鐵壁": {"defense": 0.20, "description": "中幅增加防禦力"},
        "絕對防禦": {"defense": 0.10, "description": "小幅增加防禦力"},
        "超頻之力三": {"speed": 0.30, "description": "大幅增加速度"},
        "超頻之力二": {"speed": 0.20, "description": "中幅增加速度"},
        "超頻之力一": {"speed": 0.10, "description": "小幅增加速度"},
        "賢者傳承三": {"intelligence": 0.30, "description": "大幅增加智慧"},
        "賢者傳承二": {"intelligence": 0.20, "description": "中幅增加智慧"},
        "賢者傳承一": {"intelligence": 0.10, "description": "小幅增加智慧"}
    }
    
    # 武器技能
    WEAPON_SKILLS = {
        "大劍": {
            "普攻倍率": 1.2,
            "技能": {
                "奮發一擊": {
                    "mp_cost": 30,
                    "damage_multiplier": 2.5,
                    "crit_rate_bonus": 0.3,
                    "crit_damage_bonus": 0.5,
                    "description": "消耗高MP發動強力一擊，爆擊率與爆擊傷害大幅提升"
                },
                "嘲諷": {
                    "mp_cost": 15,
                    "taunt_chance": 0.8,
                    "duration": 3,
                    "description": "高機率使敵人向自己攻擊"
                }
            }
        },
        "魔杖": {
            "普攻倍率": 0.8,
            "技能": {
                "烈焰地獄": {
                    "mp_cost": 20,
                    "damage_multiplier": 2.0,
                    "crit_rate_bonus": 0.1,
                    "crit_damage_bonus": 0.3,
                    "description": "消耗低MP發動範圍魔法攻擊"
                },
                "混元一火": {
                    "mp_cost": 40,
                    "damage_multiplier": 3.0,
                    "crit_rate_bonus": 0.4,
                    "crit_damage_bonus": 0.6,
                    "description": "消耗高MP發動超強魔法攻擊，爆擊率極高"
                }
            }
        },
        "神聖杖": {
            "普攻倍率": 0.5,
            "技能": {
                "神聖治療": {
                    "mp_cost": 15,
                    "heal_multiplier": 2.0,
                    "target": "ally",
                    "description": "消耗低MP治療隊友或自己"
                },
                "復活術": {
                    "mp_cost": 50,
                    "revive_hp_percent": 0.3,
                    "target": "dead_ally",
                    "description": "消耗高MP復活已死亡隊友"
                }
            }
        }
    }
    
    # 房屋系統
    HOUSES = {
        "orphanage": {
            "name": "小雲孤兒院",
            "storage_capacity": 20,
            "cost": 0,
            "unlocked": True
        },
        "small_house": {
            "name": "小房屋",
            "storage_capacity": 50,
            "cost": 10000,
            "unlocks": ["herb_room_lv1"]
        },
        "medium_house": {
            "name": "中房屋",
            "storage_capacity": 75,
            "cost": 50000,
            "unlocks": ["herb_room_lv2"]
        },
        "large_house": {
            "name": "大房屋",
            "storage_capacity": 100,
            "cost": 200000,
            "unlocks": ["herb_room_lv2"]
        },
        "territory": {
            "name": "領地",
            "storage_capacity": 400,
            "cost": 1000000,
            "unlocks": ["herb_room_lv3", "workshop_lv1"]
        },
        "castle": {
            "name": "城堡",
            "storage_capacity": 1000,
            "cost": 5000000,
            "unlocks": ["herb_room_lv3", "workshop_lv2"]
        }
    }
    
    # 藥水製作
    POTION_CRAFTING = {
        "herb_room_lv1": ["hp_potion_small", "mp_potion_small"],
        "herb_room_lv2": ["hp_potion_medium", "mp_potion_medium", "hp_potion_small", "mp_potion_small"],
        "herb_room_lv3": ["hp_potion_large", "mp_potion_large", "hp_potion_medium", "mp_potion_medium", "teleport_scroll"]
    }
    
    # 鍛造系統
    FORGING = {
        "workshop_lv1": {
            "weapons": ["green", "blue"],
            "armors": ["green", "blue"],
            "accessories": ["green", "blue"]
        },
        "workshop_lv2": {
            "weapons": ["green", "blue", "purple", "gold"],
            "armors": ["green", "blue", "purple", "gold"],
            "accessories": ["green", "blue", "purple", "gold"]
        }
    }
    
    # 怪物掉落設定
    DROP_RATES = {
        "normal": {
            "green": 0.70,
            "blue": 0.25,
            "purple": 0.045,
            "gold": 0.005
        },
        "elite": {
            "green": 0.50,
            "blue": 0.35,
            "purple": 0.13,
            "gold": 0.02
        },
        "boss": {
            "green": 0.20,
            "blue": 0.40,
            "purple": 0.30,
            "gold": 0.10
        }
    }


# EMOJI 對應
RPG_EMOJIS = {
    # 屬性
    "❤️": "體力",
    "⚡": "速度",
    "💪": "力量",
    "🧠": "智慧",
    "🎒": "負重",
    
    # 狀態
    "❤️‍🩹": "HP",
    "🔵": "MP",
    "⭐": "經驗值",
    "📊": "等級",
    
    # 裝備部位
    "⚔️": "武器",
    "👑": "頭部",
    "🛡️": "身體",
    "👟": "鞋子",
    "📿": "項鍊",
    "💍": "戒指",
    "🎒": "背包",
    
    # 稀有度
    "🟢": "綠色",
    "🔵": "藍色",
    "🟣": "紫色",
    "🟡": "金色",
    
    # 行動
    "🏃‍♂️": "前進",
    "🔙": "後退",
    "⚔️": "攻擊",
    "🛡️": "防禦",
    "💊": "使用藥水",
    "🏃": "逃跑",
    "🏠": "回城",
    
    # 城鎮
    "🛒": "商店",
    "⚒️": "鍛造",
    "🏘️": "房屋",
    "💼": "背包",
    "👥": "組隊",
    "📊": "狀態",
    
    # 隊伍
    "👑": "創建隊伍",
    "🔍": "搜尋隊伍",
    "🤝": "邀請",
    "✅": "準備",
    "❌": "離開",
    
    # 戰鬥行動
    "🔥": "技能1",
    "❄️": "技能2",
    "💫": "技能3",
    "🌀": "技能4"
}


def create_progress_bar(percentage, length=20):
    """創建進度條"""
    filled = int((percentage / 100) * length)
    empty = length - filled
    
    if filled == length:
        bar = "█" * filled
    else:
        bar = "█" * filled + "░" * empty
    
    return f"[{bar}]"


def calculate_exp_required(level):
    """計算升級所需經驗值"""
    base_exp = RPGConfig.EXP_CURVE["base_exp"]
    growth_rate = RPGConfig.EXP_CURVE["growth_rate"]
    return int(base_exp * (growth_rate ** (level - 1)))


def calculate_combat_power(stats):
    """計算戰鬥力"""
    combat_power = (
        stats.get('vitality', 0) * 10 +
        stats.get('speed', 0) * 5 +
        stats.get('strength', 0) * 15 +
        stats.get('intelligence', 0) * 12
    )
    return combat_power


# 導出常用函數
__all__ += ['RPGConfig', 'RPG_EMOJIS', 'create_progress_bar', 
            'calculate_exp_required', 'calculate_combat_power']

# 模組初始化信息
print(f"✅ RPG 系統已載入 (版本: {__version__})")

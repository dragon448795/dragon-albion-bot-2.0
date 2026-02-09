#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
小雲ALBION機械人 - PostgreSQL完整版本 + RPG系統
包含所有13個原指令 + 新增/blessing指令 + 聊天積分系統 + RPG系統
修復獎品增減功能，使用Railway PostgreSQL
完整功能版本（不刪減）
"""

import os
import discord
from discord.ext import commands
from discord import app_commands
import sys
import asyncio
import json
import random
from datetime import datetime, timedelta
from typing import Optional, List, Literal
import time
import asyncpg  # PostgreSQL
from dotenv import load_dotenv
import traceback
import aiosqlite  # 添加 SQLite 支持作為備份

# 載入環境變數
load_dotenv()

# ========== 設定 ==========
BOT_NAME = "小雲機械人"
OWNER_IDS = [337237662157242368]  # 你的 Discord ID

# 職業對應的EMOJI
PROFESSION_EMOJIS = {
    "🛡️": "坦克",
    "⚔️": "输出", 
    "💚": "治疗",
    "💛": "辅助"
}

# 評核評分選項
RATING_EMOJIS = {
    "⭐": "優秀",
    "👍": "良好", 
    "👌": "普通",  # 預設評級
    "❌": "不合格"
}

# 評核結束EMOJI
RATING_END_EMOJI = "🏁"

# 積分抽獎EMOJI
SCORE_DRAW_EMOJIS = ["🟢", "🔵", "🟣"]

# ========== 積分設定 ==========
SIGNUP_SCORE = 40  # 簽到積分
CHAT_SCORE = 1     # 每句話積分
DAILY_CHAT_LIMIT = 20  # 每日聊天積分上限
PROFESSION_BONUS = {
    "坦克": 0,
    "输出": 0,
    "治疗": 20,  # 補師+20積分
    "辅助": 0
}
RATING_SCORES = {
    "優秀": 40,    # 優秀+40積分
    "良好": 10,    # 良好+10積分
    "普通": 0,     # 普通+0積分
    "不合格": -5   # 不合格-5積分
}

# ========== RPG 設定 ==========
RPG_CONFIG = {
    # 屬性點系統
    "STAT_POINTS_PER_LEVEL": 3,
    "BASE_STATS": {
        "vitality": 10,      # 體力
        "speed": 10,         # 速度
        "strength": 10,      # 力量
        "intelligence": 10,  # 智慧
        "carrying_capacity": 10  # 負重
    },
    
    # 經驗值系統
    "EXP_CURVE": {
        "base_exp": 100,
        "growth_rate": 1.5,
        "max_level": 300
    },
    
    # 地圖設定
    "MAPS": {
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
    },
    
    # 裝備稀有度顏色
    "RARITY_COLORS": {
        "green": 0x00FF00,      # 綠色
        "blue": 0x0000FF,       # 藍色
        "purple": 0x800080,     # 紫色
        "gold": 0xFFD700        # 金色
    },
    
    # 詞條庫
    "SPECIAL_EFFECTS": {
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
    },
    
    # 武器技能
    "WEAPON_SKILLS": {
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
    },
    
    # 房屋系統
    "HOUSES": {
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
    },
    
    # 藥水製作
    "POTION_CRAFTING": {
        "herb_room_lv1": ["hp_potion_small", "mp_potion_small"],
        "herb_room_lv2": ["hp_potion_medium", "mp_potion_medium", "hp_potion_small", "mp_potion_small"],
        "herb_room_lv3": ["hp_potion_large", "mp_potion_large", "hp_potion_medium", "mp_potion_medium", "teleport_scroll"]
    },
    
    # 鍛造系統
    "FORGING": {
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
    },
    
    # 怪物掉落設定
    "DROP_RATES": {
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

# Intents
intents = discord.Intents.default()
intents.message_content = True
intents.reactions = True
intents.members = True
intents.presences = True

bot = commands.Bot(
    command_prefix='!',
    intents=intents,
    help_command=None,
    case_insensitive=True
)

tree = bot.tree

# ========== PostgreSQL 連接池 ==========
class Database:
    def __init__(self):
        self.pool = None
        self.is_connected = False
        self.connection_attempts = 0
    
    async def connect(self):
        """連接 PostgreSQL 資料庫"""
        self.connection_attempts += 1
        print(f"\n🔄 連接 PostgreSQL (嘗試 {self.connection_attempts})...")
        
        try:
            # Railway 會自動提供 DATABASE_URL 環境變數
            database_url = os.getenv('DATABASE_URL')
            
            print(f"🔍 DATABASE_URL 是否存在: {'✅' if database_url else '❌'}")
            
            if not database_url:
                print("⚠️ 找不到 DATABASE_URL，將無法保存數據！")
                print("💡 請在 Railway 設定環境變數：")
                print("   1. 進入 Railway 專案")
                print("   2. 點擊 Settings")
                print("   3. 點擊 Variables")
                print("   4. 新增 DATABASE_URL = 你的_postgresql_url")
                return False
            
            # 顯示 DATABASE_URL（隱藏密碼）
            if '@' in database_url:
                safe_url = database_url.split('@')[0] + '@' + database_url.split('@')[1].split('/')[0] + '/***'
                print(f"🔍 DATABASE_URL: {safe_url}")
            else:
                print(f"🔍 DATABASE_URL: {database_url[:50]}...")
            
            # 修正URL格式
            if database_url.startswith('postgres://'):
                database_url = database_url.replace('postgres://', 'postgresql://', 1)
                print(f"🔧 已修正 URL 格式: {database_url[:50]}...")
            
            # 設置連線超時和重試
            print("🔄 正在建立連線池...")
            self.pool = await asyncpg.create_pool(
                database_url, 
                min_size=1, 
                max_size=5,
                command_timeout=30,
                max_inactive_connection_lifetime=300,
                server_settings={
                    'client_encoding': 'utf8',
                    'application_name': 'discord_bot'
                }
            )
            
            # 測試連接
            print("🔧 測試連線...")
            async with self.pool.acquire() as conn:
                result = await conn.fetchval('SELECT 1')
                if result == 1:
                    print("✅ PostgreSQL 連接測試成功")
                else:
                    print("❌ PostgreSQL 連接測試失敗")
                    return False
            
            await self.init_db()
            self.is_connected = True
            print("✅ PostgreSQL 連接成功並初始化完成")
            return True
            
        except Exception as e:
            print(f"❌ PostgreSQL 連接失敗: {e}")
            print("💡 可能原因：")
            print("   1. DATABASE_URL 格式錯誤")
            print("   2. PostgreSQL 服務未啟動")
            print("   3. 網路連接問題")
            print("   4. 權限不足")
            traceback.print_exc()
            return False
    
    async def init_db(self):
        """初始化資料庫表格"""
        try:
            async with self.pool.acquire() as conn:
                # users 表格
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS users (
                        user_id BIGINT NOT NULL,
                        guild_id BIGINT NOT NULL,
                        username TEXT,
                        current_score INTEGER DEFAULT 0,
                        total_score INTEGER DEFAULT 0,
                        join_date TIMESTAMP DEFAULT NOW(),
                        last_active TIMESTAMP DEFAULT NOW(),
                        last_chat_date DATE,
                        daily_chat_score INTEGER DEFAULT 0,
                        profession_counts JSONB DEFAULT '{}',
                        activity_stats JSONB DEFAULT '{}',
                        rating_stats JSONB DEFAULT '{}',
                        PRIMARY KEY (user_id, guild_id)
                    )
                ''')
                
                # prize_pool 表格
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS prize_pool (
                        id SERIAL PRIMARY KEY,
                        prize_name TEXT NOT NULL,
                        box_level TEXT NOT NULL,
                        quantity INTEGER DEFAULT 1,
                        remaining INTEGER DEFAULT 1,
                        added_by BIGINT,
                        added_at TIMESTAMP DEFAULT NOW(),
                        guild_id BIGINT DEFAULT 0,
                        UNIQUE(prize_name, box_level, guild_id)
                    )
                ''')
                
                # giveaways 表格
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS giveaways (
                        id SERIAL PRIMARY KEY,
                        creator_id BIGINT,
                        prize TEXT,
                        winner_count INTEGER DEFAULT 1,
                        participants JSONB DEFAULT '[]',
                        winners JSONB DEFAULT '[]',
                        end_time TIMESTAMP,
                        message_id BIGINT,
                        channel_id BIGINT,
                        is_active BOOLEAN DEFAULT true,
                        created_at TIMESTAMP DEFAULT NOW(),
                        guild_id BIGINT DEFAULT 0
                    )
                ''')
                
                # score_draws 表格
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS score_draws (
                        id SERIAL PRIMARY KEY,
                        creator_id BIGINT,
                        score_cost INTEGER,
                        box_level TEXT,
                        participants JSONB DEFAULT '[]',
                        winner_prize TEXT,
                        winner_id BIGINT,
                        is_active BOOLEAN DEFAULT true,
                        created_at TIMESTAMP DEFAULT NOW(),
                        guild_id BIGINT DEFAULT 0
                    )
                ''')
                
                # score_transfers 表格
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS score_transfers (
                        id SERIAL PRIMARY KEY,
                        from_user_id BIGINT,
                        to_user_id BIGINT,
                        amount INTEGER,
                        reason TEXT,
                        timestamp TIMESTAMP DEFAULT NOW(),
                        guild_id BIGINT DEFAULT 0
                    )
                ''')
                
                # evaluation_events 表格
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS evaluation_events (
                        id SERIAL PRIMARY KEY,
                        event_name TEXT,
                        creator_id BIGINT,
                        signup_message_id BIGINT,
                        profession_message_id BIGINT,
                        rating_message_id BIGINT,
                        channel_id BIGINT,
                        participants JSONB DEFAULT '[]',
                        default_rated JSONB DEFAULT '[]',
                        professions JSONB DEFAULT '{}',
                        ratings JSONB DEFAULT '{}',
                        is_active BOOLEAN DEFAULT true,
                        start_time TIMESTAMP DEFAULT NOW(),
                        signup_end_time TIMESTAMP,
                        guild_id BIGINT DEFAULT 0
                    )
                ''')
                
                # query_logs 表格
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS query_logs (
                        id SERIAL PRIMARY KEY,
                        query_type TEXT,
                        user_id BIGINT,
                        parameters JSONB,
                        timestamp TIMESTAMP DEFAULT NOW(),
                        guild_id BIGINT DEFAULT 0
                    )
                ''')
                
                # ========== RPG 系統表格 ==========
                
                # RPG 玩家資料表
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS rpg_players (
                        user_id BIGINT PRIMARY KEY,
                        nickname VARCHAR(50),
                        level INTEGER DEFAULT 1,
                        exp INTEGER DEFAULT 0,
                        max_exp INTEGER DEFAULT 100,
                        
                        -- 基礎屬性
                        vitality INTEGER DEFAULT 10,      -- 體力
                        speed INTEGER DEFAULT 10,         -- 速度
                        strength INTEGER DEFAULT 10,      -- 力量
                        intelligence INTEGER DEFAULT 10,  -- 智慧
                        carrying_capacity INTEGER DEFAULT 10, -- 負重
                        
                        -- 狀態
                        current_hp INTEGER DEFAULT 100,
                        max_hp INTEGER DEFAULT 100,
                        current_mp INTEGER DEFAULT 50,
                        max_mp INTEGER DEFAULT 50,
                        
                        -- 裝備
                        weapon_id INTEGER,
                        head_id INTEGER,
                        body_id INTEGER,
                        shoes_id INTEGER,
                        necklace_id INTEGER,
                        ring_id INTEGER,
                        backpack_id INTEGER,
                        
                        -- 位置
                        current_map VARCHAR(50) DEFAULT '新手森林',
                        current_layer INTEGER DEFAULT 1,
                        is_in_town BOOLEAN DEFAULT true,
                        
                        -- 房屋
                        house_type VARCHAR(20) DEFAULT 'orphanage',
                        storage_capacity INTEGER DEFAULT 20,
                        
                        -- 統計
                        monsters_killed INTEGER DEFAULT 0,
                        deaths INTEGER DEFAULT 0,
                        total_damage BIGINT DEFAULT 0,
                        total_healing BIGINT DEFAULT 0,
                        
                        -- 祝福道具
                        alvis_blessings INTEGER DEFAULT 0,
                        cloudy_blessings INTEGER DEFAULT 0,
                        honest_brother_blessings INTEGER DEFAULT 0,
                        
                        -- 時間戳
                        created_at TIMESTAMP DEFAULT NOW(),
                        last_active TIMESTAMP DEFAULT NOW(),
                        last_heal_time TIMESTAMP DEFAULT NOW()
                    )
                ''')
                
                # 物品資料表
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS rpg_items (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(100),
                        item_type VARCHAR(20),  -- weapon/head/body/shoes/necklace/ring/backpack/material/potion
                        rarity VARCHAR(10) DEFAULT 'green', -- green/blue/purple/gold
                        level_requirement INTEGER DEFAULT 1,
                        
                        -- 屬性加成
                        vitality_bonus INTEGER DEFAULT 0,
                        speed_bonus INTEGER DEFAULT 0,
                        strength_bonus INTEGER DEFAULT 0,
                        intelligence_bonus INTEGER DEFAULT 0,
                        carrying_capacity_bonus INTEGER DEFAULT 0,
                        
                        -- 特殊詞條 (JSON格式)
                        special_effects JSONB DEFAULT '{}',
                        
                        -- 武器專用
                        weapon_type VARCHAR(20),  -- sword/staff/holy_staff
                        skill_name VARCHAR(50),
                        skill_mp_cost INTEGER DEFAULT 10,
                        skill_description TEXT,
                        
                        -- 藥水專用
                        potion_type VARCHAR(20), -- hp/mp/teleport/revive
                        potion_value INTEGER,
                        
                        -- 耐久度
                        max_durability INTEGER DEFAULT 100,
                        current_durability INTEGER DEFAULT 100,
                        
                        -- 擁有者
                        owner_id BIGINT,
                        is_equipped BOOLEAN DEFAULT false,
                        is_bound BOOLEAN DEFAULT false, -- 是否綁定
                        
                        -- 價格
                        base_price INTEGER DEFAULT 100,
                        
                        -- 合成材料
                        craftable BOOLEAN DEFAULT false,
                        recipe JSONB DEFAULT '{}', -- 合成配方
                        
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                ''')
                
                # 背包/倉庫
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS rpg_inventory (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT,
                        item_id INTEGER,
                        quantity INTEGER DEFAULT 1,
                        slot_type VARCHAR(20) DEFAULT 'inventory', -- inventory/storage/equipped
                        slot_index INTEGER, -- 背包格子索引
                        location VARCHAR(20) DEFAULT 'personal',  -- personal/guild_storage/market
                        created_at TIMESTAMP DEFAULT NOW(),
                        UNIQUE(user_id, item_id, slot_type, location)
                    )
                ''')
                
                # 怪物資料表
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS rpg_monsters (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(100),
                        level INTEGER,
                        rarity VARCHAR(10) DEFAULT 'normal',  -- normal/elite/boss
                        map_name VARCHAR(50),
                        layer_min INTEGER DEFAULT 1,
                        layer_max INTEGER DEFAULT 10,
                        
                        -- 屬性
                        hp_min INTEGER,
                        hp_max INTEGER,
                        attack_min INTEGER,
                        attack_max INTEGER,
                        defense_min INTEGER,
                        defense_max INTEGER,
                        speed_min INTEGER,
                        speed_max INTEGER,
                        
                        -- 掉落
                        drop_table JSONB DEFAULT '{}',  -- {material_id: {chance: float, min: int, max: int}}
                        exp_min INTEGER,
                        exp_max INTEGER,
                        
                        -- 特殊
                        is_boss BOOLEAN DEFAULT false,
                        boss_floor INTEGER DEFAULT 10,
                        
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                ''')
                
                # 隊伍系統
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS rpg_parties (
                        id SERIAL PRIMARY KEY,
                        leader_id BIGINT,
                        current_size INTEGER DEFAULT 1,
                        max_size INTEGER DEFAULT 5,
                        status VARCHAR(20) DEFAULT 'recruiting',  -- recruiting/exploring/battling/resting
                        current_map VARCHAR(50) DEFAULT '新手森林',
                        current_layer INTEGER DEFAULT 1,
                        created_at TIMESTAMP DEFAULT NOW(),
                        expires_at TIMESTAMP DEFAULT NOW() + INTERVAL '2 hours'
                    )
                ''')
                
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS rpg_party_members (
                        party_id INTEGER,
                        user_id BIGINT,
                        role VARCHAR(20),  -- tank/dps/healer/support
                        is_ready BOOLEAN DEFAULT false,
                        joined_at TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY(party_id, user_id)
                    )
                ''')
                
                # 商店系統
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS rpg_shops (
                        id SERIAL PRIMARY KEY,
                        shop_type VARCHAR(20) DEFAULT 'general', -- general/blacksmith/apothecary/auction
                        item_id INTEGER,
                        price INTEGER,
                        stock INTEGER DEFAULT -1, -- -1表示無限
                        restock_interval INTEGER DEFAULT 3600, -- 秒
                        last_restock TIMESTAMP DEFAULT NOW(),
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                ''')
                
                # 拍賣行
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS rpg_auctions (
                        id SERIAL PRIMARY KEY,
                        seller_id BIGINT,
                        item_id INTEGER,
                        quantity INTEGER DEFAULT 1,
                        price INTEGER,
                        bid_price INTEGER,
                        bidder_id BIGINT,
                        status VARCHAR(20) DEFAULT 'active', -- active/sold/expired/cancelled
                        created_at TIMESTAMP DEFAULT NOW(),
                        expires_at TIMESTAMP DEFAULT NOW() + INTERVAL '24 hours'
                    )
                ''')
                
                # 戰鬥記錄
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS rpg_battles (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT,
                        monster_id INTEGER,
                        battle_result VARCHAR(20), -- win/lose/flee
                        damage_dealt INTEGER,
                        damage_taken INTEGER,
                        exp_gained INTEGER,
                        items_dropped JSONB DEFAULT '[]',
                        battle_duration INTEGER, -- 秒
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                ''')
                
                # 成就系統
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS rpg_achievements (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT,
                        achievement_id VARCHAR(50),
                        achievement_name VARCHAR(100),
                        progress INTEGER DEFAULT 0,
                        target INTEGER,
                        completed BOOLEAN DEFAULT false,
                        completed_at TIMESTAMP,
                        reward JSONB DEFAULT '{}',
                        created_at TIMESTAMP DEFAULT NOW(),
                        UNIQUE(user_id, achievement_id)
                    )
                ''')
                
            print("✅ 資料庫表格初始化完成（包含 RPG 系統）")
            return True
            
        except Exception as e:
            print(f"❌ 資料庫初始化失敗: {e}")
            traceback.print_exc()
            return False
    
    async def execute(self, query, *args):
        """執行 SQL 查詢"""
        if not self.pool or not self.is_connected:
            print(f"⚠️ 資料庫未連接，跳過執行: {query[:50]}...")
            return None
        
        try:
            async with self.pool.acquire() as conn:
                result = await conn.execute(query, *args)
                return result
        except Exception as e:
            print(f"❌ 執行查詢失敗: {e}")
            print(f"   查詢: {query[:100]}...")
            traceback.print_exc()
            return None
    
    async def fetch(self, query, *args):
        """執行查詢並返回結果"""
        if not self.pool or not self.is_connected:
            print(f"⚠️ 資料庫未連接，返回空列表: {query[:50]}...")
            return []
        
        try:
            async with self.pool.acquire() as conn:
                return await conn.fetch(query, *args)
        except Exception as e:
            print(f"❌ 查詢失敗: {e}")
            print(f"   查詢: {query[:100]}...")
            traceback.print_exc()
            return []
    
    async def fetchrow(self, query, *args):
        """執行查詢並返回單行結果"""
        if not self.pool or not self.is_connected:
            print(f"⚠️ 資料庫未連接，返回 None: {query[:50]}...")
            return None
        
        try:
            async with self.pool.acquire() as conn:
                return await conn.fetchrow(query, *args)
        except Exception as e:
            print(f"❌ 查詢失敗: {e}")
            print(f"   查詢: {query[:100]}...")
            traceback.print_exc()
            return None
    
    async def fetchval(self, query, *args):
        """執行查詢並返回單個值"""
        if not self.pool or not self.is_connected:
            print(f"⚠️ 資料庫未連接，返回 None: {query[:50]}...")
            return None
        
        try:
            async with self.pool.acquire() as conn:
                return await conn.fetchval(query, *args)
        except Exception as e:
            print(f"❌ 查詢失敗: {e}")
            print(f"   查詢: {query[:100]}...")
            traceback.print_exc()
            return None

db = Database()

# ========== 記憶體緩存（當資料庫失敗時使用）==========
class MemoryCache:
    def __init__(self):
        self.user_scores = {}  # {user_id_guild_id: {"current": score, "total": score}}
        self.user_profiles = {}  # {user_id_guild_id: profile_data}
        self.chat_scores = {}  # {user_id_guild_id: {"date": date, "score": score}}
        self.prizes = []  # 獎品列表
        self.giveaways = []  # 抽獎列表
        self.events = []  # 活動列表
        
        print("📝 記憶體緩存已初始化")
    
    def get_key(self, user_id, guild_id=0):
        """獲取緩存鍵值"""
        return f"{user_id}_{guild_id}"
    
    async def get_user_score(self, user_id, guild_id=0):
        """從緩存取得用戶積分"""
        key = self.get_key(user_id, guild_id)
        if key in self.user_scores:
            data = self.user_scores[key]
            return data.get("current", 0), data.get("total", 0)
        return 0, 0
    
    async def update_user_score(self, user_id, username, amount, reason="", guild_id=0):
        """在緩存中更新用戶積分"""
        key = self.get_key(user_id, guild_id)
        if key not in self.user_scores:
            self.user_scores[key] = {"current": 0, "total": 0, "username": username}
        
        self.user_scores[key]["current"] += amount
        if amount > 0:
            self.user_scores[key]["total"] += amount
        
        print(f"📝 記憶體緩存: {username} {amount:+d}，目前積分: {self.user_scores[key]['current']}，原因: {reason}")
        return True
    
    async def get_user_profile(self, user_id, guild_id=0):
        """從緩存取得用戶資料"""
        key = self.get_key(user_id, guild_id)
        if key in self.user_profiles:
            return self.user_profiles[key]
        
        # 創建默認資料
        default_profile = {
            'user_id': user_id,
            'current_score': 0,
            'total_score': 0,
            'join_date': datetime.now().strftime('%Y-%m-%d'),
            'profession_counts': {},
            'activity_stats': {},
            'rating_stats': {}
        }
        
        self.user_profiles[key] = default_profile
        return default_profile

memory_cache = MemoryCache()

# ========== 通用函數 ==========

def get_guild_id(interaction_or_context):
    """獲取伺服器ID"""
    if hasattr(interaction_or_context, 'guild'):
        return interaction_or_context.guild.id if interaction_or_context.guild else 0
    elif hasattr(interaction_or_context, 'message'):
        return interaction_or_context.message.guild.id if interaction_or_context.message.guild else 0
    return 0

async def get_user_score(user_id, guild_id=0):
    """取得用戶積分"""
    if db.is_connected:
        result = await db.fetchrow(
            "SELECT current_score, total_score FROM users WHERE user_id = $1 AND guild_id = $2",
            user_id, guild_id
        )
        
        if result:
            return result['current_score'], result['total_score']
        return 0, 0
    else:
        # 使用記憶體緩存
        return await memory_cache.get_user_score(user_id, guild_id)

async def update_user_score(user_id, username, amount, reason="", guild_id=0):
    """更新用戶積分"""
    if db.is_connected:
        try:
            # 先檢查用戶是否已存在
            existing = await db.fetchrow(
                "SELECT user_id FROM users WHERE user_id = $1 AND guild_id = $2",
                user_id, guild_id
            )
            
            if not existing:
                # 用戶不存在，插入新用戶
                current_score = max(amount, 0)
                total_score = max(amount, 0)
                await db.execute(
                    "INSERT INTO users (user_id, username, current_score, total_score, guild_id) VALUES ($1, $2, $3, $4, $5)",
                    user_id, username, current_score, total_score, guild_id
                )
            else:
                # 用戶存在，更新積分
                if amount > 0:
                    await db.execute(
                        "UPDATE users SET current_score = current_score + $1, total_score = total_score + $1, last_active = NOW() WHERE user_id = $2 AND guild_id = $3",
                        amount, user_id, guild_id
                    )
                else:
                    await db.execute(
                        "UPDATE users SET current_score = current_score + $1, last_active = NOW() WHERE user_id = $2 AND guild_id = $3",
                        amount, user_id, guild_id
                    )
            
            # 記錄積分變動
            if amount != 0:
                from_user_id = user_id if amount < 0 else None
                to_user_id = user_id if amount > 0 else None
                reason_text = reason if reason else ("系統扣除" if amount < 0 else "系統增加")
                
                await db.execute(
                    "INSERT INTO score_transfers (from_user_id, to_user_id, amount, reason, guild_id) VALUES ($1, $2, $3, $4, $5)",
                    from_user_id, to_user_id, abs(amount), reason_text, guild_id
                )
            
            return True
            
        except Exception as e:
            print(f"❌ 資料庫更新用戶積分錯誤: {e}")
            # 資料庫失敗時使用記憶體緩存
            return await memory_cache.update_user_score(user_id, username, amount, reason, guild_id)
    else:
        # 使用記憶體緩存
        return await memory_cache.update_user_score(user_id, username, amount, reason, guild_id)

async def add_chat_score(user_id, username, guild_id=0):
    """添加聊天積分"""
    try:
        today = datetime.now().date()
        
        if db.is_connected:
            # 檢查用戶記錄
            result = await db.fetchrow(
                "SELECT last_chat_date, daily_chat_score FROM users WHERE user_id = $1 AND guild_id = $2",
                user_id, guild_id
            )
            
            if not result:
                # 新用戶，創建記錄
                await db.execute(
                    "INSERT INTO users (user_id, username, last_chat_date, daily_chat_score, guild_id) VALUES ($1, $2, $3, $4, $5)",
                    user_id, username, today, CHAT_SCORE, guild_id
                )
                await update_user_score(user_id, username, CHAT_SCORE, "聊天積分", guild_id)
                return CHAT_SCORE, DAILY_CHAT_LIMIT
                
            else:
                last_chat_date = result['last_chat_date']
                daily_chat_score = result['daily_chat_score'] or 0
                
                # 如果是新的一天，重置計數
                if last_chat_date != today:
                    daily_chat_score = CHAT_SCORE
                    await db.execute(
                        "UPDATE users SET last_chat_date = $1, daily_chat_score = $2 WHERE user_id = $3 AND guild_id = $4",
                        today, daily_chat_score, user_id, guild_id
                    )
                    await update_user_score(user_id, username, CHAT_SCORE, "聊天積分", guild_id)
                    return CHAT_SCORE, DAILY_CHAT_LIMIT
                else:
                    # 檢查是否已達上限
                    if daily_chat_score >= DAILY_CHAT_LIMIT:
                        return 0, DAILY_CHAT_LIMIT
                    
                    # 添加積分
                    new_score = min(daily_chat_score + CHAT_SCORE, DAILY_CHAT_LIMIT)
                    added_score = new_score - daily_chat_score
                    
                    await db.execute(
                        "UPDATE users SET daily_chat_score = $1 WHERE user_id = $2 AND guild_id = $3",
                        new_score, user_id, guild_id
                    )
                    
                    if added_score > 0:
                        await update_user_score(user_id, username, added_score, "聊天積分", guild_id)
                    
                    return added_score, DAILY_CHAT_LIMIT
        else:
            # 使用記憶體緩存
            # 簡化處理：每次聊天都給積分，直到達到上限
            current_score, total_score = await get_user_score(user_id, guild_id)
            if current_score >= DAILY_CHAT_LIMIT:
                return 0, DAILY_CHAT_LIMIT
            
            added_score = min(CHAT_SCORE, DAILY_CHAT_LIMIT - current_score)
            await update_user_score(user_id, username, added_score, "聊天積分", guild_id)
            return added_score, DAILY_CHAT_LIMIT
                
    except Exception as e:
        print(f"❌ 添加聊天積分錯誤: {e}")
        traceback.print_exc()
        return 0, DAILY_CHAT_LIMIT

async def get_user_profile(user_id, guild_id=0):
    """獲取用戶完整資料（修正JSON解析問題）"""
    if db.is_connected:
        result = await db.fetchrow(
            "SELECT current_score, total_score, join_date, profession_counts, activity_stats, rating_stats, last_chat_date, daily_chat_score FROM users WHERE user_id = $1 AND guild_id = $2",
            user_id, guild_id
        )
        
        if result:
            current_score = result['current_score'] or 0
            total_score = result['total_score'] or 0
            join_date = result['join_date']
            
            # 修正：確保 JSON 數據是字典而不是字符串
            profession_counts = result['profession_counts']
            activity_stats = result['activity_stats']
            rating_stats = result['rating_stats']
            last_chat_date = result['last_chat_date']
            daily_chat_score = result['daily_chat_score'] or 0
            
            # 處理 JSON 數據
            def parse_json_data(data):
                if data is None:
                    return {}
                if isinstance(data, dict):
                    return data
                if isinstance(data, str):
                    try:
                        return json.loads(data)
                    except:
                        return {}
                return {}
            
            profession_counts = parse_json_data(profession_counts)
            activity_stats = parse_json_data(activity_stats)
            rating_stats = parse_json_data(rating_stats)
            
            try:
                if isinstance(join_date, str):
                    join_date_str = datetime.strptime(join_date.split('.')[0], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
                else:
                    join_date_str = join_date.strftime('%Y-%m-%d')
            except:
                join_date_str = str(join_date)
            
            # 格式化聊天積分信息
            today = datetime.now().date()
            chat_info = ""
            if last_chat_date == today:
                chat_info = f"今日已獲得 {daily_chat_score}/{DAILY_CHAT_LIMIT} 分"
            else:
                chat_info = "今日尚未獲得聊天積分"
            
            return {
                'user_id': user_id,
                'current_score': current_score,
                'total_score': total_score,
                'join_date': join_date_str,
                'profession_counts': profession_counts,
                'activity_stats': activity_stats,
                'rating_stats': rating_stats,
                'chat_info': chat_info,
                'daily_chat_score': daily_chat_score,
                'last_chat_date': last_chat_date
            }
    else:
        # 使用記憶體緩存
        return await memory_cache.get_user_profile(user_id, guild_id)
    
    return None

async def update_user_profession(user_id, profession, guild_id=0):
    """更新用戶職業統計"""
    if not db.is_connected:
        print(f"⚠️ 資料庫未連接，跳過更新職業統計: {user_id}")
        return
    
    try:
        result = await db.fetchrow(
            "SELECT profession_counts, username FROM users WHERE user_id = $1 AND guild_id = $2",
            user_id, guild_id
        )
        
        if result:
            profession_counts = result['profession_counts'] or {}
            username = result['username']
            
            # 解析 JSON 數據
            if isinstance(profession_counts, str):
                try:
                    profession_counts = json.loads(profession_counts)
                except:
                    profession_counts = {}
            
            profession_str = profession
            if profession_str in profession_counts:
                profession_counts[profession_str] += 1
            else:
                profession_counts[profession_str] = 1
            
            bonus_score = PROFESSION_BONUS.get(profession, 0)
            if bonus_score > 0:
                await update_user_score(user_id, username, bonus_score, f"職業加成: {profession}", guild_id)
            
            await db.execute(
                "UPDATE users SET profession_counts = $1 WHERE user_id = $2 AND guild_id = $3",
                json.dumps(profession_counts), user_id, guild_id
            )
            
    except Exception as e:
        print(f"❌ 更新職業統計錯誤: {e}")

async def update_user_activity(user_id, event_name, attended=True, guild_id=0):
    """更新用戶活動統計"""
    if not db.is_connected:
        print(f"⚠️ 資料庫未連接，跳過更新活動統計: {user_id}")
        return
    
    try:
        result = await db.fetchrow(
            "SELECT activity_stats FROM users WHERE user_id = $1 AND guild_id = $2",
            user_id, guild_id
        )
        
        if result:
            activity_stats = result['activity_stats'] or {}
            
            # 解析 JSON 數據
            if isinstance(activity_stats, str):
                try:
                    activity_stats = json.loads(activity_stats)
                except:
                    activity_stats = {}
            
            # 獲取當前半月期
            now = datetime.now()
            year_month = now.strftime("%Y-%m")
            day = now.day
            current_period = f"{year_month}-上半" if day <= 15 else f"{year_month}-下半"
            
            if current_period not in activity_stats:
                activity_stats[current_period] = {"total": 0, "attended": 0}
            
            activity_stats[current_period]["total"] += 1
            if attended:
                activity_stats[current_period]["attended"] += 1
            
            await db.execute(
                "UPDATE users SET activity_stats = $1 WHERE user_id = $2 AND guild_id = $3",
                json.dumps(activity_stats), user_id, guild_id
            )
            
    except Exception as e:
        print(f"❌ 更新活動統計錯誤: {e}")

async def update_user_rating(user_id, rating_type, guild_id=0):
    """更新用戶評核統計"""
    if not db.is_connected:
        print(f"⚠️ 資料庫未連接，跳過更新評核統計: {user_id}")
        return
    
    try:
        result = await db.fetchrow(
            "SELECT rating_stats FROM users WHERE user_id = $1 AND guild_id = $2",
            user_id, guild_id
        )
        
        if result:
            rating_stats = result['rating_stats'] or {}
            
            # 解析 JSON 數據
            if isinstance(rating_stats, str):
                try:
                    rating_stats = json.loads(rating_stats)
                except:
                    rating_stats = {}
            
            rating_str = rating_type
            if rating_str in rating_stats:
                rating_stats[rating_str] += 1
            else:
                rating_stats[rating_str] = 1
            
            score = RATING_SCORES.get(rating_type, 0)
            
            if score != 0:
                await db.execute(
                    """
                    UPDATE users 
                    SET current_score = current_score + $1, 
                        total_score = CASE 
                                        WHEN total_score + $1 > 0 THEN total_score + $1
                                        ELSE 0
                                      END
                    WHERE user_id = $2 AND guild_id = $3
                    """,
                    score, user_id, guild_id
                )
            
            await db.execute(
                "UPDATE users SET rating_stats = $1 WHERE user_id = $2 AND guild_id = $3",
                json.dumps(rating_stats), user_id, guild_id
            )
            
    except Exception as e:
        print(f"❌ 更新評核統計錯誤: {e}")

def get_current_half_month():
    """獲取當前半月期"""
    now = datetime.now()
    year_month = now.strftime("%Y-%m")
    day = now.day
    
    if day <= 15:
        return f"{year_month}-上半"
    else:
        return f"{year_month}-下半"

async def get_total_events_in_period(guild_id=0, period: str = "current"):
    """獲取指定期間內的總活動數"""
    if not db.is_connected:
        return 0
    
    if period == "current":
        # 計算當前半月期內的總活動數
        current_period = get_current_half_month()
        
        # 獲取所有用戶的活動統計
        results = await db.fetch(
            "SELECT activity_stats FROM users WHERE guild_id = $1",
            guild_id
        )
        
        total_events_in_period = 0
        
        for row in results:
            activity_stats = row['activity_stats'] or {}
            if isinstance(activity_stats, str):
                try:
                    activity_stats = json.loads(activity_stats)
                except:
                    activity_stats = {}
                    
            if current_period in activity_stats:
                user_total = activity_stats[current_period].get("total", 0)
                if user_total > total_events_in_period:
                    total_events_in_period = user_total
        
        return total_events_in_period
        
    else:  # all
        # 計算所有活動的總數
        result = await db.fetchrow(
            "SELECT COUNT(*) FROM evaluation_events WHERE guild_id = $1",
            guild_id
        )
        total_events = result['count'] if result else 0
        
        return total_events

async def get_all_attendance_data(guild_id=0, period: str = "current"):
    """獲取所有用戶的出席數據"""
    if not db.is_connected:
        return []
    
    # 獲取總活動數
    total_events = await get_total_events_in_period(guild_id, period)
    
    if total_events == 0:
        return []
    
    # 獲取所有用戶
    results = await db.fetch(
        "SELECT user_id, username, activity_stats FROM users WHERE guild_id = $1",
        guild_id
    )
    
    rankings = []
    current_period = get_current_half_month()
    
    for row in results:
        user_id = row['user_id']
        username = row['username']
        activity_stats = row['activity_stats'] or {}
        
        # 解析 JSON 數據
        if isinstance(activity_stats, str):
            try:
                activity_stats = json.loads(activity_stats)
            except:
                activity_stats = {}
        
        if period == "current":
            if current_period in activity_stats:
                data = activity_stats[current_period]
                attended_count = data.get("attended", 0)
            else:
                attended_count = 0
        else:
            attended_count = 0
            for period_key, data in activity_stats.items():
                attended_count += data.get("attended", 0)
        
        attendance_rate = (attended_count / total_events) * 100 if total_events > 0 else 0
        
        rankings.append({
            'user_id': user_id,
            'username': username,
            'attendance_rate': attendance_rate,
            'attended': attended_count,
            'total': total_events,
            'period': current_period if period == "current" else "全部"
        })
    
    rankings.sort(key=lambda x: (-x['attendance_rate'], x['username']))
    return rankings

async def end_giveaway(message_id: int, manual: bool = False, guild_id=0):
    """結束抽獎"""
    if not db.is_connected:
        return
    
    try:
        result = await db.fetchrow(
            """
            SELECT id, creator_id, prize, winner_count, participants, winners, channel_id 
            FROM giveaways 
            WHERE message_id = $1 AND is_active = true AND guild_id = $2
            """,
            message_id, guild_id
        )
        
        if not result:
            return
        
        giveaway_id = result['id']
        creator_id = result['creator_id']
        prize = result['prize']
        winner_count = result['winner_count']
        participants = result['participants'] or []
        winners = result['winners'] or []
        channel_id = result['channel_id']
        
        # ========== 新增：除錯日誌 ==========
        print(f"🔍 抽獎結束除錯:")
        print(f"  - 抽獎ID: {giveaway_id}")
        print(f"  - participants 原始數據: {participants}")
        print(f"  - participants 類型: {type(participants)}")
        
        channel = bot.get_channel(channel_id)
        
        if not channel:
            return
        
        try:
            message = await channel.fetch_message(message_id)
        except:
            return
        
        if participants:
            # ========== 修正：確保 participants 是正確的陣列 ==========
            # 方法 1：如果是字符串，解析為 JSON
            if isinstance(participants, str):
                try:
                    participants = json.loads(participants)
                    print(f"  - 已解析 participants: {participants}")
                except Exception as e:
                    print(f"  ❌ 解析 participants 失敗: {e}")
                    participants = []
            
            # 方法 2：如果是列表，確保元素是整數（Discord ID）
            if isinstance(participants, list):
                # 清理列表，確保所有元素都是整數
                cleaned_participants = []
                for p in participants:
                    try:
                        if isinstance(p, str) and p.isdigit():
                            cleaned_participants.append(int(p))
                        elif isinstance(p, int):
                            cleaned_participants.append(p)
                        else:
                            print(f"  ⚠️ 跳過無效的參與者: {p} (類型: {type(p)})")
                    except Exception as e:
                        print(f"  ⚠️ 處理參與者時錯誤: {e}")
                
                participants = cleaned_participants
                print(f"  - 清理後的 participants: {participants}")
            
            # ========== 修正：選擇中獎者 ==========
            if not participants:
                print(f"  ⚠️ 沒有有效的參與者")
                winners_list = []
            elif len(participants) <= winner_count:
                winners_list = participants  # 所有人都是中獎者
                print(f"  - 中獎者 (所有人): {winners_list}")
            else:
                winners_list = random.sample(participants, winner_count)
                print(f"  - 隨機選擇的中獎者: {winners_list}")
            
            # ========== 修正：確保 winners_list 是字符串列表 ==========
            # Discord ID 需要是字符串格式來顯示 <@ID>
            winners_list_str = [str(uid) for uid in winners_list]
            print(f"  - 中獎者字符串格式: {winners_list_str}")
            
            # 存入資料庫（確保是 JSON 格式）
            winners_json = json.dumps(winners_list_str)
            print(f"  - 存入資料庫的 JSON: {winners_json}")
            
            await db.execute(
                "UPDATE giveaways SET winners = $1, is_active = false WHERE id = $2",
                winners_json, giveaway_id
            )
            
            # ========== 修正：顯示中獎者 ==========
            new_embed = discord.Embed(
                title="🎉 抽獎已結束 🎉",
                description="開獎完成！",
                color=0x00FF00
            )
            
            new_embed.add_field(name="🎁 獎品", value=prize, inline=True)
            new_embed.add_field(name="👑 中獎人數", value=str(len(winners_list)), inline=True)
            new_embed.add_field(name="🎫 參與人數", value=f"{len(participants)} 人", inline=True)
            
            winners_text = ""
            for i, winner_id in enumerate(winners_list[:5], 1):
                winners_text += f"{i}. <@{winner_id}>\n"
            
            if len(winners_list) > 5:
                winners_text += f"... 還有 {len(winners_list) - 5} 人"
            
            if winners_text:
                new_embed.add_field(name="🏆 獲獎者", value=winners_text, inline=False)
            
            await message.edit(embed=new_embed)
            await message.clear_reactions()
            
            # ========== 修正：發送中獎通知 ==========
            for winner_id in winners_list:
                await channel.send(f"🎉 恭喜 <@{winner_id}> 獲得了 **{prize}**！")
                
        else:
            print(f"  ⚠️ 沒有參與者")
            new_embed = discord.Embed(
                title="🎉 抽獎已結束",
                description="無人參與抽獎" + ("（手動結束）" if manual else ""),
                color=0xFF0000
            )
            await message.edit(embed=new_embed)
            await message.clear_reactions()
        
    except Exception as e:
        print(f"❌ 結束抽獎錯誤: {e}")
        traceback.print_exc()
        
async def end_evaluation(event_id, channel, event_name, guild_id=0):
    """結束評核活動"""
    if not db.is_connected:
        return
    
    try:
        result = await db.fetchrow(
            """
            SELECT participants, professions, ratings, rating_message_id 
            FROM evaluation_events 
            WHERE id = $1 AND guild_id = $2
            """,
            event_id, guild_id
        )
        
        if not result:
            return
        
        participants = result['participants'] or []
        professions = result['professions'] or {}
        ratings = result['ratings'] or {}
        rating_message_id = result['rating_message_id']
        
        # 解析 JSON 數據
        if isinstance(participants, str):
            try:
                participants = json.loads(participants)
            except:
                participants = []
        
        if isinstance(professions, str):
            try:
                professions = json.loads(professions)
            except:
                professions = {}
        
        if isinstance(ratings, str):
            try:
                ratings = json.loads(ratings)
            except:
                ratings = {}
        
        await db.execute(
            "UPDATE evaluation_events SET is_active = false WHERE id = $1",
            event_id
        )
    
        try:
            rating_message = await channel.fetch_message(rating_message_id)
            await rating_message.clear_reactions()
            
            end_embed = discord.Embed(
                title=f"✅ 評核活動已結束：{event_name}",
                description="此活動的評核階段已經結束，感謝所有參與者！",
                color=discord.Color.green()
            )
            
            end_embed.add_field(name="📊 統計信息", value=f"**總參與人數：** {len(participants)} 人", inline=False)
            
            rating_summary = {}
            for user_id, rating_list in ratings.items():
                if rating_list:
                    if isinstance(rating_list, list):
                        latest_rating = rating_list[-1]["rating"] if rating_list else None
                    else:
                        latest_rating = rating_list
                    if latest_rating:
                        rating_summary[latest_rating] = rating_summary.get(latest_rating, 0) + 1
            
            rating_text = ""
            for rating_type in ["優秀", "良好", "普通", "不合格"]:
                count = rating_summary.get(rating_type, 0)
                if count > 0:
                    rating_text += f"**{rating_type}：** {count}人\n"
            
            if rating_text:
                end_embed.add_field(name="⭐ 評級分佈", value=rating_text, inline=False)
            
            await rating_message.edit(embed=end_embed)
            
        except Exception as e:
            print(f"❌ 更新評核訊息錯誤: {e}")
        
        summary_embed = discord.Embed(
            title=f"🏁 活動總結：{event_name}",
            description="評核活動已正式結束！",
            color=discord.Color.gold()
        )
        
        summary_embed.add_field(name="👥 參與人數", value=f"{len(participants)} 人", inline=True)
        summary_embed.add_field(name="🎮 職業選擇", value=f"{len(professions)} 人", inline=True)
        summary_embed.add_field(name="⭐ 評核完成", value=f"{len(ratings)} 人", inline=True)
        summary_embed.add_field(name="📊 評級分佈", value=f"{len(rating_summary)} 種評級", inline=True)
        
        await channel.send(embed=summary_embed)
        
        print(f"✅ 評核活動已結束: {event_name}")
        
    except Exception as e:
        print(f"❌ 結束評核活動錯誤: {e}")

async def log_query(query_type: str, user_id: int, parameters: dict, guild_id: int = 0):
    """記錄查詢日誌"""
    if not db.is_connected:
        return
    
    try:
        await db.execute(
            "INSERT INTO query_logs (query_type, user_id, parameters, guild_id) VALUES ($1, $2, $3, $4)",
            query_type, user_id, json.dumps(parameters), guild_id
        )
    except Exception as e:
        print(f"❌ 記錄查詢日誌錯誤: {e}")

# ========== RPG 輔助函數 ==========

async def create_rpg_player(user_id: int, nickname: str = None) -> bool:
    """創建 RPG 玩家角色（修正版）"""
    if not db.is_connected:
        print(f"❌ 資料庫未連接，無法創建角色: {user_id}")
        return False
    
    try:
        username = nickname or f"冒險者{user_id}"
        
        print(f"🔄 嘗試創建角色: {user_id} - {username}")
        
        # 檢查是否已有角色
        existing = await db.fetchrow(
            "SELECT user_id FROM rpg_players WHERE user_id = $1",
            user_id
        )
        
        if existing:
            print(f"⚠️ 角色已存在: {user_id}")
            return True  # 已有角色
        
        # 創建新角色
        print(f"📝 正在創建新角色: {user_id} - {username}")
        
        await db.execute('''
            INSERT INTO rpg_players (
                user_id, nickname, level, exp, max_exp,
                vitality, speed, strength, intelligence, carrying_capacity,
                current_hp, max_hp, current_mp, max_mp,
                house_type, storage_capacity,
                current_map, current_layer, is_in_town,
                created_at, last_active, last_heal_time
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
        ''',
            user_id,
            username,
            1,      # level
            0,      # exp
            100,    # max_exp
            10,     # vitality
            10,     # speed
            10,     # strength
            10,     # intelligence
            10,     # carrying_capacity
            100,    # current_hp
            100,    # max_hp
            50,     # current_mp
            50,     # max_mp
            'orphanage',    # house_type
            20,             # storage_capacity
            '新手森林',     # current_map
            1,              # current_layer
            True,           # is_in_town
            datetime.now(), # created_at
            datetime.now(), # last_active
            datetime.now()  # last_heal_time
        )
        
        print(f"✅ 基礎角色創建成功: {user_id} - {username}")
        
        # 給予初始裝備（簡化版）
        # 1. 木劍
        weapon_id = await db.fetchval('''
            INSERT INTO rpg_items (
                name, item_type, rarity, level_requirement,
                weapon_type, description, base_price
            ) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id
        ''',
            "木劍", "weapon", "green", 1, "sword", "最基本的劍", 50
        )
        
        if weapon_id:
            # 添加到背包
            await db.execute('''
                INSERT INTO rpg_inventory (user_id, item_id, quantity, slot_type, location)
                VALUES ($1, $2, $3, $4, $5)
            ''',
                user_id, weapon_id, 1, "inventory", "personal"
            )
            
            # 裝備武器
            await db.execute(
                "UPDATE rpg_players SET weapon_id = $1 WHERE user_id = $2",
                weapon_id, user_id
            )
        
        # 2. 布衣
        armor_id = await db.fetchval('''
            INSERT INTO rpg_items (
                name, item_type, rarity, level_requirement,
                vitality_bonus, description, base_price
            ) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id
        ''',
            "布衣", "body", "green", 1, 5, "最基本的衣服", 30
        )
        
        if armor_id:
            await db.execute('''
                INSERT INTO rpg_inventory (user_id, item_id, quantity, slot_type, location)
                VALUES ($1, $2, $3, $4, $5)
            ''',
                user_id, armor_id, 1, "inventory", "personal"
            )
            
            await db.execute(
                "UPDATE rpg_players SET body_id = $1 WHERE user_id = $2",
                armor_id, user_id
            )
        
        # 3. 小紅藥水
        potion_id = await db.fetchval('''
            INSERT INTO rpg_items (
                name, item_type, rarity, level_requirement,
                potion_type, potion_value, description, base_price
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id
        ''',
            "小紅藥水", "potion", "normal", 1, "hp", 30, "恢復少量HP", 20
        )
        
        if potion_id:
            await db.execute('''
                INSERT INTO rpg_inventory (user_id, item_id, quantity, slot_type, location)
                VALUES ($1, $2, $3, $4, $5)
            ''',
                user_id, potion_id, 1, "inventory", "personal"
            )
        
        print(f"🎉 RPG 角色創建完成: {user_id} ({username})")
        return True
        
    except Exception as e:
        print(f"❌ 創建 RPG 角色失敗: {e}")
        traceback.print_exc()
        return False

async def get_rpg_player(user_id: int):
    """獲取 RPG 玩家資料"""
    if not db.is_connected:
        return None
    
    try:
        player = await db.fetchrow(
            '''
            SELECT 
                p.*,
                w.name as weapon_name,
                h.name as head_name,
                b.name as body_name,
                s.name as shoes_name,
                n.name as necklace_name,
                r.name as ring_name,
                bp.name as backpack_name
            FROM rpg_players p
            LEFT JOIN rpg_items w ON p.weapon_id = w.id
            LEFT JOIN rpg_items h ON p.head_id = h.id
            LEFT JOIN rpg_items b ON p.body_id = b.id
            LEFT JOIN rpg_items s ON p.shoes_id = s.id
            LEFT JOIN rpg_items n ON p.necklace_id = n.id
            LEFT JOIN rpg_items r ON p.ring_id = r.id
            LEFT JOIN rpg_items bp ON p.backpack_id = bp.id
            WHERE p.user_id = $1
            ''',
            user_id
        )
        
        if player:
            # 計算總屬性（基礎+裝備）
            total_stats = calculate_total_stats(player)
            player['total_stats'] = total_stats
            
            # 計算戰鬥力
            player['combat_power'] = calculate_combat_power(total_stats)
            
            # 計算下一級所需經驗
            player['exp_to_next'] = calculate_exp_required(player['level'])
            
            # 計算屬性點剩餘
            total_stat_points = (player['level'] - 1) * RPG_CONFIG["STAT_POINTS_PER_LEVEL"]
            used_stat_points = (
                (player['vitality'] - RPG_CONFIG["BASE_STATS"]["vitality"]) +
                (player['speed'] - RPG_CONFIG["BASE_STATS"]["speed"]) +
                (player['strength'] - RPG_CONFIG["BASE_STATS"]["strength"]) +
                (player['intelligence'] - RPG_CONFIG["BASE_STATS"]["intelligence"]) +
                (player['carrying_capacity'] - RPG_CONFIG["BASE_STATS"]["carrying_capacity"])
            )
            player['remaining_stat_points'] = max(0, total_stat_points - used_stat_points)
            
        return player
        
    except Exception as e:
        print(f"❌ 獲取 RPG 玩家資料失敗: {e}")
        return None

def calculate_total_stats(player):
    """計算總屬性"""
    stats = {
        'vitality': player['vitality'],
        'speed': player['speed'],
        'strength': player['strength'],
        'intelligence': player['intelligence'],
        'carrying_capacity': player['carrying_capacity'],
        'max_hp': player['max_hp'],
        'max_mp': player['max_mp']
    }
    
    # 這裡之後會加上裝備加成
    return stats

def calculate_combat_power(stats):
    """計算戰鬥力"""
    combat_power = (
        stats['vitality'] * 10 +     # 體力對血量的貢獻
        stats['speed'] * 5 +         # 速度對先手和迴避的貢獻
        stats['strength'] * 15 +     # 力量對物理攻擊的貢獻
        stats['intelligence'] * 12   # 智慧對魔法攻擊的貢獻
    )
    return combat_power

def calculate_exp_required(level):
    """計算升級所需經驗值"""
    base_exp = RPG_CONFIG["EXP_CURVE"]["base_exp"]
    growth_rate = RPG_CONFIG["EXP_CURVE"]["growth_rate"]
    return int(base_exp * (growth_rate ** (level - 1)))

async def create_rpg_item(name, item_type, rarity, level_req=1,
                         vit_bonus=0, spd_bonus=0, str_bonus=0, int_bonus=0, cap_bonus=0,
                         weapon_type=None, skill_name=None, skill_mp_cost=0, description="",
                         potion_type=None, potion_value=0):
    """創建 RPG 物品"""
    if not db.is_connected:
        return None
    
    try:
        # 根據稀有度生成特殊詞條
        special_effects = {}
        if rarity in ["blue", "purple", "gold"]:
            # 隨機選擇詞條
            available_effects = list(RPG_CONFIG["SPECIAL_EFFECTS"].keys())
            if rarity == "blue":
                # 藍裝：1個詞條
                effect = random.choice(available_effects)
                special_effects[effect] = RPG_CONFIG["SPECIAL_EFFECTS"][effect]
            elif rarity == "purple":
                # 紫裝：1個詞條（比藍裝強）
                effect = random.choice(available_effects[:9])  # 前9個較強的詞條
                special_effects[effect] = RPG_CONFIG["SPECIAL_EFFECTS"][effect]
            else:  # gold
                # 金裝：3個詞條
                effects = random.sample(available_effects[:12], 3)  # 前12個較強的詞條中選3個
                for effect in effects:
                    special_effects[effect] = RPG_CONFIG["SPECIAL_EFFECTS"][effect]
        
        result = await db.fetchrow(
            '''
            INSERT INTO rpg_items (
                name, item_type, rarity, level_requirement,
                vitality_bonus, speed_bonus, strength_bonus, intelligence_bonus, carrying_capacity_bonus,
                special_effects, weapon_type, skill_name, skill_mp_cost, description,
                potion_type, potion_value, max_durability, current_durability, base_price
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
            RETURNING id
            ''',
            name, item_type, rarity, level_req,
            vit_bonus, spd_bonus, str_bonus, int_bonus, cap_bonus,
            json.dumps(special_effects, ensure_ascii=False),
            weapon_type, skill_name, skill_mp_cost, description,
            potion_type, potion_value,
            100, 100,  # 耐久度
            calculate_item_price(rarity, level_req, len(special_effects))  # 基礎價格
        )
        
        return result['id'] if result else None
        
    except Exception as e:
        print(f"❌ 創建 RPG 物品失敗: {e}")
        return None

def calculate_item_price(rarity, level, effect_count):
    """計算物品價格"""
    price_multipliers = {
        "green": 1.0,
        "blue": 2.5,
        "purple": 6.0,
        "gold": 15.0
    }
    
    base_price = 100
    rarity_multiplier = price_multipliers.get(rarity, 1.0)
    level_multiplier = 1 + (level / 10)
    effect_multiplier = 1 + (effect_count * 0.3)
    
    return int(base_price * rarity_multiplier * level_multiplier * effect_multiplier)

async def add_item_to_inventory(user_id, item_id, quantity=1, slot_type="inventory", location="personal"):
    """添加物品到背包/倉庫"""
    if not db.is_connected:
        return False
    
    try:
        # 檢查是否有相同物品
        existing = await db.fetchrow(
            "SELECT id, quantity FROM rpg_inventory WHERE user_id = $1 AND item_id = $2 AND slot_type = $3 AND location = $4",
            user_id, item_id, slot_type, location
        )
        
        if existing:
            # 增加數量
            await db.execute(
                "UPDATE rpg_inventory SET quantity = quantity + $1 WHERE id = $2",
                quantity, existing['id']
            )
        else:
            # 創建新記錄
            # 尋找空槽位
            max_slots = 50  # 基礎背包大小
            used_slots = await db.fetch(
                "SELECT slot_index FROM rpg_inventory WHERE user_id = $1 AND slot_type = $2 AND location = $3",
                user_id, slot_type, location
            )
            used_indices = {slot['slot_index'] for slot in used_slots}
            
            # 尋找第一個空槽位
            slot_index = None
            for i in range(max_slots):
                if i not in used_indices:
                    slot_index = i
                    break
            
            if slot_index is None:
                return False  # 背包已滿
            
            await db.execute(
                '''
                INSERT INTO rpg_inventory (user_id, item_id, quantity, slot_type, slot_index, location)
                VALUES ($1, $2, $3, $4, $5, $6)
                ''',
                user_id, item_id, quantity, slot_type, slot_index, location
            )
        
        return True
        
    except Exception as e:
        print(f"❌ 添加物品到背包失敗: {e}")
        return False

async def equip_item(user_id, item_id, slot):
    """裝備物品"""
    if not db.is_connected:
        return False
    
    try:
        # 獲取物品資訊
        item = await db.fetchrow(
            "SELECT item_type, level_requirement FROM rpg_items WHERE id = $1",
            item_id
        )
        
        if not item:
            return False
        
        # 檢查玩家等級
        player = await get_rpg_player(user_id)
        if not player or player['level'] < item['level_requirement']:
            return False
        
        # 更新裝備欄位
        slot_column = f"{slot}_id"
        await db.execute(
            f"UPDATE rpg_players SET {slot_column} = $1 WHERE user_id = $2",
            item_id, user_id
        )
        
        # 標記物品為已裝備
        await db.execute(
            "UPDATE rpg_items SET is_equipped = true, owner_id = $1 WHERE id = $2",
            user_id, item_id
        )
        
        # 從背包移除（如果存在）
        await db.execute(
            "DELETE FROM rpg_inventory WHERE user_id = $1 AND item_id = $2 AND slot_type = 'inventory'",
            user_id, item_id
        )
        
        return True
        
    except Exception as e:
        print(f"❌ 裝備物品失敗: {e}")
        return False

def create_progress_bar(percentage, length=20):
    """創建進度條"""
    filled = int((percentage / 100) * length)
    empty = length - filled
    
    if filled == length:
        bar = "█" * filled
    else:
        bar = "█" * filled + "░" * empty
    
    return f"[{bar}]"

# ========== 新增測試指令 ==========

@tree.command(name="db_status", description="檢查資料庫連接狀態")
async def db_status_slash(interaction: discord.Interaction):
    """檢查資料庫狀態"""
    await interaction.response.defer(ephemeral=True)
    
    try:
        embed = discord.Embed(title="📊 資料庫狀態檢查", color=0x7289DA)
        
        # 檢查連接狀態
        if db.is_connected:
            embed.add_field(name="🔌 連接狀態", value="✅ 已連接", inline=True)
            
            # 測試查詢
            try:
                test_result = await db.fetchval("SELECT 1")
                if test_result == 1:
                    embed.add_field(name="🔧 測試查詢", value="✅ 成功", inline=True)
                else:
                    embed.add_field(name="🔧 測試查詢", value="❌ 失敗", inline=True)
                    
                # 檢查表格
                tables = await db.fetch("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                """)
                
                table_count = len(tables)
                embed.add_field(name="📋 資料表數量", value=f"{table_count} 個", inline=True)
                
                if table_count > 0:
                    table_list = "\n".join([f"• {t['table_name']}" for t in tables[:5]])
                    if table_count > 5:
                        table_list += f"\n... 還有 {table_count-5} 個"
                    embed.add_field(name="📊 資料表列表", value=table_list, inline=False)
                
            except Exception as e:
                embed.add_field(name="🔧 測試查詢", value=f"❌ 錯誤: {str(e)[:100]}", inline=True)
                
        else:
            embed.add_field(name="🔌 連接狀態", value="❌ 未連接", inline=True)
            
            # 檢查環境變數
            database_url = os.getenv('DATABASE_URL')
            if database_url:
                # 隱藏密碼顯示
                safe_url = database_url
                if '@' in database_url:
                    safe_url = database_url.split('@')[0] + '@***/***'
                embed.add_field(name="🔑 DATABASE_URL", value=f"✅ 已設定\n`{safe_url[:50]}...`", inline=False)
            else:
                embed.add_field(name="🔑 DATABASE_URL", value="❌ 未設定", inline=False)
            
            embed.add_field(
                name="💡 解決方案",
                value="1. 檢查 Railway 環境變數\n2. 重啟機器人\n3. 聯繫管理員",
                inline=False
            )
        
        # 添加記憶體緩存狀態
        cache_status = "✅ 啟用中" if not db.is_connected else "⚡ 備用中"
        embed.add_field(name="📝 記憶體緩存", value=cache_status, inline=True)
        
        embed.set_footer(text=f"檢查時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        await interaction.followup.send(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.followup.send(f"❌ 檢查失敗: {str(e)[:100]}", ephemeral=True)

@tree.command(name="test_chat_score", description="測試聊天積分功能")
async def test_chat_score_slash(interaction: discord.Interaction):
    """測試聊天積分"""
    await interaction.response.defer(ephemeral=True)
    
    guild_id = get_guild_id(interaction)
    
    # 測試添加聊天積分
    added_score, daily_limit = await add_chat_score(
        interaction.user.id,
        interaction.user.name,
        guild_id
    )
    
    # 獲取當前狀態
    current_score, total_score = await get_user_score(interaction.user.id, guild_id)
    
    embed = discord.Embed(
        title="💬 聊天積分測試",
        color=0x00FF00 if added_score > 0 else 0xFFA500
    )
    
    embed.add_field(name="本次獲得積分", value=f"+{added_score} 分", inline=True)
    embed.add_field(name="當前總積分", value=f"{current_score} 分", inline=True)
    embed.add_field(name="歷史總積分", value=f"{total_score} 分", inline=True)
    embed.add_field(name="積分規則", value=f"每句話 +{CHAT_SCORE} 分", inline=False)
    embed.add_field(name="每日上限", value=f"{DAILY_CHAT_LIMIT} 分", inline=True)
    embed.add_field(name="下次重置", value="每日 UTC+8 00:00", inline=True)
    embed.add_field(name="資料庫狀態", value="✅ 正常" if db.is_connected else "⚠️ 使用緩存", inline=True)
    
    await interaction.followup.send(embed=embed, ephemeral=True)

# ========== 同步指令 ==========

@tree.command(name="sync", description="同步斜槓指令（擁有者）")
async def sync_slash(interaction: discord.Interaction):
    """同步指令"""
    await interaction.response.defer(ephemeral=True)
    
    if interaction.user.id not in OWNER_IDS:
        embed = discord.Embed(
            title="❌ 權限不足",
            description="只有機器人擁有者可以使用此指令",
            color=0xFF0000
        )
        await interaction.followup.send(embed=embed, ephemeral=True)
        return
    
    try:
        global_synced = await tree.sync()
        
        embed = discord.Embed(
            title="🔄 指令同步完成",
            description=f"已同步 {len(global_synced)} 個指令",
            color=0x43B581
        )
        
        await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 同步失敗",
            description=f"錯誤訊息: {str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed, ephemeral=True)

# ========== 用戶指令 (10個) ==========
# ========== RPG 診斷指令 ==========

@tree.command(name="rpg_debug", description="RPG 系統診斷")
async def rpg_debug_slash(interaction: discord.Interaction):
    """RPG 系統診斷"""
    await interaction.response.defer(ephemeral=True)
    
    try:
        user_id = interaction.user.id
        
        embed = discord.Embed(
            title="🔧 RPG 系統診斷報告",
            color=0x7289DA
        )
        
        # 1. 檢查資料庫連接
        if db.is_connected:
            embed.add_field(
                name="🔌 資料庫連接",
                value="✅ 正常",
                inline=True
            )
            
            # 測試查詢
            try:
                test_result = await db.fetchval("SELECT 1")
                embed.add_field(
                    name="🔍 查詢測試",
                    value=f"✅ 成功 (結果: {test_result})",
                    inline=True
                )
            except Exception as e:
                embed.add_field(
                    name="🔍 查詢測試",
                    value=f"❌ 失敗: {str(e)[:50]}",
                    inline=True
                )
        else:
            embed.add_field(
                name="🔌 資料庫連接",
                value="❌ 未連接",
                inline=True
            )
        
        # 2. 檢查 RPG 表格是否存在
        if db.is_connected:
            try:
                # 檢查主要 RPG 表格
                tables_to_check = [
                    "rpg_players",
                    "rpg_items", 
                    "rpg_inventory",
                    "rpg_monsters"
                ]
                
                table_status = []
                for table in tables_to_check:
                    exists = await db.fetchval(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = '{table}'
                        )
                    """)
                    table_status.append((table, exists))
                
                tables_text = ""
                for table, exists in table_status:
                    status = "✅" if exists else "❌"
                    tables_text += f"{status} {table}\n"
                
                embed.add_field(
                    name="📋 RPG 資料表狀態",
                    value=tables_text,
                    inline=False
                )
                
            except Exception as e:
                embed.add_field(
                    name="📋 RPG 資料表檢查",
                    value=f"❌ 檢查失敗: {str(e)[:100]}",
                    inline=False
                )
        
        # 3. 檢查角色是否存在
        if db.is_connected:
            try:
                player = await db.fetchrow(
                    "SELECT * FROM rpg_players WHERE user_id = $1",
                    user_id
                )
                
                if player:
                    embed.add_field(
                        name="👤 你的角色狀態",
                        value=f"✅ 角色存在\n名稱: {player.get('nickname', '未知')}\n等級: {player.get('level', 1)}",
                        inline=True
                    )
                    
                    # 檢查裝備
                    weapon_name = "無"
                    if player.get('weapon_id'):
                        weapon = await db.fetchrow(
                            "SELECT name FROM rpg_items WHERE id = $1",
                            player['weapon_id']
                        )
                        if weapon:
                            weapon_name = weapon['name']
                    
                    embed.add_field(
                        name="⚔️ 當前武器",
                        value=weapon_name,
                        inline=True
                    )
                else:
                    embed.add_field(
                        name="👤 你的角色狀態",
                        value="❌ 角色不存在\n使用 `/rpg_start` 創建角色",
                        inline=True
                    )
                    
            except Exception as e:
                embed.add_field(
                    name="👤 角色檢查",
                    value=f"❌ 檢查失敗: {str(e)[:100]}",
                    inline=True
                )
        
        # 4. 創建角色測試
        embed.add_field(
            name="🔧 快速測試",
            value="點擊下方按鈕測試角色創建",
            inline=False
        )
        
        await interaction.followup.send(embed=embed, ephemeral=True, view=RPGDebugView(user_id))
        
    except Exception as e:
        await interaction.followup.send(f"❌ 診斷失敗: {str(e)[:200]}", ephemeral=True)

class RPGDebugView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
    
    @discord.ui.button(label="測試創建角色", style=discord.ButtonStyle.primary, emoji="🧪")
    async def test_create_character(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ 這不是你的診斷！", ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        
        try:
            # 直接執行創建角色
            success = await create_rpg_player_simple(interaction.user.id, "測試角色")
            
            if success:
                await interaction.followup.send("✅ 測試創建成功！請使用 `/rpg_status` 查看", ephemeral=True)
            else:
                await interaction.followup.send("❌ 測試創建失敗", ephemeral=True)
                
        except Exception as e:
            await interaction.followup.send(f"❌ 測試失敗: {str(e)[:100]}", ephemeral=True)
    
    @discord.ui.button(label="查看所有用戶", style=discord.ButtonStyle.secondary, emoji="👥")
    async def view_all_players(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ 這不是你的診斷！", ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        
        try:
            players = await db.fetch(
                "SELECT user_id, nickname, level FROM rpg_players ORDER BY level DESC LIMIT 10"
            )
            
            if players:
                player_list = "\n".join([f"• {p['nickname']} (ID: {p['user_id']}) Lv.{p['level']}" for p in players])
                await interaction.followup.send(f"**👥 RPG 玩家列表 (前10名)**\n{player_list}", ephemeral=True)
            else:
                await interaction.followup.send("❌ 資料庫中沒有任何 RPG 玩家", ephemeral=True)
                
        except Exception as e:
            await interaction.followup.send(f"❌ 查詢失敗: {str(e)[:100]}", ephemeral=True)

async def create_rpg_player_simple(user_id: int, nickname: str = None) -> bool:
    """簡化版角色創建（用於測試）"""
    try:
        username = nickname or f"冒險者{user_id}"
        
        print(f"🧪 測試創建角色: {user_id} - {username}")
        
        # 直接插入，不檢查是否已存在
        await db.execute('''
            INSERT INTO rpg_players (
                user_id, nickname, level, exp, max_exp,
                vitality, speed, strength, intelligence, carrying_capacity,
                current_hp, max_hp, current_mp, max_mp,
                house_type, storage_capacity,
                current_map, current_layer, is_in_town
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
        ''',
            user_id,
            username,
            1,      # level
            0,      # exp
            100,    # max_exp
            10,     # vitality
            10,     # speed
            10,     # strength
            10,     # intelligence
            10,     # carrying_capacity
            100,    # current_hp
            100,    # max_hp
            50,     # current_mp
            50,     # max_mp
            'orphanage',    # house_type
            20,             # storage_capacity
            '新手森林',     # current_map
            1,              # current_layer
            True            # is_in_town
        )
        
        print(f"✅ 測試創建成功: {user_id}")
        return True
        
    except Exception as e:
        print(f"❌ 測試創建失敗: {e}")
        return False
        
@tree.command(name="help", description="顯示幫助訊息")
async def help_slash(interaction: discord.Interaction):
    """顯示幫助"""
    embed = discord.Embed(
        title="🤖 小雲機械人 - 幫助中心",
        description="以下是可用指令列表：",
        color=0x7289DA
    )
    
    embed.add_field(
        name="🎮 RPG 指令 (新增)",
        value=(
            "`/rpg_start [暱稱]` - 開始 RPG 冒險\n"
            "`/rpg_status` - 查看角色狀態\n"
            "`/rpg_inventory` - 查看背包 **(開發中)**\n"
            "`/rpg_equip` - 裝備管理 **(開發中)**\n"
            "`/rpg_explore` - 開始冒險 **(開發中)**\n"
            "`/rpg_shop` - 商店系統 **(開發中)**"
        ),
        inline=False
    )
    
    embed.add_field(
        name="👤 用戶指令 (11個)",
        value=(
            "`/help` - 顯示此幫助訊息\n"
            "`/profile` - 查看我的數據\n"
            "`/giveaway [獎品] [時間]` - 創建抽獎\n"
            "`/score_draw` - 使用積分抽獎\n"
            "`/score_transfer [用戶] [積分]` - 轉移積分給其他用戶\n"
            "`/prizelist` - 查看彩池列表\n"
            "`/random_team [人數] [組數]` - 隨機分組\n"
            "`/score_ranking` - 查看積分排行榜\n"
            "`/attendance_ranking` - 查看出席率排行榜\n"
            "`/blessing` - 測試今日運程\n"
            "`/test_chat_score` - 測試聊天積分"
        ),
        inline=False
    )
    
    embed.add_field(
        name="🛠️ 管理員指令 (4個)",
        value=(
            "`/add_prize [名稱] [類型] [數量]` - **調整彩池 (可增減)**\n"
            "`/add_score [用戶] [積分] [原因]` - 加減積分\n"
            "`/create_event [活動名稱]` - 創建評核活動\n"
            "`/activity_stats` - 查看活動統計"
        ),
        inline=False
    )
    
    embed.add_field(
        name="🔧 系統指令 (2個)",
        value=(
            "`/sync` - 同步指令（擁有者）\n"
            "`/db_status` - 檢查資料庫狀態"
        ),
        inline=False
    )
    
    embed.add_field(
        name="💰 積分系統",
        value=(
            f"**聊天獎勵：** 每句話 +{CHAT_SCORE} 分，每日上限 {DAILY_CHAT_LIMIT} 分\n"
            "**簽到獎勵：** 40 積分\n"
            "**職業加成：** 補師 +20 積分\n"
            "**評核獎勵：**\n"
            "  • 優秀：+40 積分\n"
            "  • 良好：+10 積分\n"
            "  • 普通：+0 積分（預設）\n"
            "  • 不合格：-5 積分"
        ),
        inline=False
    )
    
    # 檢查 RPG 系統狀態
    rpg_status = "✅ 正常" if db.is_connected else "⚠️ 待資料庫連接"
    
    db_status = "✅ 正常" if db.is_connected else "⚠️ 使用緩存"
    embed.set_footer(text=f"總指令數: 20個 | RPG 狀態: {rpg_status} | 資料庫狀態: {db_status}")
    await interaction.response.send_message(embed=embed)

@tree.command(name="profile", description="查看我的數據")
async def profile_slash(interaction: discord.Interaction):
    """查看用戶資料"""
    await interaction.response.defer()
    
    try:
        # 檢查資料庫連接
        if not db.is_connected:
            embed = discord.Embed(
                title="ℹ️ 資料庫未連接",
                description="目前使用記憶體緩存模式，數據可能不會永久保存。",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
        
        user_id = interaction.user.id
        username = interaction.user.name
        guild_id = get_guild_id(interaction)
        
        await log_query("profile", user_id, {"action": "view_profile"}, guild_id)
        
        profile = await get_user_profile(user_id, guild_id)
        
        if not profile:
            if db.is_connected:
                await db.execute(
                    "INSERT INTO users (user_id, username, current_score, total_score, guild_id) VALUES ($1, $2, $3, $4, $5)",
                    user_id, username, 0, 0, guild_id
                )
            
            profile = {
                'user_id': user_id,
                'current_score': 0,
                'total_score': 0,
                'join_date': datetime.now().strftime('%Y-%m-%d'),
                'profession_counts': {},
                'activity_stats': {},
                'rating_stats': {},
                'chat_info': '尚未開始聊天',
                'daily_chat_score': 0
            }
        
        current_score = profile['current_score']
        total_score = profile['total_score']
        join_date_str = profile['join_date']
        profession_counts = profile['profession_counts']
        activity_stats = profile['activity_stats']
        rating_stats = profile['rating_stats']
        chat_info = profile.get('chat_info', '')
        daily_chat_score = profile.get('daily_chat_score', 0)
        
        current_period = get_current_half_month()
        period_data = activity_stats.get(current_period, {})
        total_events = period_data.get('total', 0)
        attended_events = period_data.get('attended', 0)
        attendance_rate = (attended_events / total_events * 100) if total_events > 0 else 0.0
        
        embed = discord.Embed(
            title=f"📊 {username} 的評核數據",
            color=0x43B581
        )
        
        # 添加資料庫狀態標記
        db_status = "✅ 資料庫" if db.is_connected else "📝 緩存"
        embed.add_field(name="儲存狀態", value=db_status, inline=True)
        
        # 添加聊天積分信息
        embed.add_field(
            name="💬 今日聊天積分",
            value=f"{chat_info}\n每日上限: {DAILY_CHAT_LIMIT} 分\n每句話: +{CHAT_SCORE} 分",
            inline=True
        )
        
        attendance_info = (
            f"**當前半月期：** {current_period}\n"
            f"**總活動數：** {total_events} 次\n"
            f"**實際出席：** {attended_events} 次\n"
            f"**出席率：** {attendance_rate:.1f}%\n\n"
            f"**計算公式：** (實際出席次數 ÷ 總活動數) × 100%\n"
            f"**註：** 僅計算活動時間內簽到"
        )
        
        embed.add_field(
            name="📅 半月期出席率",
            value=attendance_info,
            inline=False
        )
        
        # 修正這裡的縮排問題
        score_info = f"**當前積分：** {current_score} 分\n"
        score_info += f"**總獲得積分：** {total_score} 分\n"
        score_info += f"**可用積分：** {current_score} 分\n\n"
        score_info += f"**積分規則：**\n"
        score_info += f"• 聊天：每句話 +{CHAT_SCORE}分（每日上限 {DAILY_CHAT_LIMIT}分）\n"
        score_info += f"• 簽到：+{SIGNUP_SCORE}分\n"
        for profession, bonus in PROFESSION_BONUS.items():
            if bonus > 0:
                score_info += f"• {profession}：+{bonus}分\n"
        score_info += f"• 優秀：+{RATING_SCORES['優秀']}分\n"
        score_info += f"• 良好：+{RATING_SCORES['良好']}分\n"
        score_info += f"• 普通：{RATING_SCORES['普通']}分（預設）\n"
        score_info += f"• 不合格：{RATING_SCORES['不合格']}分"
        
        embed.add_field(
            name="💰 積分統計",
            value=score_info,
            inline=False
        )
        
        if profession_counts:
            profession_info = ""
            total_plays = sum(profession_counts.values())
            for profession, count in profession_counts.items():
                percentage = (count / total_plays * 100) if total_plays > 0 else 0
                profession_info += f"**{profession}：** {count}次 ({percentage:.1f}%)\n"
        else:
            profession_info = "尚未記錄職業數據"
        
        embed.add_field(
            name="🎮 職業統計",
            value=profession_info,
            inline=False
        )
        
        if rating_stats:
            rating_info = ""
            total_ratings = sum(rating_stats.values())
            total_rating_score = 0
            
            for rating_type in ["優秀", "良好", "普通", "不合格"]:
                count = rating_stats.get(rating_type, 0)
                if count > 0:
                    percentage = (count / total_ratings * 100) if total_ratings > 0 else 0
                    score = RATING_SCORES.get(rating_type, 0)
                    rating_info += f"**{rating_type}：** {count}次 ({percentage:.1f}%)\n"
                    total_rating_score += count * score
            
            if total_ratings > 0:
                rating_info += f"\n**評核總獲得積分：** {total_rating_score} 分"
        else:
            rating_info = "尚未有評核記錄"
        
        embed.add_field(
            name="⭐ 評核統計",
            value=rating_info,
            inline=False
        )
        
        embed.add_field(name="用戶ID", value=f"`{user_id}`", inline=True)
        embed.add_field(name="加入日期", value=join_date_str, inline=True)
        
        if interaction.user.avatar:
            embed.set_thumbnail(url=interaction.user.avatar.url)
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 發生錯誤",
            description=f"無法讀取用戶資料：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)
        
        # ... 其他程式碼 ...
        
    except Exception as e:  # <-- 確保有這個 except 區塊
        error_embed = discord.Embed(
            title="❌ 發生錯誤",
            description=f"無法讀取用戶資料：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)
        
        if profession_counts:
            profession_info = ""
            total_plays = sum(profession_counts.values())
            for profession, count in profession_counts.items():
                percentage = (count / total_plays * 100) if total_plays > 0 else 0
                profession_info += f"**{profession}：** {count}次 ({percentage:.1f}%)\n"
        else:
            profession_info = "尚未記錄職業數據"
        
        embed.add_field(
            name="🎮 職業統計",
            value=profession_info,
            inline=False
        )
        
        if rating_stats:
            rating_info = ""
            total_ratings = sum(rating_stats.values())
            total_rating_score = 0
            
            for rating_type in ["優秀", "良好", "普通", "不合格"]:
                count = rating_stats.get(rating_type, 0)
                if count > 0:
                    percentage = (count / total_ratings * 100) if total_ratings > 0 else 0
                    score = RATING_SCORES.get(rating_type, 0)
                    rating_info += f"**{rating_type}：** {count}次 ({percentage:.1f}%)\n"
                    total_rating_score += count * score
            
            if total_ratings > 0:
                rating_info += f"\n**評核總獲得積分：** {total_rating_score} 分"
        else:
            rating_info = "尚未有評核記錄"
        
        embed.add_field(
            name="⭐ 評核統計",
            value=rating_info,
            inline=False
        )
        
        embed.add_field(name="用戶ID", value=f"`{user_id}`", inline=True)
        embed.add_field(name="加入日期", value=join_date_str, inline=True)
        
        if interaction.user.avatar:
            embed.set_thumbnail(url=interaction.user.avatar.url)
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 發生錯誤",
            description=f"無法讀取用戶資料：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)
        
@tree.command(name="giveaway", description="創建抽獎活動")
@app_commands.describe(
    prize="獎品內容",
    duration="抽獎持續時間（例如：60s, 1m, 1h, 1d）",
    winners="獲獎人數"
)
async def giveaway_slash(
    interaction: discord.Interaction,
    prize: str,
    duration: str = "1h",
    winners: int = 1
):
    """創建抽獎"""
    await interaction.response.defer()
    
    try:
        if not db.is_connected:
            embed = discord.Embed(
                title="⚠️ 資料庫未連接",
                description="無法創建抽獎，請稍後再試。",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        guild_id = get_guild_id(interaction)
        await log_query("giveaway", interaction.user.id, {"prize": prize, "duration": duration, "winners": winners}, guild_id)
        
        duration_lower = duration.lower().strip()
        seconds = 3600
        
        if duration_lower.endswith('s'):
            seconds = int(duration_lower[:-1])
        elif duration_lower.endswith('m'):
            seconds = int(duration_lower[:-1]) * 60
        elif duration_lower.endswith('h'):
            seconds = int(duration_lower[:-1]) * 3600
        elif duration_lower.endswith('d'):
            seconds = int(duration_lower[:-1]) * 86400
        elif duration_lower.isdigit():
            seconds = int(duration_lower)
        
        if seconds < 10:
            await interaction.followup.send("❌ 抽獎時間必須至少10秒！")
            return
        
        if seconds > 86400 * 7:
            await interaction.followup.send("❌ 抽獎時間不能超過7天！")
            return
        
        end_time = datetime.now() + timedelta(seconds=seconds)
        
        # 簡單的時間顯示
        if seconds < 60:
            time_display = f"{seconds}秒"
        elif seconds < 3600:
            minutes = seconds // 60
            time_display = f"{minutes}分鐘"
        elif seconds < 86400:
            hours = seconds // 3600
            time_display = f"{hours}小時"
        else:
            days = seconds // 86400
            time_display = f"{days}天"
        
        embed = discord.Embed(
            title="🎉 自動抽獎活動 🎉",
            description="時間到自動開獎！",
            color=0xFFD700
        )
        
        embed.add_field(name="🎁 獎品", value=prize, inline=True)
        embed.add_field(name="👑 中獎人數", value=str(winners), inline=True)
        embed.add_field(name="⏰ 結束時間", value=f"{time_display}內", inline=True)
        embed.add_field(name="🎫 參與人數", value="0 人", inline=True)
        embed.add_field(name="📝 參與方式", value="點擊下方 🎫 按鈕參與", inline=True)
        embed.add_field(name="🔧 主辦人操作", value="點擊 ⏹️ 手動結束抽獎", inline=True)
        
        creator_name = interaction.user.display_name
        giveaway_id = f"giveaway_{int(time.time())}_{random.randint(1000, 9999)}"
        
        embed.set_footer(text=f"抽獎ID: {giveaway_id} | 主辦人: {creator_name} • {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        
        await interaction.followup.send(embed=embed)
        message = await interaction.original_response()
        
        # 添加反應
        await message.add_reaction("🎫")
        await message.add_reaction("⏹️")
        
        # 使用記憶體緩存參與者列表，減少資料庫讀取
        participants_cache = []
        
        # 創建抽獎記錄
        await db.execute(
            '''
            INSERT INTO giveaways (creator_id, prize, winner_count, end_time, message_id, channel_id, guild_id, participants)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ''',
            interaction.user.id, prize, winners, end_time, message.id, interaction.channel.id, guild_id, json.dumps([])
        )
        
        print(f"✅ 抽獎已創建: 獎品={prize}, 時間={seconds}秒, 訊息ID={message.id}")
        
        async def countdown_timer():
            nonlocal participants_cache
            remaining = seconds
            
            # 更高效的更新邏輯：只在不頻繁的時間點更新顯示
            last_update_time = time.time()
            update_interval = 5  # 每5秒檢查一次是否有新參與者
            
            while remaining > 0:
                start_time = time.time()
                
                # 只在需要時更新顯示
                if time.time() - last_update_time >= update_interval or remaining <= 30:
                    # 從資料庫獲取最新參與者
                    result = await db.fetchrow(
                        "SELECT participants FROM giveaways WHERE message_id = $1 AND guild_id = $2",
                        message.id, guild_id
                    )
                    
                    if result and result['participants']:
                        participants = result['participants']
                        if isinstance(participants, str):
                            try:
                                participants = json.loads(participants)
                            except:
                                participants = []
                        
                        # 只有當參與者數量變化時才更新訊息
                        if participants != participants_cache:
                            participants_cache = participants.copy()
                            participants_count = len(participants)
                            
                            # 更新時間顯示
                            if remaining < 60:
                                time_display = f"{remaining}秒"
                            elif remaining < 3600:
                                minutes = remaining // 60
                                time_display = f"{minutes}分鐘"
                            else:
                                hours = remaining // 3600
                                time_display = f"{hours}小時"
                            
                            new_embed = discord.Embed(
                                title="🎉 自動抽獎活動 🎉",
                                description="時間到自動開獎！",
                                color=0xFFD700
                            )
                            
                            new_embed.add_field(name="🎁 獎品", value=prize, inline=True)
                            new_embed.add_field(name="👑 中獎人數", value=str(winners), inline=True)
                            new_embed.add_field(name="⏰ 結束時間", value=f"{time_display}內", inline=True)
                            new_embed.add_field(name="🎫 參與人數", value=f"{participants_count} 人", inline=True)
                            new_embed.add_field(name="📝 參與方式", value="點擊下方 🎫 按鈕參與", inline=True)
                            new_embed.add_field(name="🔧 主辦人操作", value="點擊 ⏹️ 手動結束抽獎", inline=True)
                            
                            # 顯示最近5個參與者
                            if participants_count > 0:
                                recent_participants = []
                                for uid in participants[-5:]:  # 只取最後5個
                                    recent_participants.append(f"<@{uid}>")
                                
                                participants_text = ", ".join(recent_participants)
                                if participants_count > 5:
                                    participants_text += f" 等 {participants_count} 人"
                                
                                new_embed.add_field(
                                    name="👥 參與者",
                                    value=participants_text,
                                    inline=False
                                )
                            
                            new_embed.set_footer(text=f"抽獎ID: {giveaway_id} | 主辦人: {creator_name} • {datetime.now().strftime('%H:%M')}")
                            
                            try:
                                await message.edit(embed=new_embed)
                            except Exception as e:
                                print(f"更新抽獎訊息失敗: {e}")
                            
                            last_update_time = time.time()
                
                # 計算剩餘時間（考慮處理時間）
                elapsed = time.time() - start_time
                sleep_time = max(0.1, 1 - elapsed)  # 確保至少休眠0.1秒
                await asyncio.sleep(sleep_time)
                remaining -= 1
            
            # 抽獎結束
            await end_giveaway(message.id, guild_id=guild_id)
        
        # 啟動計時器
        asyncio.create_task(countdown_timer())
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 創建抽獎失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

@tree.command(name="score_draw", description="使用積分抽獎")
async def score_draw_slash(interaction: discord.Interaction):
    """積分抽獎"""
    await interaction.response.defer()
    
    try:
        if not db.is_connected:
            embed = discord.Embed(
                title="⚠️ 資料庫未連接",
                description="無法進行積分抽獎，請稍後再試。",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        guild_id = get_guild_id(interaction)
        await log_query("score_draw", interaction.user.id, {"action": "open_draw"}, guild_id)
        
        current_score, _ = await get_user_score(interaction.user.id, guild_id)
        
        embed = discord.Embed(
            title="🎲 積分抽獎系統",
            description="請選擇要扣除的積分進行抽獎：",
            color=0x9B59B6
        )
        
        embed.add_field(
            name="🟢 50積分抽獎",
            value="• 綠箱 70%\n• 藍箱 25%\n• 紫箱 4.5%\n• 金箱 0.5%",
            inline=True
        )
        
        embed.add_field(
            name="🔵 100積分抽獎",
            value="• 綠箱 50%\n• 藍箱 40%\n• 紫箱 9%\n• 金箱 1%",
            inline=True
        )
        
        embed.add_field(
            name="🟣 500積分抽獎",
            value="• 綠箱 10%\n• 藍箱 65%\n• 紫箱 20%\n• 金箱 5%",
            inline=True
        )
        
        embed.add_field(
            name="💰 你的積分",
            value=f"{current_score} 分",
            inline=False
        )
        
        embed.set_footer(text="點擊下方對應的emoji選擇抽獎類型")
        
        class ScoreDrawView(discord.ui.View):
            def __init__(self, user_id, guild_id):
                super().__init__(timeout=60)
                self.user_id = user_id
                self.guild_id = guild_id
            
            @discord.ui.button(label="50分", style=discord.ButtonStyle.success, emoji="🟢", row=0)
            async def fifty_points(self, interaction: discord.Interaction, button: discord.ui.Button):
                await self.process_draw(interaction, 50)
            
            @discord.ui.button(label="100分", style=discord.ButtonStyle.primary, emoji="🔵", row=0)
            async def hundred_points(self, interaction: discord.Interaction, button: discord.ui.Button):
                await self.process_draw(interaction, 100)
            
            @discord.ui.button(label="500分", style=discord.ButtonStyle.secondary, emoji="🟣", row=1)
            async def five_hundred_points(self, interaction: discord.Interaction, button: discord.ui.Button):
                await self.process_draw(interaction, 500)
            
            async def process_draw(self, interaction: discord.Interaction, score_cost: int):
                if interaction.user.id != self.user_id:
                    await interaction.response.send_message("❌ 這不是你的抽獎！", ephemeral=True)
                    return
                
                current_score, _ = await get_user_score(interaction.user.id, self.guild_id)
                if current_score < score_cost:
                    await interaction.response.send_message(
                        f"❌ 積分不足！需要 {score_cost} 分，你目前有 {current_score} 分",
                        ephemeral=True
                    )
                    return
                
                weights = {
                    50: {"綠箱": 70, "藍箱": 25, "紫箱": 4.5, "金箱": 0.5},
                    100: {"綠箱": 50, "藍箱": 40, "紫箱": 9, "金箱": 1},
                    500: {"綠箱": 10, "藍箱": 65, "紫箱": 20, "金箱": 5}
                }
                
                box_weights = weights[score_cost]
                box_types = list(box_weights.keys())
                box_weights_list = list(box_weights.values())
                selected_box = random.choices(box_types, weights=box_weights_list, k=1)[0]
                
                result = await db.fetchrow(
                    "SELECT id, prize_name FROM prize_pool WHERE box_level = $1 AND remaining > 0 AND guild_id = $2 ORDER BY RANDOM() LIMIT 1",
                    selected_box, self.guild_id
                )
                
                if not result:
                    await interaction.response.send_message(f"❌ {selected_box}中沒有可用獎品！", ephemeral=True)
                    return
                
                prize_id = result['id']
                prize_name = result['prize_name']
                
                await update_user_score(interaction.user.id, interaction.user.name, -score_cost, f"積分抽獎: {score_cost}分", self.guild_id)
                
                await db.execute(
                    "UPDATE prize_pool SET remaining = remaining - 1 WHERE id = $1",
                    prize_id
                )
                
                await db.execute(
                    '''
                    INSERT INTO score_draws (creator_id, score_cost, box_level, winner_prize, winner_id, guild_id)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ''',
                    interaction.user.id, score_cost, selected_box, prize_name, interaction.user.id, self.guild_id
                )
                
                colors = {
                    "綠箱": 0x00FF00,
                    "藍箱": 0x0000FF,
                    "紫箱": 0x800080,
                    "金箱": 0xFFD700
                }
                
                result_embed = discord.Embed(
                    title=f"🎉 抽獎結果：{selected_box} 🎉",
                    description=f"花費 {score_cost} 積分進行抽獎",
                    color=colors.get(selected_box, 0x7289DA)
                )
                
                result_embed.add_field(name="🎁 獲得的獎品", value=prize_name, inline=True)
                result_embed.add_field(name="💰 消耗積分", value=f"-{score_cost} 分", inline=True)
                result_embed.add_field(name="🎲 箱子類型", value=selected_box, inline=True)
                
                current_score, total_score = await get_user_score(interaction.user.id, self.guild_id)
                result_embed.add_field(name="💎 剩餘積分", value=f"{current_score} 分", inline=True)
                
                await interaction.response.edit_message(embed=result_embed, view=None)
                await interaction.followup.send(f"🎉 <@{interaction.user.id}> 使用了 {score_cost} 積分抽獎，獲得了 **{prize_name}**！")
        
        await interaction.followup.send(embed=embed, view=ScoreDrawView(interaction.user.id, guild_id))
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 抽獎失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)
@tree.command(name="score_transfer", description="轉移積分給其他用戶")
@app_commands.describe(
    user="要轉移給誰",
    amount="轉移積分數量",
    reason="轉移原因（可選）"
)
async def score_transfer_slash(
    interaction: discord.Interaction,
    user: discord.Member,
    amount: int,
    reason: str = ""
):
    """轉移積分"""
    await interaction.response.defer()
    
    try:
        if amount <= 0:
            await interaction.followup.send("❌ 轉移積分必須大於0！")
            return
        
        if user.bot:
            await interaction.followup.send("❌ 不能轉移積分給機器人！")
            return
        
        if interaction.user.id == user.id:
            await interaction.followup.send("❌ 不能轉移積分給自己！")
            return
        
        guild_id = get_guild_id(interaction)
        
        current_score, _ = await get_user_score(interaction.user.id, guild_id)
        
        if current_score < amount:
            await interaction.followup.send(f"❌ 積分不足！你的積分：{current_score}，需要：{amount}")
            return
        
        reason_text = reason if reason else f"轉移給 {user.name}"
        
        await update_user_score(interaction.user.id, interaction.user.name, -amount, f"轉移給 {user.name}: {reason_text}", guild_id)
        await update_user_score(user.id, user.name, amount, f"從 {interaction.user.name} 收到: {reason_text}", guild_id)
        
        embed = discord.Embed(
            title="✅ 積分轉移成功",
            description=f"{interaction.user.mention} 轉移了 **{amount}** 積分給 {user.mention}",
            color=0x00FF00
        )
        
        embed.add_field(name="📝 轉移原因", value=reason_text if reason else "未指定原因", inline=True)
        
        new_score_sender, _ = await get_user_score(interaction.user.id, guild_id)
        new_score_receiver, _ = await get_user_score(user.id, guild_id)
        
        embed.add_field(name="💰 剩餘積分", value=f"{interaction.user.mention}: {new_score_sender}分\n{user.mention}: {new_score_receiver}分", inline=False)
        
        await interaction.followup.send(embed=embed)
        
        await log_query("score_transfer", interaction.user.id, {"to_user": user.id, "amount": amount, "reason": reason}, guild_id)
        
    except Exception as e:
        await interaction.followup.send(f"❌ 轉移積分失敗：{str(e)}")

@tree.command(name="prizelist", description="查看彩池列表")
async def prizelist_slash(interaction: discord.Interaction):
    """查看彩池"""
    await interaction.response.defer()
    
    try:
        if not db.is_connected:
            embed = discord.Embed(
                title="⚠️ 資料庫未連接",
                description="無法讀取彩池列表，請稍後再試。",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        guild_id = get_guild_id(interaction)
        
        await log_query("prizelist", interaction.user.id, {"action": "view_prizes"}, guild_id)
        
        results = await db.fetch(
            "SELECT prize_name, box_level, quantity, remaining FROM prize_pool WHERE guild_id = $1 ORDER BY box_level, prize_name",
            guild_id
        )
        
        if not results:
            embed = discord.Embed(
                title="📦 彩池列表",
                description="目前彩池是空的，請使用 `/add_prize` 添加獎品。",
                color=0xFFD700
            )
            await interaction.followup.send(embed=embed)
            return
        
        boxes = {}
        total_prizes = 0
        
        for row in results:
            box_level = row['box_level']
            if box_level not in boxes:
                boxes[box_level] = []
            
            quantity = row['quantity'] or 0
            remaining = row['remaining'] or 0
            prize_name = row['prize_name']
            
            boxes[box_level].append({
                "name": prize_name,
                "quantity": quantity,
                "remaining": remaining
            })
            total_prizes += remaining
        
        embed = discord.Embed(
            title="📦 彩池列表",
            description=f"總獎品數量：{total_prizes} 個",
            color=0xFFD700
        )
        
        box_colors = {
            "綠箱": 0x00FF00,
            "藍箱": 0x0000FF,
            "紫箱": 0x800080,
            "金箱": 0xFFD700
        }
        
        for box_level, prizes in boxes.items():
            prize_list = ""
            total_box_remaining = 0
            
            for prize in prizes:
                prize_list += f"• **{prize['name']}** - {prize['remaining']}/{prize['quantity']}個\n"
                total_box_remaining += prize['remaining']
            
            color = box_colors.get(box_level, 0x7289DA)
            
            embed.add_field(
                name=f"{box_level} ({total_box_remaining}個)",
                value=prize_list if prize_list else "無獎品",
                inline=False
            )
        
        embed.set_footer(text="使用 /add_prize 添加獎品 | 使用 /score_draw 進行抽獎")
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 讀取彩池失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

@tree.command(name="random_team", description="隨機分組")
@app_commands.describe(
    team_size="每組人數",
    team_count="組數（可選）"
)
async def random_team_slash(
    interaction: discord.Interaction,
    team_size: int,
    team_count: int = None
):
    """隨機分組"""
    await interaction.response.defer()
    
    try:
        if team_size <= 0:
            await interaction.followup.send("❌ 每組人數必須大於0！")
            return
        
        if team_count is not None and team_count <= 0:
            await interaction.followup.send("❌ 組數必須大於0！")
            return
        
        voice_channel = interaction.user.voice
        if not voice_channel or not voice_channel.channel:
            await interaction.followup.send("❌ 請先加入一個語音頻道！")
            return
        
        voice_channel = voice_channel.channel
        members = [member for member in voice_channel.members if not member.bot]
        
        if len(members) < 2:
            await interaction.followup.send("❌ 語音頻道中至少需要2名非機器人成員！")
            return
        
        random.shuffle(members)
        
        if team_count is None:
            team_count = len(members) // team_size
            if len(members) % team_size != 0:
                team_count += 1
        
        teams = []
        for i in range(team_count):
            start_idx = i * team_size
            end_idx = start_idx + team_size
            team = members[start_idx:end_idx]
            if team:
                teams.append(team)
        
        embed = discord.Embed(
            title="🎲 隨機分組結果",
            description=f"語音頻道：{voice_channel.name}\n總人數：{len(members)}人",
            color=0x7289DA
        )
        
        for i, team in enumerate(teams, 1):
            team_members = "\n".join([f"• {member.mention}" for member in team])
            embed.add_field(
                name=f"第 {i} 組 ({len(team)}人)",
                value=team_members,
                inline=False
            )
        
        embed.set_footer(text=f"分組方式：每組 {team_size} 人 | 共 {len(teams)} 組")
        
        await interaction.followup.send(embed=embed)
        
        await log_query("random_team", interaction.user.id, {"team_size": team_size, "team_count": team_count, "total_members": len(members)}, get_guild_id(interaction))
        
    except Exception as e:
        await interaction.followup.send(f"❌ 分組失敗：{str(e)}")

@tree.command(name="score_ranking", description="查看積分排行榜")
@app_commands.describe(
    show_all="是否顯示所有用戶（預設顯示前10名）"
)
async def score_ranking_slash(
    interaction: discord.Interaction,
    show_all: bool = False
):
    """積分排行榜"""
    await interaction.response.defer()
    
    try:
        if not db.is_connected:
            embed = discord.Embed(
                title="⚠️ 資料庫未連接",
                description="無法讀取排行榜，請稍後再試。",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        guild_id = get_guild_id(interaction)
        
        await log_query("score_ranking", interaction.user.id, {"show_all": show_all}, guild_id)
        
        if show_all:
            results = await db.fetch(
                "SELECT username, current_score, total_score FROM users WHERE guild_id = $1 ORDER BY current_score DESC",
                guild_id
            )
        else:
            results = await db.fetch(
                "SELECT username, current_score, total_score FROM users WHERE guild_id = $1 ORDER BY current_score DESC LIMIT 10",
                guild_id
            )
        
        if not results:
            embed = discord.Embed(
                title="🏆 積分排行榜",
                description="目前還沒有用戶資料",
                color=0xFFD700
            )
            await interaction.followup.send(embed=embed)
            return
        
        total_users = await db.fetchval(
            "SELECT COUNT(*) FROM users WHERE guild_id = $1",
            guild_id
        ) or 0
        
        embed = discord.Embed(
            title="🏆 積分排行榜",
            description=f"總用戶數：{total_users} 人" + ("（顯示前10名）" if not show_all and total_users > 10 else ""),
            color=0xFFD700
        )
        
        ranks = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
        
        for i, row in enumerate(results, 1):
            username = row['username'] or "未知用戶"
            current_score = row['current_score'] or 0
            total_score = row['total_score'] or 0
            
            rank_emoji = ranks[i-1] if i <= 10 else f"**#{i}**"
            
            embed.add_field(
                name=f"{rank_emoji} {username}",
                value=f"**當前積分：** {current_score}\n**總獲得積分：** {total_score}",
                inline=False
            )
        
        user_score, user_total = await get_user_score(interaction.user.id, guild_id)
        user_rank = await db.fetchval(
            """
            SELECT COUNT(*) + 1 FROM users 
            WHERE guild_id = $1 AND current_score > $2
            """,
            guild_id, user_score
        ) or 1
        
        embed.add_field(
            name="📊 你的排名",
            value=f"**排名：** 第 {user_rank} 名\n**當前積分：** {user_score}\n**總獲得積分：** {user_total}",
            inline=False
        )
        
        embed.set_footer(text=f"使用 /profile 查看詳細數據" + (" | 使用 /score_ranking show_all:true 查看全部" if not show_all and total_users > 10 else ""))
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 讀取排行榜失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

@tree.command(name="attendance_ranking", description="查看出席率排行榜")
@app_commands.describe(
    period="統計期間",
    show_all="是否顯示所有用戶"
)
async def attendance_ranking_slash(
    interaction: discord.Interaction,
    period: Literal["current", "all"] = "current",
    show_all: bool = False
):
    """出席率排行榜"""
    await interaction.response.defer()
    
    try:
        guild_id = get_guild_id(interaction)
        
        await log_query("attendance_ranking", interaction.user.id, {"period": period, "show_all": show_all}, guild_id)
        
        rankings = await get_all_attendance_data(guild_id, period)
        
        if not rankings:
            embed = discord.Embed(
                title="📊 出席率排行榜",
                description="目前還沒有活動數據",
                color=0xFFD700
            )
            await interaction.followup.send(embed=embed)
            return
        
        embed = discord.Embed(
            title="📊 出席率排行榜",
            description=f"統計期間：{period}",
            color=0xFFD700
        )
        
        if period == "current":
            current_period = get_current_half_month()
            embed.description += f"（{current_period}）"
        
        ranks = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
        limit = len(rankings) if show_all else min(10, len(rankings))
        
        total_events = rankings[0]['total'] if rankings else 0
        
        for i in range(limit):
            data = rankings[i]
            username = data['username'] or f"用戶 {data['user_id']}"
            attendance_rate = data['attendance_rate']
            attended = data['attended']
            
            rank_emoji = ranks[i] if i < 10 else f"**#{i+1}**"
            
            bar = create_progress_bar(attendance_rate, 15)
            
            embed.add_field(
                name=f"{rank_emoji} {username}",
                value=(
                    f"{bar}\n"
                    f"**出席率：** {attendance_rate:.1f}%\n"
                    f"**出席次數：** {attended}/{total_events}"
                ),
                inline=False
            )
        
        user_data = next((r for r in rankings if r['user_id'] == interaction.user.id), None)
        user_rank = next((i+1 for i, r in enumerate(rankings) if r['user_id'] == interaction.user.id), None)
        
        if user_data:
            user_bar = create_progress_bar(user_data['attendance_rate'], 15)
            
            embed.add_field(
                name="📊 你的數據",
                value=(
                    f"{user_bar}\n"
                    f"**排名：** 第 {user_rank} 名\n"
                    f"**出席率：** {user_data['attendance_rate']:.1f}%\n"
                    f"**出席次數：** {user_data['attended']}/{total_events}"
                ),
                inline=False
            )
        
        embed.set_footer(
            text=f"總活動數：{total_events} | 總用戶數：{len(rankings)}人" + 
                 (" | 使用 /attendance_ranking show_all:true 查看全部" if not show_all and len(rankings) > 10 else "")
        )
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 讀取出席率失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

@tree.command(name="blessing", description="測試今日運程（阿爾比恩版）")
async def blessing_slash(interaction: discord.Interaction):
    """測試今日運程 - 阿爾比恩特別版"""
    await interaction.response.defer()
    
    try:
        # ========== 運程等級定義 ==========
        fortune_levels = {
            "大吉": {
                "weight": 20,      # 20% 機率
                "color": 0xFFD700,  # 金色
                "emoji": "🌟",
                "title_suffix": " ✨ 鴻運當頭 ✨"
            },
            "吉": {
                "weight": 25,      # 25% 機率
                "color": 0x00FF00,  # 綠色
                "emoji": "🍀",
                "title_suffix": " 👍 順心如意"
            },
            "中平": {
                "weight": 30,      # 30% 機率
                "color": 0xFFFF00,  # 黃色
                "emoji": "⚖️",
                "title_suffix": " 🤝 平穩過渡"
            },
            "凶": {
                "weight": 15,      # 15% 機率
                "color": 0xFF4500,  # 橙色
                "emoji": "⚠️",
                "title_suffix": " ⚠️ 謹慎行事"
            },
            "大凶": {
                "weight": 10,      # 10% 機率
                "color": 0xFF0000,  # 紅色
                "emoji": "💀",
                "title_suffix": " 🔥 挑戰考驗"
            }
        }
        
        # ========== 大量祝福語庫 ==========
        fortune_texts = {
            "大吉": [
                "今日運勢爆棚！做什麼都順風順水！",
                "貴人相助，事半功倍的一天！",
                "財神眷顧，財運亨通！",
                "靈感湧現，創意無限！",
                "人際關係和諧，合作順利！",
                "健康狀況極佳，精力充沛！",
                "學習效率超高，吸收力強！",
                "愛情甜蜜，感情升溫！",
                "機會來敲門，好好把握！",
                "心想事成，美夢成真！",
                "事業突破，晉升有望！",
                "投資眼光精準，回報豐厚！",
                "團隊合作無間，戰績輝煌！",
                "幸運女神特別眷顧你！",
                "一切障礙都將迎刃而解！",
                "光芒四射，成為焦點人物！",
                "收穫滿滿，成果豐碩！",
                "正能量滿滿，影響他人！",
                "突破極限，創造奇蹟！",
                "天時地利人和，完美的一天！"
            ],
            "吉": [
                "運勢不錯，小有收穫！",
                "平穩順利，無風無浪！",
                "小小驚喜在前方等著你！",
                "人際關係融洽，心情愉快！",
                "工作學習進展順利！",
                "健康狀況良好，精神飽滿！",
                "財運平穩，小有進帳！",
                "感情穩定，溫馨甜蜜！",
                "有新的機會出現，值得嘗試！",
                "努力會有回報，繼續加油！",
                "遇到困難也能順利解決！",
                "團隊氣氛和諧，合作愉快！",
                "靈感來臨，創作順利！",
                "計劃順利推進，目標可期！",
                "得到他人幫助，心存感激！",
                "心情舒暢，正能量滿滿！",
                "小確幸不斷，幸福感提升！",
                "溝通順暢，誤會化解！",
                "身體健康，活力充沛！",
                "平靜安穩，享受當下！"
            ],
            "中平": [
                "保持平常心，平安就是福！",
                "平淡的一天，但無災無難！",
                "中庸之道，不偏不倚！",
                "按部就班，穩步前進！",
                "保持現狀，蓄勢待發！",
                "風平浪靜，適合反思規劃！",
                "不特別好也不特別壞！",
                "適合整理思緒，重新出發！",
                "平平淡淡才是真！",
                "維持現狀，等待時機！",
                "沒有驚喜也沒有驚嚇！",
                "適合休息充電的一天！",
                "保持平衡，避免極端！",
                "穩紮穩打，步步為營！",
                "中規中矩，平安度過！",
                "適合低調行事的一天！",
                "保持冷靜，理性思考！",
                "不求有功，但求無過！",
                "平平穩穩，順其自然！",
                "休息是為了走更長遠的路！"
            ],
            "凶": [
                "小心駛得萬年船，謹慎行事！",
                "可能遇到小麻煩，保持冷靜！",
                "注意健康，適當休息！",
                "財務上要小心謹慎！",
                "人際關係可能有些緊張！",
                "計劃可能遇到阻礙！",
                "情緒波動較大，需要調整！",
                "注意溝通，避免誤會！",
                "做事要更細心一些！",
                "可能會有小小的不順心！",
                "壓力較大，需要紓解！",
                "注意時間管理，避免拖延！",
                "可能有些意外狀況！",
                "需要更多耐心和毅力！",
                "謹言慎行，避免衝突！",
                "健康方面要多加注意！",
                "財務上要保守一些！",
                "情緒管理很重要！",
                "做事要三思而後行！",
                "保持低調，避免麻煩！"
            ],
            "大凶": {
                "weight": 10,      # 10% 機率
                "color": 0xFF0000,  # 紅色
                "emoji": "💀",
                "title_suffix": " 🔥 挑戰考驗"
            }
        }
        
        fortune_texts = {
            "大吉": [
                "今日運勢爆棚！做什麼都順風順水！",
                "貴人相助，事半功倍的一天！",
                "財神眷顧，財運亨通！",
                "靈感湧現，創意無限！",
                "人際關係和諧，合作順利！",
                "健康狀況極佳，精力充沛！",
                "學習效率超高，吸收力強！",
                "愛情甜蜜，感情升溫！",
                "機會來敲門，好好把握！",
                "心想事成，美夢成真！",
                "事業突破，晉升有望！",
                "投資眼光精準，回報豐厚！",
                "團隊合作無間，戰績輝煌！",
                "幸運女神特別眷顧你！",
                "一切障礙都將迎刃而解！",
                "光芒四射，成為焦點人物！",
                "收穫滿滿，成果豐碩！",
                "正能量滿滿，影響他人！",
                "突破極限，創造奇蹟！",
                "天時地利人和，完美的一天！"
            ],
            "吉": [
                "運勢不錯，小有收穫！",
                "平穩順利，無風無浪！",
                "小小驚喜在前方等著你！",
                "人際關係融洽，心情愉快！",
                "工作學習進展順利！",
                "健康狀況良好，精神飽滿！",
                "財運平穩，小有進帳！",
                "感情穩定，溫馨甜蜜！",
                "有新的機會出現，值得嘗試！",
                "努力會有回報，繼續加油！",
                "遇到困難也能順利解決！",
                "團隊氣氛和諧，合作愉快！",
                "靈感來臨，創作順利！",
                "計劃順利推進，目標可期！",
                "得到他人幫助，心存感激！",
                "心情舒暢，正能量滿滿！",
                "小確幸不斷，幸福感提升！",
                "溝通順暢，誤會化解！",
                "身體健康，活力充沛！",
                "平靜安穩，享受當下！"
            ],
            "中平": [
                "保持平常心，平安就是福！",
                "平淡的一天，但無災無難！",
                "中庸之道，不偏不倚！",
                "按部就班，穩步前進！",
                "保持現狀，蓄勢待發！",
                "風平浪靜，適合反思規劃！",
                "不特別好也不特別壞！",
                "適合整理思緒，重新出發！",
                "平平淡淡才是真！",
                "維持現狀，等待時機！",
                "沒有驚喜也沒有驚嚇！",
                "適合休息充電的一天！",
                "保持平衡，避免極端！",
                "穩紮穩打，步步為營！",
                "中規中矩，平安度過！",
                "適合低調行事的一天！",
                "保持冷靜，理性思考！",
                "不求有功，但求無過！",
                "平平穩穩，順其自然！",
                "休息是為了走更長遠的路！"
            ],
            "凶": [
                "小心駛得萬年船，謹慎行事！",
                "可能遇到小麻煩，保持冷靜！",
                "注意健康，適當休息！",
                "財務上要小心謹慎！",
                "人際關係可能有些緊張！",
                "計劃可能遇到阻礙！",
                "情緒波動較大，需要調整！",
                "注意溝通，避免誤會！",
                "做事要更細心一些！",
                "可能會有小小的不順心！",
                "壓力較大，需要紓解！",
                "注意時間管理，避免拖延！",
                "可能有些意外狀況！",
                "需要更多耐心和毅力！",
                "謹言慎行，避免衝突！",
                "健康方面要多加注意！",
                "財務上要保守一些！",
                "情緒管理很重要！",
                "做事要三思而後行！",
                "保持低調，避免麻煩！"
            ],
            "大凶": [
                "挑戰重重，需要加倍努力！",
                "運勢不佳，凡事要小心！",
                "可能遇到較大困難！",
                "需要堅強的意志力！",
                "人際關係可能出現問題！",
                "健康要特別注意！",
                "財務上要非常謹慎！",
                "計劃可能全面受阻！",
                "情緒低落，需要支持！",
                "一切都不太順利！",
                "壓力山大，需要紓壓！",
                "可能會有意外的打擊！",
                "需要重新調整策略！",
                "合作關係可能出現裂痕！",
                "健康亮紅燈，要休息！",
                "財務危機可能出現！",
                "心情沉重，需要調適！",
                "做事阻礙重重！",
                "需要尋求他人幫助！",
                "考驗你的韌性和毅力！"
            ]
        }
        
        # ========== 阿爾比恩遊戲相關內容 ==========
        albion_activities = [
            "地城探索", "PvP戰鬥", "公會戰", "資源採集", "裝備製作",
            "市場交易", "領地爭奪", "世界BOSS", "競技場", "運輸貿易",
            "釣魚", "農業", "煉金術", "附魔", "寶石鑲嵌",
            "坐騎培養", "房屋建設", "公會任務", "懸賞任務", "探險任務"
        ]
        
        albion_zones = [
            "皇家大陸", "黑區", "紅區", "藍區", "黃區",
            "阿瓦隆路", "地獄門", "腐化地牢", "懸崖邊境", "迷霧",
            "雪地", "沙漠", "森林", "沼澤", "山脈",
            "海岸線", "地下城", "城堡遺跡", "神廟廢墟", "龍之巢穴"
        ]
        
        albion_weapons = [
            "單手劍", "雙手劍", "戰斧", "戰鎚", "長矛",
            "匕首", "魔杖", "法杖", "弓", "十字弓",
            "神聖杖", "自然法杖", "咒術法杖", "冰霜法杖", "火焰法杖",
            "電擊法杖", "詛咒法杖", "治療法杖", "坦克盾", "輔助法器"
        ]
        
        albion_armor = [
            "布甲", "皮甲", "板甲", "法袍", "戰袍",
            "斗篷", "頭盔", "護肩", "護手", "護腿",
            "靴子", "腰帶", "項鍊", "戒指", "耳環"
        ]
        
        albion_mounts = [
            "戰馬", "迅猛龍", "狼", "熊", "野豬",
            "蠍子", "蜘蛛", "渡鴉", "獅鷲", "飛龍",
            "獨角獸", "駱駝", "大象", "犀牛", "劍齒虎"
        ]
        
        albion_resources = [
            "原木", "石頭", "礦石", "纖維", "皮革",
            "布料", "藥草", "魚類", "穀物", "水果",
            "肉類", "寶石", "水晶", "符文", "魂石"
        ]
        
        # ========== 職業相關建議 ==========
        profession_advice = {
            "坦克": [
                "今天你的嘲諷特別有效，敵人會優先攻擊你！",
                "防禦時機特別準確，會成為團隊的中流砥柱！",
                "站位選擇極佳，能完美保護隊友！",
                "格擋成功率大幅提升！",
                "吸收傷害的能力特別強！",
                "控場技能效果加倍！",
                "仇恨值管理得心應手！",
                "保護隊友的意識特別敏銳！",
                "生存能力大幅提升！",
                "成為團隊的堅實盾牌！"
            ],
            "输出": [
                "暴擊率大幅提升，傷害爆表！",
                "技能連招特別流暢！",
                "走位靈活，難以被擊中！",
                "攻擊速度明顯加快！",
                "技能冷卻時間縮短！",
                "命中率極高，招招致命！",
                "能量管理得心應手！",
                "爆發時機掌握完美！",
                "輸出循環特別順暢！",
                "成為團隊的主要傷害來源！"
            ],
            "治疗": [
                "治療效果大幅提升！",
                "治療範圍明顯擴大！",
                "法力消耗減少！",
                "治療時機掌握完美！",
                "群體治療效果極佳！",
                "復活成功率大幅提升！",
                "淨化技能效果加倍！",
                "治療之泉效果持久！",
                "護盾強度大幅提升！",
                "成為團隊的生命線！"
            ],
            "辅助": [
                "增益效果持續時間延長！",
                "減益效果威力加強！",
                "團隊buff覆蓋率提高！",
                "控場技能效果顯著！",
                "移動速度加成明顯！",
                "資源恢復速度加快！",
                "技能冷卻減免效果加倍！",
                "團隊協同能力提升！",
                "戰術指揮特別有效！",
                "成為團隊的靈魂人物！"
            ]
        }
        
        # ========== 選擇運程等級 ==========
        levels = list(fortune_levels.keys())
        weights = [fortune_levels[level]["weight"] for level in levels]
        selected_level = random.choices(levels, weights=weights, k=1)[0]
        level_info = fortune_levels[selected_level]
        
        # ========== 選擇祝福語 ==========
        selected_text = random.choice(fortune_texts[selected_level])
        
        # ========== 生成詳細內容 ==========
        # 幸運度（1-100）
        if selected_level == "大吉":
            luck_score = random.randint(85, 100)
        elif selected_level == "吉":
            luck_score = random.randint(70, 84)
        elif selected_level == "中平":
            luck_score = random.randint(50, 69)
        elif selected_level == "凶":
            luck_score = random.randint(30, 49)
        else:  # 大凶
            luck_score = random.randint(1, 29)
        
        # 時段運勢
        time_fortunes = ["大吉", "吉", "中平", "凶", "大凶"]
        time_weights = {
            "大吉": [40, 30, 20, 7, 3],
            "吉": [20, 40, 25, 10, 5],
            "中平": [10, 20, 40, 20, 10],
            "凶": [5, 15, 25, 40, 15],
            "大凶": [3, 7, 15, 25, 50]
        }
        
        morning = random.choices(time_fortunes, weights=time_weights[selected_level], k=1)[0]
        afternoon = random.choices(time_fortunes, weights=time_weights[selected_level], k=1)[0]
        evening = random.choices(time_fortunes, weights=time_weights[selected_level], k=1)[0]
        
        # 幸運顏色
        lucky_colors = ["紅色", "藍色", "綠色", "金色", "紫色", "白色", "黑色", "黃色", "橙色", "粉色"]
        lucky_color = random.choice(lucky_colors)
        
        # 幸運物品
        lucky_items = random.sample(albion_resources + albion_armor + albion_weapons, 3)
        
        # 阿爾比恩活動建議
        recommended_activity = random.choice(albion_activities)
        recommended_zone = random.choice(albion_zones)
        recommended_weapon = random.choice(albion_weapons)
        recommended_mount = random.choice(albion_mounts)
        
        # 生成幸運數字
        lucky_numbers = sorted(random.sample(range(1, 101), 3))
        
        # 幸運方向
        directions = ["東", "南", "西", "北", "東北", "東南", "西北", "西南"]
        lucky_direction = random.choice(directions)
        
        # 隨機選擇一個職業建議
        random_profession = random.choice(list(profession_advice.keys()))
        profession_tip = random.choice(profession_advice[random_profession])
        
        # 愛情運勢
        love_fortunes = [
            "單身者可能遇到心儀對象！",
            "感情穩定發展，甜蜜升級！",
            "多溝通少猜疑，感情更融洽！",
            "適合浪漫約會，增進感情！",
            "愛情運平穩，保持現狀！",
            "需要多花時間陪伴另一半！",
            "可能有些小誤會，及時溝通！",
            "感情需要更多耐心！",
            "單身者桃花運不錯！",
            "感情面臨考驗，需要堅持！"
        ]
        love_fortune = random.choice(love_fortunes)
        
        # 財運指南
        money_fortunes = [
            "財運亨通，投資有好回報！",
            "收支平衡，穩健為上！",
            "可能有意外之財！",
            "財務狀況良好！",
            "保守理財，避免風險！",
            "有小額進帳！",
            "注意開支控制！",
            "投資需謹慎！",
            "財運平穩！",
            "可能有些財務壓力！"
        ]
        money_fortune = random.choice(money_fortunes)
        
        # 健康建議
        health_tips = [
            "精力充沛，適合運動！",
            "注意休息，避免過勞！",
            "健康狀況良好！",
            "需要多補充水分！",
            "適合進行健康檢查！",
            "注意飲食均衡！",
            "適度運動有益健康！",
            "保持良好作息！",
            "注意身體小狀況！",
            "精神飽滿，狀態極佳！"
        ]
        health_tip = random.choice(health_tips)
        
        # 生活建議
        life_advices = [
            "保持心情平靜最重要！",
            "多與家人朋友交流！",
            "避免衝動消費或決定！",
            "適合學習新知識！",
            "給自己一些獨處時間！",
            "嘗試新事物！",
            "整理環境，煥然一新！",
            "幫助他人會有好運！",
            "保持樂觀心態！",
            "珍惜當下時光！"
        ]
        # 隨機選擇3個生活建議
        selected_advices = random.sample(life_advices, 3)
        
        # 趣味統計
        positive_energy = random.randint(50, 100)
        surprise_chance = random.randint(20, 80)
        
        # 生成統計圖標
        luck_bar = create_progress_bar(luck_score, 20)
        energy_bar = create_progress_bar(positive_energy, 20)
        
        # ========== 創建Embed ==========
        embed = discord.Embed(
            title=f"{level_info['emoji']} {interaction.user.display_name} 的今日運程 {level_info['title_suffix']}",
            description=f"**{selected_text}**\n\n{level_info['emoji'] * (luck_score // 20)}",
            color=level_info['color']
        )
        
        # 運程分析
        embed.add_field(
            name="📊 運程分析",
            value=(
                f"**運程等級：** {selected_level}\n"
                f"**出現機率：** {level_info['weight']}%\n"
                f"**幸運顏色：** {lucky_color}\n"
                f"**幸運物品：** {', '.join(lucky_items)}"
            ),
            inline=True
        )
        
        # 時段運勢
        embed.add_field(
            name="🕰️ 時段運勢",
            value=(
                f"**上午：** {morning}\n"
                f"**下午：** {afternoon}\n"
                f"**晚上：** {evening}"
            ),
            inline=True
        )
        
        # 幸運指引
        embed.add_field(
            name="🎲 幸運指引",
            value=(
                f"**幸運數字：** {', '.join(map(str, lucky_numbers))}\n"
                f"**幸運方向：** {lucky_direction}"
            ),
            inline=False
        )
        
        # 阿爾比恩專屬建議
        embed.add_field(
            name="🎮 阿爾比恩冒險指南",
            value=(
                f"**推薦活動：** {recommended_activity}\n"
                f"**最佳區域：** {recommended_zone}\n"
                f"**趁手武器：** {recommended_weapon}\n"
                f"**推薦坐騎：** {recommended_mount}"
            ),
            inline=False
        )
        
        # 職業建議（隨機一個職業）
        embed.add_field(
            name=f"⚔️ {random_profession}專屬建議",
            value=profession_tip,
            inline=False
        )
        
        # 今日生活建議
        embed.add_field(
            name="💡 今日生活建議",
            value="\n".join([f"• {advice}" for advice in selected_advices]),
            inline=False
        )
        
        # 愛情運勢
        embed.add_field(
            name="💖 愛情運勢",
            value=love_fortune,
            inline=True
        )
        
        # 財運指南
        embed.add_field(
            name="💰 財運指南",
            value=money_fortune,
            inline=True
        )
        
        # 健康建議
        embed.add_field(
            name="🏥 健康建議",
            value=health_tip,
            inline=True
        )
        
        # 今日趣味統計
        embed.add_field(
            name="📈 今日趣味統計",
            value=(
                f"**今日幸運指數：** {luck_bar} {luck_score}%\n"
                f"**正能量指數：** {energy_bar} {positive_energy}%\n"
                f"**驚喜機率：** {surprise_chance}%"
            ),
            inline=False
        )
        
        # 名言警句
        daily_quotes = [
            "堅持就是最好的運氣。",
            "幸運女神眷顧有準備的人。",
            "每一天都是新的開始。",
            "保持微笑，好運自然來。",
            "勇敢面對挑戰，運氣就在轉角。",
            "心態決定運氣的高度。",
            "堅持不懈，終有回報。",
            "好運總是偏愛努力的人。",
            "保持善良，運氣會更好。",
            "今天是你改變命運的機會。"
        ]
        
        embed.add_field(
            name="💝 今日名言",
            value=f"「{random.choice(daily_quotes)}」",
            inline=False
        )
        
        embed.set_footer(
            text=f"阿爾比恩運程系統 • {datetime.now().strftime('%Y年%m月%d日 %H:%M')} • 僅供娛樂參考"
        )
        
        if interaction.user.avatar:
            embed.set_thumbnail(url=interaction.user.avatar.url)
        
        await interaction.followup.send(embed=embed)
        
        await log_query("blessing", interaction.user.id, {"fortune_level": selected_level}, get_guild_id(interaction))
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 運程測試失敗",
            description=f"錯誤：{str(e)[:100]}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

# ========== 管理員指令 (4個) ==========

@tree.command(name="add_prize", description="新增獎品到彩池（可增減數量）")
@app_commands.describe(
    prize_name="獎品名稱",
    box_level="獎品箱子類型",
    quantity="數量（可為負數來減少）"
)
@app_commands.choices(box_level=[
    app_commands.Choice(name="綠箱", value="綠箱"),
    app_commands.Choice(name="藍箱", value="藍箱"),
    app_commands.Choice(name="紫箱", value="紫箱"),
    app_commands.Choice(name="金箱", value="金箱")
])
async def add_prize_slash(
    interaction: discord.Interaction,
    prize_name: str,
    box_level: str,
    quantity: int
):
    """新增/減少獎品"""
    await interaction.response.defer()
    
    try:
        if interaction.user.id not in OWNER_IDS:
            await interaction.followup.send("❌ 只有機器人擁有者可以使用此指令！")
            return
        
        if not db.is_connected:
            await interaction.followup.send("❌ 資料庫未連接，無法操作彩池！")
            return
        
        if quantity == 0:
            await interaction.followup.send("❌ 數量不能為0！")
            return
        
        guild_id = get_guild_id(interaction)
        
        await log_query("add_prize", interaction.user.id, {"prize_name": prize_name, "box_level": box_level, "quantity": quantity}, guild_id)
        
        existing = await db.fetchrow(
            "SELECT id, quantity, remaining FROM prize_pool WHERE prize_name = $1 AND box_level = $2 AND guild_id = $3",
            prize_name, box_level, guild_id
        )
        
        if existing:
            new_quantity = existing['quantity'] + quantity
            new_remaining = existing['remaining'] + quantity
            
            if new_remaining < 0:
                await interaction.followup.send(f"❌ 不能減少超過現有數量！目前剩餘：{existing['remaining']}個")
                return
            
            await db.execute(
                "UPDATE prize_pool SET quantity = $1, remaining = $2, added_at = NOW() WHERE id = $3",
                new_quantity, new_remaining, existing['id']
            )
            
            action = "增加" if quantity > 0 else "減少"
            embed = discord.Embed(
                title="✅ 獎品數量已更新",
                color=0x00FF00 if quantity > 0 else 0xFFA500
            )
        else:
            if quantity < 0:
                await interaction.followup.send("❌ 不能創建數量為負數的獎品！")
                return
            
            await db.execute(
                """
                INSERT INTO prize_pool (prize_name, box_level, quantity, remaining, added_by, guild_id)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                prize_name, box_level, quantity, quantity, interaction.user.id, guild_id
            )
            
            action = "新增"
            embed = discord.Embed(
                title="✅ 獎品已新增",
                color=0x00FF00
            )
        
        box_colors = {
            "綠箱": 0x00FF00,
            "藍箱": 0x0000FF,
            "紫箱": 0x800080,
            "金箱": 0xFFD700
        }
        
        embed.color = box_colors.get(box_level, 0x7289DA)
        embed.add_field(name="🎁 獎品名稱", value=prize_name, inline=True)
        embed.add_field(name="📦 箱子類型", value=box_level, inline=True)
        embed.add_field(name="📊 數量變化", value=f"{'+' if quantity > 0 else ''}{quantity} 個", inline=True)
        
        if existing:
            embed.add_field(name="📈 目前總數", value=f"{new_quantity} 個", inline=True)
            embed.add_field(name="📉 目前剩餘", value=f"{new_remaining} 個", inline=True)
        
        embed.set_footer(text=f"操作人: {interaction.user.name} | 時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        await interaction.followup.send(f"❌ 操作失敗：{str(e)}")

@tree.command(name="add_score", description="增加/減少用戶積分")
@app_commands.describe(
    user="目標用戶",
    amount="積分數量（可為負數）",
    reason="原因（可選）"
)
async def add_score_slash(
    interaction: discord.Interaction,
    user: discord.Member,
    amount: int,
    reason: str = ""
):
    """增加/減少積分"""
    await interaction.response.defer()
    
    try:
        if interaction.user.id not in OWNER_IDS:
            await interaction.followup.send("❌ 只有機器人擁有者可以使用此指令！")
            return
        
        if user.bot:
            await interaction.followup.send("❌ 不能操作機器人的積分！")
            return
        
        guild_id = get_guild_id(interaction)
        
        current_score, total_score = await get_user_score(user.id, guild_id)
        
        if amount < 0 and current_score + amount < 0:
            await interaction.followup.send(f"❌ 不能將積分減少到負數！目前積分：{current_score}，操作後：{current_score + amount}")
            return
        
        reason_text = reason if reason else f"管理員 {interaction.user.name} 操作"
        
        await update_user_score(user.id, user.name, amount, reason_text, guild_id)
        
        new_score, new_total = await get_user_score(user.id, guild_id)
        
        embed = discord.Embed(
            title="✅ 積分操作成功",
            color=0x00FF00 if amount >= 0 else 0xFFA500
        )
        
        embed.add_field(name="👤 目標用戶", value=f"{user.mention} ({user.name})", inline=True)
        embed.add_field(name="📊 積分變化", value=f"{'+' if amount > 0 else ''}{amount} 分", inline=True)
        embed.add_field(name="💰 目前積分", value=f"{new_score} 分", inline=True)
        embed.add_field(name="📈 總獲得積分", value=f"{new_total} 分", inline=True)
        embed.add_field(name="📝 操作原因", value=reason_text, inline=False)
        embed.add_field(name="🛠️ 操作人員", value=interaction.user.mention, inline=True)
        
        embed.set_footer(text=f"操作時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        await interaction.followup.send(embed=embed)
        
        await log_query("add_score", interaction.user.id, {"target_user": user.id, "amount": amount, "reason": reason}, guild_id)
        
    except Exception as e:
        await interaction.followup.send(f"❌ 操作失敗：{str(e)}")

@tree.command(name="create_event", description="創建評核活動")
@app_commands.describe(
    event_name="活動名稱",
    duration="簽到持續時間（例如：30m, 1h, 2h）",
    signup_message="簽到訊息（可選）"
)
async def create_event_slash(
    interaction: discord.Interaction,
    event_name: str,
    duration: str = "1h",
    signup_message: str = None
):
    """創建評核活動"""
    await interaction.response.defer()
    
    try:
        if interaction.user.id not in OWNER_IDS:
            await interaction.followup.send("❌ 只有機器人擁有者可以使用此指令！")
            return
        
        if not db.is_connected:
            await interaction.followup.send("❌ 資料庫未連接，無法創建活動！")
            return
        
        guild_id = get_guild_id(interaction)
        
        duration_lower = duration.lower().strip()
        seconds = 3600
        
        if duration_lower.endswith('s'):
            seconds = int(duration_lower[:-1])
        elif duration_lower.endswith('m'):
            seconds = int(duration_lower[:-1]) * 60
        elif duration_lower.endswith('h'):
            seconds = int(duration_lower[:-1]) * 3600
        elif duration_lower.isdigit():
            seconds = int(duration_lower)
        
        if seconds < 300:
            await interaction.followup.send("❌ 簽到時間必須至少5分鐘！")
            return
        
        if seconds > 86400:
            await interaction.followup.send("❌ 簽到時間不能超過24小時！")
            return
        
        end_time = datetime.now() + timedelta(seconds=seconds)
        
        embed = discord.Embed(
            title=f"📋 評核活動：{event_name}",
            description=signup_message or "請點擊 ✅ 簽到參加本次評核活動",
            color=0x7289DA
        )
        
        if seconds < 60:
            time_display = f"{seconds}秒"
        elif seconds < 3600:
            time_display = f"{seconds//60}分鐘"
        else:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            time_display = f"{hours}小時{minutes}分鐘" if minutes > 0 else f"{hours}小時"
        
        embed.add_field(name="⏰ 簽到截止", value=f"{time_display}內", inline=True)
        embed.add_field(name="👥 已簽到人數", value="0 人", inline=True)
        embed.add_field(name="📝 活動說明", value="簽到後請選擇職業，然後等待評核", inline=False)
        
        embed.set_footer(text=f"活動創建者: {interaction.user.name}")
        
        await interaction.followup.send(embed=embed)
        message = await interaction.original_response()
        
        await message.add_reaction("✅")
        
        event_id = await db.fetchval(
            """
            INSERT INTO evaluation_events (
                event_name, creator_id, signup_message_id, channel_id, 
                signup_end_time, guild_id
            ) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id
            """,
            event_name, interaction.user.id, message.id, interaction.channel.id,
            end_time, guild_id
        )
        
        await log_query("create_event", interaction.user.id, {"event_name": event_name, "duration": duration}, guild_id)
        
        print(f"✅ 評核活動已創建: {event_name} (ID: {event_id}), 簽到時間: {seconds}秒")
        
        async def signup_phase():
            remaining = seconds
            last_update = time.time()
            
            while remaining > 0:
                await asyncio.sleep(1)
                remaining -= 1
                
                if time.time() - last_update >= 30:
                    result = await db.fetchrow(
                        "SELECT participants FROM evaluation_events WHERE id = $1",
                        event_id
                    )
                    
                    if result and result['participants']:
                        participants = result['participants']
                        if isinstance(participants, str):
                            try:
                                participants = json.loads(participants)
                            except:
                                participants = []
                        participants_count = len(participants)
                        
                        new_embed = discord.Embed(
                            title=f"📋 評核活動：{event_name}",
                            description=signup_message or "請點擊 ✅ 簽到參加本次評核活動",
                            color=0x7289DA
                        )
                        
                        if remaining < 60:
                            time_display = f"{remaining}秒"
                        elif remaining < 3600:
                            time_display = f"{remaining//60}分鐘"
                        else:
                            hours = remaining // 3600
                            minutes = (remaining % 3600) // 60
                            time_display = f"{hours}小時{minutes}分鐘" if minutes > 0 else f"{hours}小時"
                        
                        new_embed.add_field(name="⏰ 簽到截止", value=f"{time_display}內", inline=True)
                        new_embed.add_field(name="👥 已簽到人數", value=f"{participants_count} 人", inline=True)
                        new_embed.add_field(name="📝 活動說明", value="簽到後請選擇職業，然後等待評核", inline=False)
                        
                        new_embed.set_footer(text=f"活動創建者: {interaction.user.name}")
                        
                        await message.edit(embed=new_embed)
                        last_update = time.time()
            
            await message.clear_reactions()
            
            result = await db.fetchrow(
                "SELECT participants FROM evaluation_events WHERE id = $1",
                event_id
            )
            
            participants_count = 0
            if result and result['participants']:
                participants = result['participants']
                if isinstance(participants, str):
                    try:
                        participants = json.loads(participants)
                    except:
                        participants = []
                participants_count = len(participants)
            
            if participants_count == 0:
                await message.channel.send(f"❌ 活動 **{event_name}** 無人參加，已取消。")
                await db.execute(
                    "UPDATE evaluation_events SET is_active = false WHERE id = $1",
                    event_id
                )
                return
            
            profession_embed = discord.Embed(
                title=f"🎮 職業選擇：{event_name}",
                description=f"共有 {participants_count} 人參加本次活動\n\n請選擇你的職業：",
                color=0x43B581
            )
            
            for emoji, profession in PROFESSION_EMOJIS.items():
                profession_embed.add_field(
                    name=f"{emoji} {profession}",
                    value=f"點擊 {emoji} 選擇此職業",
                    inline=True
                )
            
            profession_embed.add_field(
                name="📊 職業加成",
                value="選擇特定職業可獲得積分加成",
                inline=False
            )
            
            for profession, bonus in PROFESSION_BONUS.items():
                if bonus > 0:
                    profession_embed.add_field(
                        name=f"✨ {profession}",
                        value=f"+{bonus} 積分",
                        inline=True
                    )
            
            profession_embed.set_footer(text="選擇職業後，等待下一步評核")
            
            profession_message = await message.channel.send(embed=profession_embed)
            
            for emoji in PROFESSION_EMOJIS.keys():
                await profession_message.add_reaction(emoji)
            
            await db.execute(
                "UPDATE evaluation_events SET profession_message_id = $1 WHERE id = $2",
                profession_message.id, event_id
            )
            
            await asyncio.sleep(300)
            
            await profession_message.clear_reactions()
            
            event_data = await db.fetchrow(
                "SELECT participants, professions FROM evaluation_events WHERE id = $1",
                event_id
            )
            
            if event_data:
                participants = event_data['participants'] or []
                professions = event_data['professions'] or {}
                
                if isinstance(participants, str):
                    try:
                        participants = json.loads(participants)
                    except:
                        participants = []
                
                if isinstance(professions, str):
                    try:
                        professions = json.loads(professions)
                    except:
                        professions = {}
                
                for user_id in participants:
                    if str(user_id) not in professions:
                        professions[str(user_id)] = "未選擇"
                
                await db.execute(
                    "UPDATE evaluation_events SET professions = $1 WHERE id = $2",
                    json.dumps(professions), event_id
                )
            
            rating_embed = discord.Embed(
                title=f"⭐ 評核階段：{event_name}",
                description="請對參加者進行評核：",
                color=0xFFD700
            )
            
            rating_embed.add_field(
                name="📊 評分標準",
                value="點擊對應的emoji進行評分",
                inline=False
            )
            
            for emoji, rating in RATING_EMOJIS.items():
                score = RATING_SCORES.get(rating, 0)
                score_text = f"+{score}分" if score > 0 else f"{score}分"
                rating_embed.add_field(
                    name=f"{emoji} {rating}",
                    value=f"{score_text} | 點擊 {emoji} 選擇此評級",
                    inline=True
                )
            
            rating_embed.add_field(
                name="🏁 完成評核",
                value=f"點擊 {RATING_END_EMOJI} 結束評核階段",
                inline=False
            )
            
            rating_message = await message.channel.send(embed=rating_embed)
            
            for emoji in list(RATING_EMOJIS.keys()) + [RATING_END_EMOJI]:
                await rating_message.add_reaction(emoji)
            
            await db.execute(
                "UPDATE evaluation_events SET rating_message_id = $1 WHERE id = $2",
                rating_message.id, event_id
            )
            
            await asyncio.sleep(600)
            
            await rating_message.clear_reactions()
            
            await end_evaluation(event_id, message.channel, event_name, guild_id)
        
        asyncio.create_task(signup_phase())
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 創建活動失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

@tree.command(name="activity_stats", description="查看活動統計")
async def activity_stats_slash(interaction: discord.Interaction):
    """活動統計"""
    await interaction.response.defer()
    
    try:
        if interaction.user.id not in OWNER_IDS:
            await interaction.followup.send("❌ 只有機器人擁有者可以使用此指令！")
            return
        
        guild_id = get_guild_id(interaction)
        
        if not db.is_connected:
            embed = discord.Embed(
                title="⚠️ 資料庫未連接",
                description="無法讀取活動統計，請稍後再試。",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        await log_query("activity_stats", interaction.user.id, {}, guild_id)
        
        users = await db.fetch(
            "SELECT user_id, username, activity_stats FROM users WHERE guild_id = $1",
            guild_id
        )
        
        current_period = get_current_half_month()
        
        period_stats = {}
        all_time_stats = {}
        
        for row in users:
            user_id = row['user_id']
            username = row['username']
            activity_stats = row['activity_stats'] or {}
            
            if isinstance(activity_stats, str):
                try:
                    activity_stats = json.loads(activity_stats)
                except:
                    activity_stats = {}
            
            if current_period in activity_stats:
                period_stats[user_id] = {
                    "username": username,
                    "total": activity_stats[current_period].get("total", 0),
                    "attended": activity_stats[current_period].get("attended", 0)
                }
            
            all_time_attended = 0
            all_time_total = 0
            for period_key, data in activity_stats.items():
                if isinstance(data, dict):
                    all_time_total += data.get("total", 0)
                    all_time_attended += data.get("attended", 0)
            
            all_time_stats[user_id] = {
                "username": username,
                "total": all_time_total,
                "attended": all_time_attended
            }
        
        period_attendance = [
            {
                "user_id": user_id,
                "username": data["username"],
                "attendance_rate": (data["attended"] / data["total"] * 100) if data["total"] > 0 else 0,
                "attended": data["attended"],
                "total": data["total"]
            }
            for user_id, data in period_stats.items()
        ]
        
        all_time_attendance = [
            {
                "user_id": user_id,
                "username": data["username"],
                "attendance_rate": (data["attended"] / data["total"] * 100) if data["total"] > 0 else 0,
                "attended": data["attended"],
                "total": data["total"]
            }
            for user_id, data in all_time_stats.items() if data["total"] > 0
        ]
        
        period_attendance.sort(key=lambda x: (-x["attendance_rate"], -x["attended"]))
        all_time_attendance.sort(key=lambda x: (-x["attendance_rate"], -x["attended"]))
        
        embed = discord.Embed(
            title="📊 活動統計",
            description="所有用戶的活動參與情況",
            color=0x7289DA
        )
        
        total_events_current = await get_total_events_in_period(guild_id, "current")
        total_events_all = await get_total_events_in_period(guild_id, "all")
        
        embed.add_field(
            name="📅 統計概覽",
            value=(
                f"**當前半月期：** {current_period}\n"
                f"**本期活動數：** {total_events_current}\n"
                f"**總活動數：** {total_events_all}\n"
                f"**統計用戶數：** {len(users)}人"
            ),
            inline=False
        )
        
        if period_attendance:
            top_period = period_attendance[:3]
            period_text = ""
            for i, data in enumerate(top_period, 1):
                bar = create_progress_bar(data["attendance_rate"], 10)
                period_text += (
                    f"**第{i}名：** {data['username']}\n"
                    f"{bar} {data['attendance_rate']:.1f}%\n"
                    f"({data['attended']}/{data['total']}次)\n\n"
                )
            embed.add_field(
                name=f"🏆 {current_period} 出席率 TOP 3",
                value=period_text,
                inline=True
            )
        else:
            embed.add_field(
                name=f"🏆 {current_period} 出席率",
                value="尚無數據",
                inline=True
            )
        
        if all_time_attendance:
            top_all = all_time_attendance[:3]
            all_text = ""
            for i, data in enumerate(top_all, 1):
                bar = create_progress_bar(data["attendance_rate"], 10)
                all_text += (
                    f"**第{i}名：** {data['username']}\n"
                    f"{bar} {data['attendance_rate']:.1f}%\n"
                    f"({data['attended']}/{data['total']}次)\n\n"
                )
            embed.add_field(
                name="🏆 歷史總出席率 TOP 3",
                value=all_text,
                inline=True
            )
        else:
            embed.add_field(
                name="🏆 歷史總出席率",
                value="尚無數據",
                inline=True
            )
        
        user_data = None
        for data in period_attendance:
            if data["user_id"] == interaction.user.id:
                user_data = data
                break
        
        if user_data:
            user_bar = create_progress_bar(user_data["attendance_rate"], 15)
            embed.add_field(
                name="📊 你的數據（本期）",
                value=(
                    f"{user_bar}\n"
                    f"**出席率：** {user_data['attendance_rate']:.1f}%\n"
                    f"**出席次數：** {user_data['attended']}/{user_data['total']}\n"
                    f"**總活動數：** {total_events_current}"
                ),
                inline=False
            )
        
        embed.set_footer(text=f"統計時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 讀取統計失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

# ========== RPG 指令 ==========

@tree.command(name="rpg_start", description="開始 RPG 冒險")
@app_commands.describe(nickname="角色暱稱（可選）")
async def rpg_start_slash(interaction: discord.Interaction, nickname: str = None):
    """開始 RPG 冒險"""
    await interaction.response.defer()
    
    try:
        if not db.is_connected:
            embed = discord.Embed(
                title="⚠️ 資料庫未連接",
                description="無法開始 RPG 冒險，請稍後再試。",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        success = await create_rpg_player(interaction.user.id, nickname)
        
        if success:
            embed = discord.Embed(
                title="🎮 RPG 冒險開始！",
                description="歡迎來到阿爾比恩大陸！",
                color=0x00FF00
            )
            
            embed.add_field(
                name="👤 角色資訊",
                value=(
                    f"**冒險者：** {nickname or interaction.user.name}\n"
                    f"**初始等級：** 1\n"
                    f"**初始HP：** 100\n"
                    f"**初始MP：** 50\n"
                    f"**所屬：** 小雲孤兒院"
                ),
                inline=False
            )
            
            embed.add_field(
                name="🎒 初始裝備",
                value=(
                    "• 木劍（武器）\n"
                    "• 布衣（身體）\n"
                    "• 草鞋（鞋子）\n"
                    "• 小紅藥水 x1\n"
                    "• 小藍藥水 x1"
                ),
                inline=True
            )
            
            embed.add_field(
                name="🎯 屬性點系統",
                value=(
                    f"**每級獲得：** {RPG_CONFIG['STAT_POINTS_PER_LEVEL']} 點\n"
                    "**可分配屬性：**\n"
                    "• 體力（❤️）\n"
                    "• 速度（⚡）\n"
                    "• 力量（💪）\n"
                    "• 智慧（🧠）\n"
                    "• 負重（🎒）"
                ),
                inline=True
            )
            
            embed.set_footer(text="使用 /rpg_status 查看角色狀態 | /rpg_help 查看所有指令")
            
            if interaction.user.avatar:
                embed.set_thumbnail(url=interaction.user.avatar.url)
            
            await interaction.followup.send(embed=embed)
        else:
            embed = discord.Embed(
                title="❌ 創建角色失敗",
                description="可能已經有角色存在，或資料庫錯誤。",
                color=0xFF0000
            )
            await interaction.followup.send(embed=embed)
            
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ RPG 創建失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

@tree.command(name="rpg_status", description="查看角色狀態")
async def rpg_status_slash(interaction: discord.Interaction):
    """查看角色狀態"""
    await interaction.response.defer()
    
    try:
        if not db.is_connected:
            embed = discord.Embed(
                title="⚠️ 資料庫未連接",
                description="無法讀取角色狀態，請稍後再試。",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        player = await get_rpg_player(interaction.user.id)
        
        if not player:
            embed = discord.Embed(
                title="❌ 角色不存在",
                description="請先使用 `/rpg_start` 創建角色",
                color=0xFF0000
            )
            await interaction.followup.send(embed=embed)
            return
        
        embed = discord.Embed(
            title=f"📊 {player['nickname']} 的狀態",
            color=0x7289DA
        )
        
        # 基本信息
        embed.add_field(
            name="👤 基本信息",
            value=(
                f"**等級：** {player['level']} 📊\n"
                f"**經驗：** {player['exp']}/{player['max_exp']} ⭐\n"
                f"**下一級：** {player['exp_to_next'] - player['exp']} ⭐\n"
                f"**戰鬥力：** {player['combat_power']} ⚔️"
            ),
            inline=False
        )
        
        # 狀態條
        hp_percent = (player['current_hp'] / player['max_hp']) * 100
        mp_percent = (player['current_mp'] / player['max_mp']) * 100
        
        hp_bar = create_progress_bar(hp_percent, 15)
        mp_bar = create_progress_bar(mp_percent, 15)
        
        embed.add_field(
            name="❤️‍🩹 狀態",
            value=(
                f"**HP：** {hp_bar}\n"
                f"`{player['current_hp']}/{player['max_hp']}`\n\n"
                f"**MP：** {mp_bar}\n"
                f"`{player['current_mp']}/{player['max_mp']}`"
            ),
            inline=True
        )
        
        # 屬性
        embed.add_field(
            name="📈 屬性",
            value=(
                f"**體力：** {player['vitality']} ❤️\n"
                f"**速度：** {player['speed']} ⚡\n"
                f"**力量：** {player['strength']} 💪\n"
                f"**智慧：** {player['intelligence']} 🧠\n"
                f"**負重：** {player['carrying_capacity']} 🎒\n"
                f"**剩餘點數：** {player['remaining_stat_points']} ✨"
            ),
            inline=True
        )
        
        # 裝備
        equipment = []
        slots = {
            "武器": player['weapon_name'],
            "頭部": player['head_name'],
            "身體": player['body_name'],
            "鞋子": player['shoes_name'],
            "項鍊": player['necklace_name'],
            "戒指": player['ring_name'],
            "背包": player['backpack_name']
        }
        
        for slot, item_name in slots.items():
            if item_name:
                equipment.append(f"• {slot}：{item_name}")
            else:
                equipment.append(f"• {slot}：無")
        
        embed.add_field(
            name="⚔️ 裝備",
            value="\n".join(equipment),
            inline=False
        )
        
        # 位置與房屋
        embed.add_field(
            name="📍 位置",
            value=(
                f"**所在地圖：** {player['current_map']}\n"
                f"**當前層數：** {player['current_layer']}\n"
                f"**是否在城鎮：** {'✅' if player['is_in_town'] else '❌'}"
            ),
            inline=True
        )
        
        house_info = RPG_CONFIG['HOUSES'].get(player['house_type'], {})
        embed.add_field(
            name="🏠 房屋",
            value=(
                f"**房屋類型：** {house_info.get('name', '未知')}\n"
                f"**倉庫容量：** {player['storage_capacity']}格"
            ),
            inline=True
        )
        
        # 統計
        embed.add_field(
            name="📊 統計",
            value=(
                f"**擊敗怪物：** {player['monsters_killed']}隻\n"
                f"**死亡次數：** {player['deaths']}次\n"
                f"**總傷害：** {player['total_damage']}\n"
                f"**總治療：** {player['total_healing']}"
            ),
            inline=False
        )
        
        embed.set_footer(text=f"最後活動: {player['last_active'].strftime('%Y-%m-%d %H:%M:%S')}")
        
        if interaction.user.avatar:
            embed.set_thumbnail(url=interaction.user.avatar.url)
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 讀取角色狀態失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

@tree.command(name="rpg_help", description="顯示 RPG 幫助訊息")
async def rpg_help_slash(interaction: discord.Interaction):
    """RPG 幫助"""
    embed = discord.Embed(
        title="🎮 RPG 系統幫助",
        description="阿爾比恩大陸冒險指南",
        color=0x7289DA
    )
    
    embed.add_field(
        name="🎯 核心指令",
        value=(
            "`/rpg_start` - 開始冒險\n"
            "`/rpg_status` - 角色狀態\n"
            "`/rpg_help` - 顯示此訊息"
        ),
        inline=False
    )
    
    embed.add_field(
        name="🛒 背包與裝備",
        value=(
            "`/rpg_inventory` - 查看背包 **(開發中)**\n"
            "`/rpg_equip` - 裝備管理 **(開發中)**\n"
            "`/rpg_shop` - 商店系統 **(開發中)**"
        ),
        inline=False
    )
    
    embed.add_field(
        name="🏃‍♂️ 探索與戰鬥",
        value=(
            "`/rpg_explore` - 開始冒險 **(開發中)**\n"
            "`/rpg_party` - 組隊系統 **(開發中)**\n"
            "`/rpg_battle` - 戰鬥系統 **(開發中)**"
        ),
        inline=False
    )
    
    embed.add_field(
        name="⚒️ 生產與製作",
        value=(
            "`/rpg_craft` - 製作物品 **(開發中)**\n"
            "`/rpg_forge` - 鍛造裝備 **(開發中)**\n"
            "`/rpg_house` - 房屋管理 **(開發中)**"
        ),
        inline=False
    )
    
    embed.add_field(
        name="📊 屬性系統",
        value=(
            f"**每級獲得：** {RPG_CONFIG['STAT_POINTS_PER_LEVEL']} 屬性點\n"
            "**屬性類型：**\n"
            "• ❤️ 體力：增加 HP\n"
            "• ⚡ 速度：影響行動順序\n"
            "• 💪 力量：物理攻擊力\n"
            "• 🧠 智慧：魔法攻擊力\n"
            "• 🎒 負重：攜帶物品上限"
        ),
        inline=False
    )
    
    embed.add_field(
        name="⚔️ 裝備系統",
        value=(
            "**稀有度：**\n"
            "• 🟢 綠色：普通裝備\n"
            "• 🔵 藍色：稀有裝備\n"
            "• 🟣 紫色：史詩裝備\n"
            "• 🟡 金色：傳說裝備\n\n"
            "**特殊詞條：** 稀有裝備有特殊效果"
        ),
        inline=False
    )
    
    embed.set_footer(text="RPG 系統開發中，更多功能即將推出！")
    
    await interaction.response.send_message(embed=embed)

# ========== 事件處理 ==========

@bot.event
async def on_ready():
    """機器人準備就緒"""
    print(f"\n{'='*50}")
    print(f"🤖 {BOT_NAME} 已上線！")
    print(f"👤 使用者名稱：{bot.user.name}")
    print(f"🆔 使用者ID：{bot.user.id}")
    print(f"{'='*50}\n")
    
    try:
        await bot.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.playing,
                name=f"使用 /help 查看指令 | v2.0"
            )
        )
        
        # 連接資料庫
        connected = await db.connect()
        
        if not connected:
            print("⚠️ 資料庫連接失敗，使用記憶體緩存模式")
            print("⚠️ 注意：數據可能不會永久保存")
        
        print("🔄 正在同步指令樹...")
        
        try:
            global_synced = await tree.sync()
            print(f"✅ 已同步 {len(global_synced)} 個指令")
        except Exception as sync_error:
            print(f"❌ 同步指令失敗: {sync_error}")
        
        print("✅ 機器人準備就緒！")
        
    except Exception as e:
        print(f"❌ 啟動過程中出現錯誤: {e}")
        traceback.print_exc()

@bot.event
async def on_message(message):
    """處理訊息事件"""
    if message.author.bot:
        return
    
    if not message.guild:
        return
    
    # 處理聊天積分
    if not message.content.startswith(('!', '/')) and len(message.content.strip()) >= 2:
        try:
            guild_id = message.guild.id
            added_score, daily_limit = await add_chat_score(
                message.author.id,
                message.author.name,
                guild_id
            )
            
            if added_score > 0 and random.random() < 0.01:  # 1% 機率顯示
                embed = discord.Embed(
                    title="💬 聊天積分",
                    description=f"你獲得了 **{added_score}** 聊天積分！",
                    color=0x00FF00
                )
                embed.add_field(
                    name="📊 積分規則",
                    value=f"每句話 +{CHAT_SCORE}分，每日上限 {daily_limit}分",
                    inline=False
                )
                embed.set_footer(text="積分可用於抽獎和獎勵")
                await message.channel.send(embed=embed, delete_after=10)
                
        except Exception as e:
            print(f"❌ 處理聊天積分錯誤: {e}")
    
    await bot.process_commands(message)

@bot.event
async def on_raw_reaction_add(payload):
    """處理反應事件"""
    if payload.user_id == bot.user.id:
        return
    
    try:
        guild = bot.get_guild(payload.guild_id)
        if not guild:
            return
        
        channel = guild.get_channel(payload.channel_id)
        if not channel:
            return
        
        try:
            message = await channel.fetch_message(payload.message_id)
        except:
            return
        
        guild_id = guild.id
        
        # 處理抽獎參與
        if str(payload.emoji) == "🎫":
            if not db.is_connected:
                return
            
            result = await db.fetchrow(
                "SELECT id, participants FROM giveaways WHERE message_id = $1 AND is_active = true AND guild_id = $2",
                payload.message_id, guild_id
            )
            
            if result:
                giveaway_id = result['id']
                participants = result['participants'] or []
                
                if isinstance(participants, str):
                    try:
                        participants = json.loads(participants)
                    except:
                        participants = []
                
                if payload.user_id not in participants:
                    if isinstance(participants, list):
                        participants.append(payload.user_id)
                    else:
                        participants = [payload.user_id]
                    
                    await db.execute(
                        "UPDATE giveaways SET participants = $1 WHERE id = $2",
                        json.dumps(participants), giveaway_id
                    )
                    
                    try:
                        user = await guild.fetch_member(payload.user_id)
                        await message.remove_reaction(payload.emoji, user)
                    except:
                        pass
                
                return
        
        # 處理抽獎手動結束
        if str(payload.emoji) == "⏹️":
            if not db.is_connected:
                return
            
            result = await db.fetchrow(
                "SELECT creator_id FROM giveaways WHERE message_id = $1 AND is_active = true AND guild_id = $2",
                payload.message_id, guild_id
            )
            
            if result and result['creator_id'] == payload.user_id:
                await end_giveaway(payload.message_id, manual=True, guild_id=guild_id)
                try:
                    await message.remove_reaction(payload.emoji, payload.member)
                except:
                    pass
                return
        
        # 處理評核活動簽到
        if str(payload.emoji) == "✅":
            if not db.is_connected:
                return
            
            result = await db.fetchrow(
                "SELECT id, participants FROM evaluation_events WHERE signup_message_id = $1 AND is_active = true AND guild_id = $2",
                payload.message_id, guild_id
            )
            
            if result:
                event_id = result['id']
                participants = result['participants'] or []
                
                if isinstance(participants, str):
                    try:
                        participants = json.loads(participants)
                    except:
                        participants = []
                
                if payload.user_id not in participants:
                    participants.append(payload.user_id)
                    
                    await db.execute(
                        "UPDATE evaluation_events SET participants = $1 WHERE id = $2",
                        json.dumps(participants), event_id
                    )
                    
                    await update_user_activity(payload.user_id, f"event_{event_id}", attended=True, guild_id=guild_id)
                    
                    try:
                        await message.remove_reaction(payload.emoji, payload.member)
                    except:
                        pass
                
                return
        
        # 處理職業選擇
        emoji_str = str(payload.emoji)
        if emoji_str in PROFESSION_EMOJIS:
            if not db.is_connected:
                return
            
            result = await db.fetchrow(
                "SELECT id, participants, professions FROM evaluation_events WHERE profession_message_id = $1 AND is_active = true AND guild_id = $2",
                payload.message_id, guild_id
            )
            
            if result:
                event_id = result['id']
                participants = result['participants'] or []
                professions = result['professions'] or {}
                
                if isinstance(participants, str):
                    try:
                        participants = json.loads(participants)
                    except:
                        participants = []
                
                if isinstance(professions, str):
                    try:
                        professions = json.loads(professions)
                    except:
                        professions = {}
                
                if payload.user_id in participants:
                    profession = PROFESSION_EMOJIS[emoji_str]
                    professions[str(payload.user_id)] = profession
                    
                    await db.execute(
                        "UPDATE evaluation_events SET professions = $1 WHERE id = $2",
                        json.dumps(professions), event_id
                    )
                    
                    await update_user_profession(payload.user_id, profession, guild_id)
                    
                    try:
                        user = await guild.fetch_member(payload.user_id)
                        await user.send(f"✅ 你已選擇職業：**{profession}**")
                    except:
                        pass
                    
                    try:
                        await message.remove_reaction(payload.emoji, user)
                    except:
                        pass
                
                return
        
        # 處理評核評分
        if emoji_str in RATING_EMOJIS or emoji_str == RATING_END_EMOJI:
            if not db.is_connected:
                return
            
            result = await db.fetchrow(
                "SELECT id, creator_id, participants, ratings, default_rated FROM evaluation_events WHERE rating_message_id = $1 AND is_active = true AND guild_id = $2",
                payload.message_id, guild_id
            )
            
            if result:
                event_id = result['id']
                creator_id = result['creator_id']
                participants = result['participants'] or []
                ratings = result['ratings'] or {}
                default_rated = result['default_rated'] or []
                
                if isinstance(participants, str):
                    try:
                        participants = json.loads(participants)
                    except:
                        participants = []
                
                if isinstance(ratings, str):
                    try:
                        ratings = json.loads(ratings)
                    except:
                        ratings = {}
                
                if isinstance(default_rated, str):
                    try:
                        default_rated = json.loads(default_rated)
                    except:
                        default_rated = []
                
                # 結束評核
                if emoji_str == RATING_END_EMOJI and payload.user_id == creator_id:
                    channel = message.channel
                    event_name = await db.fetchval(
                        "SELECT event_name FROM evaluation_events WHERE id = $1",
                        event_id
                    )
                    
                    if event_name:
                        await end_evaluation(event_id, channel, event_name, guild_id)
                    
                    try:
                        await message.clear_reactions()
                    except:
                        pass
                    
                    return
                
                # 評分
                if emoji_str in RATING_EMOJIS:
                    rating_type = RATING_EMOJIS[emoji_str]
                    
                    for user_id in participants:
                        if str(user_id) in default_rated:
                            continue
                        
                        if payload.user_id != user_id:
                            continue
                        
                        user_ratings = ratings.get(str(user_id), [])
                        if isinstance(user_ratings, str):
                            try:
                                user_ratings = json.loads(user_ratings)
                            except:
                                user_ratings = []
                        
                        if not isinstance(user_ratings, list):
                            user_ratings = []
                        
                        user_ratings.append({
                            "rater": payload.user_id,
                            "rating": rating_type,
                            "timestamp": datetime.now().isoformat()
                        })
                        
                        ratings[str(user_id)] = user_ratings
                        
                        await update_user_rating(user_id, rating_type, guild_id)
                        
                        default_rated.append(str(user_id))
                        
                        await db.execute(
                            "UPDATE evaluation_events SET ratings = $1, default_rated = $2 WHERE id = $3",
                            json.dumps(ratings), json.dumps(default_rated), event_id
                        )
                        
                        try:
                            await message.remove_reaction(payload.emoji, payload.member)
                        except:
                            pass
                        
                        break
                    
                    return
                    
    except Exception as e:
        print(f"❌ 處理反應事件錯誤: {e}")
        traceback.print_exc()

# ========== 錯誤處理 ==========

@bot.event
async def on_command_error(ctx, error):
    """處理指令錯誤"""
    if isinstance(error, commands.CommandNotFound):
        return
    
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("❌ 你沒有足夠的權限使用此指令！")
        return
    
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f"❌ 缺少必要參數：{error.param.name}")
        return
    
    if isinstance(error, commands.BadArgument):
        await ctx.send("❌ 參數格式錯誤！")
        return
    
    print(f"❌ 指令錯誤: {error}")
    traceback.print_exc()

@bot.event
async def on_app_command_error(interaction: discord.Interaction, error):
    """處理斜槓指令錯誤"""
    if isinstance(error, app_commands.errors.MissingPermissions):
        await interaction.response.send_message("❌ 你沒有足夠的權限使用此指令！", ephemeral=True)
        return
    
    if isinstance(error, app_commands.errors.CommandOnCooldown):
        await interaction.response.send_message(f"❌ 指令冷卻中，請等待 {error.retry_after:.1f} 秒", ephemeral=True)
        return
    
    print(f"❌ 斜槓指令錯誤: {error}")
    traceback.print_exc()
    
    try:
        await interaction.response.send_message(
            f"❌ 執行指令時發生錯誤：{str(error)[:100]}",
            ephemeral=True
        )
    except:
        pass

# ========== 啟動機器人 ==========

if __name__ == "__main__":
    token = os.getenv('DISCORD_TOKEN')
    
    if not token:
        print("❌ 找不到 DISCORD_TOKEN 環境變數！")
        print("💡 請設定環境變數：")
        print("   1. 在 Railway 中設定 DISCORD_TOKEN")
        print("   2. 或在 .env 檔案中添加 DISCORD_TOKEN=your_bot_token")
        sys.exit(1)
    
    print("🤖 正在啟動機器人...")
    
    try:
        bot.run(token)
    except KeyboardInterrupt:
        print("\n🛑 機器人正在關閉...")
    except Exception as e:
        print(f"❌ 機器人啟動失敗: {e}")
        traceback.print_exc()

# ========== RPG 冒險探索系統 ==========

@tree.command(name="rpg_explore", description="開始冒險探索")
async def rpg_explore_slash(interaction: discord.Interaction):
    """開始冒險探索"""
    await interaction.response.defer()
    
    try:
        if not db.is_connected:
            embed = discord.Embed(
                title="⚠️ 資料庫未連接",
                description="無法進行探索，請稍後再試。",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        player = await get_rpg_player(interaction.user.id)
        
        if not player:
            embed = discord.Embed(
                title="❌ 角色不存在",
                description="請先使用 `/rpg_start` 創建角色",
                color=0xFF0000
            )
            await interaction.followup.send(embed=embed)
            return
        
        if not player['is_in_town']:
            embed = discord.Embed(
                title="❌ 已在探索中",
                description="你已經在探索地圖中，請先返回城鎮。",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        # 檢查隊伍狀態
        party_member = await db.fetchrow(
            "SELECT party_id FROM rpg_party_members WHERE user_id = $1",
            interaction.user.id
        )
        
        if party_member:
            embed = discord.Embed(
                title="❌ 隊伍探索中",
                description="你已在隊伍中，請使用隊伍指令進行探索。",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        maps = RPG_CONFIG["MAPS"]
        current_map = player['current_map']
        
        if current_map not in maps:
            current_map = "新手森林"
            await db.execute(
                "UPDATE rpg_players SET current_map = $1, current_layer = 1 WHERE user_id = $2",
                current_map, interaction.user.id
            )
        
        map_info = maps[current_map]
        
        embed = discord.Embed(
            title=f"🗺️ 探索地圖：{current_map}",
            description=f"層數：{player['current_layer']}/{map_info['layers']}",
            color=0x2ECC71
        )
        
        embed.add_field(
            name="📊 地圖資訊",
            value=(
                f"**適合等級：** {map_info['min_level']}-{map_info['max_level']}\n"
                f"**總層數：** {map_info['layers']}\n"
                f"**怪物數量：** {map_info['monster_count']}隻\n"
                f"**Boss：** {map_info['boss']}"
            ),
            inline=False
        )
        
        if player['level'] < map_info['min_level']:
            embed.add_field(
                name="⚠️ 等級不足",
                value=f"建議等級 {map_info['min_level']} 以上，你目前 {player['level']} 級",
                color=0xFFA500
            )
        
        # 創建探索視圖
        class ExploreView(discord.ui.View):
            def __init__(self, user_id, current_layer):
                super().__init__(timeout=120)
                self.user_id = user_id
                self.current_layer = current_layer
                self.monsters_killed = 0
                self.is_battling = False
                
            @discord.ui.button(label="前進", style=discord.ButtonStyle.primary, emoji="🏃‍♂️", row=0)
            async def move_forward(self, interaction: discord.Interaction, button: discord.ui.Button):
                if interaction.user.id != self.user_id:
                    await interaction.response.send_message("❌ 這不是你的探索！", ephemeral=True)
                    return
                
                if self.is_battling:
                    await interaction.response.send_message("❌ 正在戰鬥中！", ephemeral=True)
                    return
                
                player = await get_rpg_player(self.user_id)
                if not player:
                    await interaction.response.send_message("❌ 角色不存在！", ephemeral=True)
                    return
                
                map_info = maps[player['current_map']]
                
                if player['current_layer'] >= map_info['layers']:
                    await interaction.response.send_message("❌ 已到達最高層！", ephemeral=True)
                    return
                
                # 更新層數
                new_layer = player['current_layer'] + 1
                await db.execute(
                    "UPDATE rpg_players SET current_layer = $1 WHERE user_id = $2",
                    new_layer, self.user_id
                )
                
                # 隨機遭遇事件
                event_type = random.choices(
                    ['monster', 'treasure', 'nothing', 'healing'],
                    weights=[0.5, 0.2, 0.2, 0.1]
                )[0]
                
                if event_type == 'monster':
                    await self.encounter_monster(interaction, player, new_layer)
                elif event_type == 'treasure':
                    await self.find_treasure(interaction, player, new_layer)
                elif event_type == 'healing':
                    await self.find_healing(interaction, player, new_layer)
                else:
                    embed = discord.Embed(
                        title="🌲 平靜的旅程",
                        description=f"你安全地前進到了第 {new_layer} 層",
                        color=0x7289DA
                    )
                    embed.add_field(
                        name="📍 目前位置",
                        value=f"**地圖：** {player['current_map']}\n**層數：** {new_layer}/{map_info['layers']}",
                        inline=False
                    )
                    await interaction.response.edit_message(embed=embed)
                
            @discord.ui.button(label="後退", style=discord.ButtonStyle.secondary, emoji="🔙", row=0)
            async def move_back(self, interaction: discord.Interaction, button: discord.ui.Button):
                if interaction.user.id != self.user_id:
                    await interaction.response.send_message("❌ 這不是你的探索！", ephemeral=True)
                    return
                
                if self.is_battling:
                    await interaction.response.send_message("❌ 正在戰鬥中！", ephemeral=True)
                    return
                
                player = await get_rpg_player(self.user_id)
                if not player:
                    await interaction.response.send_message("❌ 角色不存在！", ephemeral=True)
                    return
                
                if player['current_layer'] <= 1:
                    await interaction.response.send_message("❌ 已在第1層！", ephemeral=True)
                    return
                
                new_layer = player['current_layer'] - 1
                await db.execute(
                    "UPDATE rpg_players SET current_layer = $1 WHERE user_id = $2",
                    new_layer, self.user_id
                )
                
                map_info = maps[player['current_map']]
                embed = discord.Embed(
                    title="🔙 後退",
                    description=f"你退回到了第 {new_layer} 層",
                    color=0x7289DA
                )
                embed.add_field(
                    name="📍 目前位置",
                    value=f"**地圖：** {player['current_map']}\n**層數：** {new_layer}/{map_info['layers']}",
                    inline=False
                )
                
                await interaction.response.edit_message(embed=embed)
            
            @discord.ui.button(label="返回城鎮", style=discord.ButtonStyle.success, emoji="🏠", row=1)
            async def return_to_town(self, interaction: discord.Interaction, button: discord.ui.Button):
                if interaction.user.id != self.user_id:
                    await interaction.response.send_message("❌ 這不是你的探索！", ephemeral=True)
                    return
                
                if self.is_battling:
                    await interaction.response.send_message("❌ 正在戰鬥中！", ephemeral=True)
                    return
                
                await db.execute(
                    "UPDATE rpg_players SET is_in_town = true WHERE user_id = $1",
                    self.user_id
                )
                
                # 更新探索統計
                if self.monsters_killed > 0:
                    await db.execute(
                        "UPDATE rpg_players SET monsters_killed = monsters_killed + $1 WHERE user_id = $2",
                        self.monsters_killed, self.user_id
                    )
                
                embed = discord.Embed(
                    title="🏠 返回城鎮",
                    description="你已安全返回城鎮",
                    color=0x00FF00
                )
                embed.add_field(
                    name="📊 本次探索",
                    value=f"**擊敗怪物：** {self.monsters_killed}隻",
                    inline=False
                )
                
                await interaction.response.edit_message(embed=embed, view=None)
                self.stop()
            
            async def encounter_monster(self, interaction: discord.Interaction, player, layer):
                self.is_battling = True
                
                # 生成怪物
                monster_level = max(1, min(player['level'] + random.randint(-2, 2), 300))
                monster_rarity = random.choices(
                    ['normal', 'elite'],
                    weights=[0.85, 0.15]
                )[0]
                
                monster_name = self.generate_monster_name(player['current_map'], monster_rarity)
                
                # 怪物屬性
                base_stats = RPG_CONFIG["BASE_STATS"]
                monster_hp = int((base_stats['vitality'] * 10) * (monster_level / 10) * (1.5 if monster_rarity == 'elite' else 1))
                monster_attack = int((base_stats['strength'] * 2) * (monster_level / 10) * (1.3 if monster_rarity == 'elite' else 1))
                monster_defense = int((base_stats['vitality'] * 0.5) * (monster_level / 10) * (1.2 if monster_rarity == 'elite' else 1))
                
                embed = discord.Embed(
                    title=f"⚔️ 遭遇怪物！",
                    description=f"遇到了 **{monster_name}**",
                    color=0xFF0000 if monster_rarity == 'elite' else 0xFFA500
                )
                
                rarity_text = "精英" if monster_rarity == 'elite' else "普通"
                embed.add_field(
                    name="📊 怪物資訊",
                    value=(
                        f"**名稱：** {monster_name}\n"
                        f"**等級：** {monster_level} ({rarity_text})\n"
                        f"**HP：** {monster_hp}\n"
                        f"**攻擊：** {monster_attack}\n"
                        f"**防禦：** {monster_defense}"
                    ),
                    inline=False
                )
                
                embed.add_field(
                    name="📊 你的狀態",
                    value=(
                        f"**HP：** {player['current_hp']}/{player['max_hp']}\n"
                        f"**MP：** {player['current_mp']}/{player['max_mp']}\n"
                        f"**攻擊：** {player['strength']}\n"
                        f"**防禦：** {player['vitality'] * 0.5}"
                    ),
                    inline=False
                )
                
                # 戰鬥視圖
                class BattleView(discord.ui.View):
                    def __init__(self, user_id, monster_name, monster_hp, monster_attack, monster_defense, monster_level, monster_rarity):
                        super().__init__(timeout=60)
                        self.user_id = user_id
                        self.monster_name = monster_name
                        self.monster_hp = monster_hp
                        self.max_monster_hp = monster_hp
                        self.monster_attack = monster_attack
                        self.monster_defense = monster_defense
                        self.monster_level = monster_level
                        self.monster_rarity = monster_rarity
                        self.turn = 'player'
                        self.battle_log = []
                    
                    @discord.ui.button(label="攻擊", style=discord.ButtonStyle.danger, emoji="⚔️", row=0)
                    async def attack(self, interaction: discord.Interaction, button: discord.ui.Button):
                        if interaction.user.id != self.user_id:
                            await interaction.response.send_message("❌ 這不是你的戰鬥！", ephemeral=True)
                            return
                        
                        if self.turn != 'player':
                            await interaction.response.send_message("❌ 現在是怪物的回合！", ephemeral=True)
                            return
                        
                        await self.player_turn(interaction, 'attack')
                    
                    @discord.ui.button(label="防禦", style=discord.ButtonStyle.primary, emoji="🛡️", row=0)
                    async def defend(self, interaction: discord.Interaction, button: discord.ui.Button):
                        if interaction.user.id != self.user_id:
                            await interaction.response.send_message("❌ 這不是你的戰鬥！", ephemeral=True)
                            return
                        
                        if self.turn != 'player':
                            await interaction.response.send_message("❌ 現在是怪物的回合！", ephemeral=True)
                            return
                        
                        await self.player_turn(interaction, 'defend')
                    
                    @discord.ui.button(label="逃跑", style=discord.ButtonStyle.secondary, emoji="🏃", row=1)
                    async def flee(self, interaction: discord.Interaction, button: discord.ui.Button):
                        if interaction.user.id != self.user_id:
                            await interaction.response.send_message("❌ 這不是你的戰鬥！", ephemeral=True)
                            return
                        
                        flee_chance = 0.7  # 70%逃跑成功率
                        if random.random() < flee_chance:
                            await self.end_battle(interaction, 'flee')
                        else:
                            self.battle_log.append("逃跑失敗！")
                            self.turn = 'monster'
                            await self.monster_turn(interaction)
                    
                    async def player_turn(self, interaction: discord.Interaction, action):
                        player = await get_rpg_player(self.user_id)
                        if not player:
                            await interaction.response.send_message("❌ 角色不存在！", ephemeral=True)
                            return
                        
                        damage = 0
                        if action == 'attack':
                            # 計算玩家傷害
                            base_damage = player['strength'] * 2
                            defense_reduction = self.monster_defense * 0.3
                            damage = max(1, int(base_damage - defense_reduction))
                            
                            # 爆擊計算
                            crit_chance = 0.1  # 10%爆擊率
                            crit_damage = 1.5  # 50%額外傷害
                            
                            if random.random() < crit_chance:
                                damage = int(damage * crit_damage)
                                self.battle_log.append(f"💥 **爆擊！** 你對 {self.monster_name} 造成了 {damage} 點傷害！")
                            else:
                                self.battle_log.append(f"⚔️ 你對 {self.monster_name} 造成了 {damage} 點傷害")
                            
                            self.monster_hp -= damage
                        
                        elif action == 'defend':
                            # 防禦效果：下回合減傷
                            defense_bonus = 0.5  # 50%減傷
                            self.battle_log.append(f"🛡️ 你擺出了防禦姿態，下回合減傷 {defense_bonus*100}%")
                        
                        # 檢查怪物是否死亡
                        if self.monster_hp <= 0:
                            await self.end_battle(interaction, 'win')
                            return
                        
                        self.turn = 'monster'
                        await self.monster_turn(interaction)
                    
                    async def monster_turn(self, interaction: discord.Interaction):
                        player = await get_rpg_player(self.user_id)
                        if not player:
                            await interaction.response.send_message("❌ 角色不存在！", ephemeral=True)
                            return
                        
                        # 怪物攻擊
                        monster_damage = max(1, int(self.monster_attack * 0.8))
                        
                        # 檢查玩家是否防禦
                        is_defending = False
                        for log in reversed(self.battle_log):
                            if "擺出了防禦姿態" in log:
                                is_defending = True
                                break
                        
                        if is_defending:
                            monster_damage = int(monster_damage * 0.5)  # 減傷50%
                            self.battle_log.append(f"🛡️ 防禦生效！{self.monster_name} 的攻擊被減弱")
                        
                        self.battle_log.append(f"👹 {self.monster_name} 對你造成了 {monster_damage} 點傷害")
                        
                        new_hp = player['current_hp'] - monster_damage
                        if new_hp <= 0:
                            await self.end_battle(interaction, 'lose')
                            return
                        
                        await db.execute(
                            "UPDATE rpg_players SET current_hp = $1 WHERE user_id = $2",
                            new_hp, self.user_id
                        )
                        
                        self.turn = 'player'
                        await self.update_battle_embed(interaction)
                    
                    async def update_battle_embed(self, interaction: discord.Interaction):
                        player = await get_rpg_player(self.user_id)
                        if not player:
                            return
                        
                        embed = discord.Embed(
                            title=f"⚔️ 戰鬥中 - {self.monster_name}",
                            description="\n".join(self.battle_log[-5:]),
                            color=0xFF0000 if self.monster_rarity == 'elite' else 0xFFA500
                        )
                        
                        hp_percent = (player['current_hp'] / player['max_hp']) * 100
                        monster_hp_percent = (self.monster_hp / self.max_monster_hp) * 100
                        
                        hp_bar = create_progress_bar(hp_percent, 15)
                        monster_hp_bar = create_progress_bar(monster_hp_percent, 15)
                        
                        embed.add_field(
                            name="❤️‍🩹 你的HP",
                            value=f"{hp_bar}\n{player['current_hp']}/{player['max_hp']}",
                            inline=False
                        )
                        
                        embed.add_field(
                            name=f"👹 {self.monster_name}的HP",
                            value=f"{monster_hp_bar}\n{self.monster_hp}/{self.max_monster_hp}",
                            inline=False
                        )
                        
                        embed.add_field(
                            name="🎯 行動",
                            value=f"**當前回合：** {'你的回合' if self.turn == 'player' else '怪物的回合'}",
                            inline=True
                        )
                        
                        await interaction.response.edit_message(embed=embed)
                    
                    async def end_battle(self, interaction: discord.Interaction, result):
                        if result == 'win':
                            # 計算經驗值獎勵
                            base_exp = 50
                            level_bonus = max(1, self.monster_level / player['level'])
                            rarity_bonus = 2 if self.monster_rarity == 'elite' else 1
                            exp_gained = int(base_exp * level_bonus * rarity_bonus)
                            
                            # 更新玩家經驗
                            new_exp = player['exp'] + exp_gained
                            level_up = False
                            new_level = player['level']
                            
                            while new_exp >= player['max_exp']:
                                new_exp -= player['max_exp']
                                new_level += 1
                                level_up = True
                                
                                # 計算新等級所需經驗
                                new_max_exp = calculate_exp_required(new_level)
                                await db.execute(
                                    """
                                    UPDATE rpg_players 
                                    SET level = $1, exp = $2, max_exp = $3,
                                        current_hp = max_hp, current_mp = max_mp,
                                        last_active = NOW()
                                    WHERE user_id = $4
                                    """,
                                    new_level, new_exp, new_max_exp, self.user_id
                                )
                            
                            if not level_up:
                                await db.execute(
                                    "UPDATE rpg_players SET exp = $1 WHERE user_id = $2",
                                    new_exp, self.user_id
                                )
                            
                            # 掉落物品
                            drop_chance = 0.3 if self.monster_rarity == 'normal' else 0.6
                            drop_text = ""
                            
                            if random.random() < drop_chance:
                                rarity_weights = RPG_CONFIG["DROP_RATES"][self.monster_rarity]
                                rarity = random.choices(
                                    list(rarity_weights.keys()),
                                    weights=list(rarity_weights.values())
                                )[0]
                                
                                # 生成掉落物品
                                item_types = ['material', 'potion', 'equipment']
                                item_type = random.choice(item_types)
                                
                                if item_type == 'material':
                                    item_name = f"怪物素材 [{rarity}]"
                                    item_value = 10
                                elif item_type == 'potion':
                                    potion_types = ['hp', 'mp']
                                    potion_type = random.choice(potion_types)
                                    size = random.choice(['小', '中', '大'])
                                    item_name = f"{size}{'紅' if potion_type == 'hp' else '藍'}藥水"
                                    item_value = 30 if potion_type == 'hp' else 20
                                else:
                                    equipment_types = ['武器', '防具', '飾品']
                                    equip_type = random.choice(equipment_types)
                                    item_name = f"{equip_type} [{rarity}]"
                                    item_value = 50
                                
                                drop_text = f"\n🎁 **獲得掉落物：** {item_name}"
                            
                            embed = discord.Embed(
                                title="🎉 戰鬥勝利！",
                                description=(
                                    f"你擊敗了 **{self.monster_name}**！\n"
                                    f"**獲得經驗：** +{exp_gained} ⭐\n"
                                    f"{drop_text}"
                                ),
                                color=0x00FF00
                            )
                            
                            if level_up:
                                embed.add_field(
                                    name="✨ 等級提升！",
                                    value=f"恭喜升到 **{new_level}** 級！",
                                    inline=False
                                )
                            
                            # 更新探索統計
                            await db.execute(
                                """
                                UPDATE rpg_players 
                                SET monsters_killed = monsters_killed + 1,
                                    total_damage = total_damage + $1
                                WHERE user_id = $2
                                """,
                                exp_gained * 10, self.user_id
                            )
                            
                            # 記錄戰鬥
                            await db.execute(
                                """
                                INSERT INTO rpg_battles (
                                    user_id, monster_name, battle_result, 
                                    damage_dealt, exp_gained, battle_duration
                                ) VALUES ($1, $2, $3, $4, $5, $6)
                                """,
                                self.user_id, self.monster_name, 'win',
                                exp_gained * 10, exp_gained, 60
                            )
                            
                        elif result == 'lose':
                            # 死亡懲罰
                            exp_loss = int(player['exp'] * 0.1)
                            new_exp = max(0, player['exp'] - exp_loss)
                            
                            await db.execute(
                                """
                                UPDATE rpg_players 
                                SET current_hp = 1, exp = $1,
                                    deaths = deaths + 1,
                                    last_active = NOW()
                                WHERE user_id = $2
                                """,
                                new_exp, self.user_id
                            )
                            
                            embed = discord.Embed(
                                title="💀 戰鬥失敗",
                                description=(
                                    f"你被 **{self.monster_name}** 擊敗了！\n"
                                    f"**經驗損失：** -{exp_loss} ⭐\n"
                                    f"HP恢復到 1，請及時治療"
                                ),
                                color=0xFF0000
                            )
                            
                            # 記錄戰鬥
                            await db.execute(
                                """
                                INSERT INTO rpg_battles (
                                    user_id, monster_name, battle_result,
                                    damage_taken, battle_duration
                                ) VALUES ($1, $2, $3, $4, $5)
                                """,
                                self.user_id, self.monster_name, 'lose',
                                player['current_hp'], 60
                            )
                            
                        else:  # flee
                            embed = discord.Embed(
                                title="🏃 成功逃跑",
                                description=f"你成功從 **{self.monster_name}** 面前逃跑了",
                                color=0xFFA500
                            )
                        
                        await interaction.response.edit_message(embed=embed, view=None)
                        self.stop()
                
                battle_view = BattleView(
                    interaction.user.id, monster_name,
                    monster_hp, monster_attack, monster_defense,
                    monster_level, monster_rarity
                )
                
                await interaction.response.edit_message(embed=embed, view=battle_view)
                
                # 等待戰鬥結束
                await battle_view.wait()
                self.is_battling = False
                self.monsters_killed += 1
            
            async def find_treasure(self, interaction: discord.Interaction, player, layer):
                # 寶箱獎勵
                treasure_types = [
                    ("💰 金幣", "獲得一些金幣", 0x00FF00),
                    ("💎 寶石", "找到閃亮的寶石", 0xFFD700),
                    ("📜 卷軸", "發現魔法卷軸", 0x800080),
                    ("⚔️ 裝備", "找到一件裝備", 0x2ECC71)
                ]
                
                treasure = random.choice(treasure_types)
                treasure_name, treasure_desc, treasure_color = treasure
                
                # 獎勵數量
                reward_multiplier = layer * (player['level'] / 10)
                reward_amount = int(random.randint(10, 50) * reward_multiplier)
                
                embed = discord.Embed(
                    title="🎁 發現寶箱！",
                    description=treasure_desc,
                    color=treasure_color
                )
                
                embed.add_field(
                    name="📦 寶箱內容",
                    value=f"**{treasure_name}** x{reward_amount}",
                    inline=False
                )
                
                embed.add_field(
                    name="📍 目前位置",
                    value=f"**地圖：** {player['current_map']}\n**層數：** {layer}",
                    inline=False
                )
                
                await interaction.response.edit_message(embed=embed)
            
            async def find_healing(self, interaction: discord.Interaction, player, layer):
                # 治療效果
                heal_amount = int(player['max_hp'] * random.uniform(0.1, 0.3))
                new_hp = min(player['max_hp'], player['current_hp'] + heal_amount)
                
                mp_recover = int(player['max_mp'] * random.uniform(0.1, 0.2))
                new_mp = min(player['max_mp'], player['current_mp'] + mp_recover)
                
                await db.execute(
                    "UPDATE rpg_players SET current_hp = $1, current_mp = $2 WHERE user_id = $3",
                    new_hp, new_mp, interaction.user.id
                )
                
                embed = discord.Embed(
                    title="💚 發現治療泉水",
                    description="你發現了一處神奇的治療泉水",
                    color=0x00FF00
                )
                
                embed.add_field(
                    name="💊 恢復效果",
                    value=(
                        f"**HP恢復：** +{heal_amount} ({player['current_hp']} → {new_hp})\n"
                        f"**MP恢復：** +{mp_recover} ({player['current_mp']} → {new_mp})"
                    ),
                    inline=False
                )
                
                embed.add_field(
                    name="📍 目前位置",
                    value=f"**地圖：** {player['current_map']}\n**層數：** {layer}",
                    inline=False
                )
                
                await interaction.response.edit_message(embed=embed)
            
            def generate_monster_name(self, map_name, rarity):
                """生成怪物名稱"""
                biomes = {
                    "新手森林": ["樹精", "野豬", "哥布林", "狼", "蜘蛛"],
                    "沙漠遺跡": ["沙蟲", "蠍子", "木乃伊", "仙人掌怪", "沙暴元素"],
                    "冰封山脈": ["雪怪", "冰狼", "寒冰元素", "霜凍蜘蛛", "冰晶獸"],
                    "深淵地獄": ["惡魔", "地獄犬", "炎魔", "骷髏戰士", "怨靈"]
                }
                
                base_names = biomes.get(map_name, ["怪物"])
                base_name = random.choice(base_names)
                
                if rarity == 'elite':
                    prefixes = ["狂暴的", "巨大的", "變異的", "遠古的", "被詛咒的"]
                    prefix = random.choice(prefixes)
                    return f"{prefix}{base_name}"
                else:
                    return base_name
        
        # 開始探索
        await db.execute(
            "UPDATE rpg_players SET is_in_town = false WHERE user_id = $1",
            interaction.user.id
        )
        
        view = ExploreView(interaction.user.id, player['current_layer'])
        await interaction.followup.send(embed=embed, view=view)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 探索失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

# ========== 背包系統 ==========

@tree.command(name="rpg_inventory", description="查看背包")
async def rpg_inventory_slash(interaction: discord.Interaction):
    """查看背包"""
    await interaction.response.defer()
    
    try:
        if not db.is_connected:
            embed = discord.Embed(
                title="⚠️ 資料庫未連接",
                description="無法讀取背包，請稍後再試。",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        # 獲取玩家背包物品
        items = await db.fetch(
            '''
            SELECT 
                i.id, i.name, i.item_type, i.rarity, 
                i.level_requirement, i.description,
                inv.quantity
            FROM rpg_inventory inv
            JOIN rpg_items i ON inv.item_id = i.id
            WHERE inv.user_id = $1 AND inv.slot_type = 'inventory'
            ORDER BY 
                CASE i.item_type
                    WHEN 'weapon' THEN 1
                    WHEN 'head' THEN 2
                    WHEN 'body' THEN 3
                    WHEN 'shoes' THEN 4
                    WHEN 'necklace' THEN 5
                    WHEN 'ring' THEN 6
                    WHEN 'backpack' THEN 7
                    WHEN 'potion' THEN 8
                    WHEN 'material' THEN 9
                    ELSE 10
                END,
                i.rarity DESC,
                i.level_requirement DESC,
                i.name
            ''',
            interaction.user.id
        )
        
        if not items:
            embed = discord.Embed(
                title="🎒 背包",
                description="你的背包是空的",
                color=0x7289DA
            )
            await interaction.followup.send(embed=embed)
            return
        
        # 計算背包容量
        player = await get_rpg_player(interaction.user.id)
        if not player:
            embed = discord.Embed(
                title="❌ 角色不存在",
                description="請先使用 `/rpg_start` 創建角色",
                color=0xFF0000
            )
            await interaction.followup.send(embed=embed)
            return
        
        total_items = sum(item['quantity'] for item in items)
        capacity = player['carrying_capacity'] * 10  # 每點負重=10格
        
        embed = discord.Embed(
            title="🎒 背包",
            description=f"物品數：{total_items}/{capacity}",
            color=0x7289DA
        )
        
        # 分類顯示物品
        categories = {
            '武器': [],
            '防具': [],
            '藥水': [],
            '材料': [],
            '其他': []
        }
        
        rarity_colors = {
            'green': 0x00FF00,
            'blue': 0x0000FF,
            'purple': 0x800080,
            'gold': 0xFFD700,
            'normal': 0xCCCCCC
        }
        
        for item in items:
            item_type = item['item_type']
            rarity = item['rarity']
            name = item['name']
            quantity = item['quantity']
            level_req = item['level_requirement']
            
            # 確定分類
            if item_type in ['weapon']:
                category = '武器'
            elif item_type in ['head', 'body', 'shoes']:
                category = '防具'
            elif item_type == 'potion':
                category = '藥水'
            elif item_type == 'material':
                category = '材料'
            else:
                category = '其他'
            
            # 格式化顯示
            rarity_emoji = {
                'green': '🟢',
                'blue': '🔵',
                'purple': '🟣',
                'gold': '🟡',
                'normal': '⚪'
            }.get(rarity, '⚪')
            
            item_display = f"{rarity_emoji} **{name}**"
            if quantity > 1:
                item_display += f" x{quantity}"
            if level_req > 1:
                item_display += f" (Lv.{level_req})"
            
            categories[category].append(item_display)
        
        # 添加分類到embed
        for category, item_list in categories.items():
            if item_list:
                embed.add_field(
                    name=f"{category} ({len(item_list)}種)",
                    value="\n".join(item_list[:10]),  # 每類最多顯示10個
                    inline=False
                )
                
                if len(item_list) > 10:
                    embed.add_field(
                        name=f"更多{category}",
                        value=f"... 還有 {len(item_list) - 10} 個",
                        inline=False
                    )
        
        # 添加倉庫資訊
        storage_items = await db.fetchval(
            "SELECT COUNT(*) FROM rpg_inventory WHERE user_id = $1 AND location = 'storage'",
            interaction.user.id
        ) or 0
        
        embed.add_field(
            name="🏠 倉庫",
            value=f"**物品數：** {storage_items}/{player['storage_capacity']}",
            inline=True
        )
        
        # 創建互動視圖
        class InventoryView(discord.ui.View):
            def __init__(self, user_id, items):
                super().__init__(timeout=60)
                self.user_id = user_id
                self.items = items
                
                # 添加物品選擇下拉選單
                if items:
                    self.item_select = discord.ui.Select(
                        placeholder="選擇物品查看詳情...",
                        min_values=1,
                        max_values=1,
                        options=[
                            discord.SelectOption(
                                label=f"{item['name'][:25]}",
                                description=f"數量: {item['quantity']} | 類型: {item['item_type']}",
                                value=str(item['id'])
                            )
                            for item in items[:25]  # 最多25個選項
                        ]
                    )
                    self.item_select.callback = self.show_item_detail
                    self.add_item(self.item_select)
            
            async def show_item_detail(self, interaction: discord.Interaction):
                if interaction.user.id != self.user_id:
                    await interaction.response.send_message("❌ 這不是你的背包！", ephemeral=True)
                    return
                
                item_id = int(self.item_select.values[0])
                
                # 獲取物品詳細資訊
                item = await db.fetchrow(
                    '''
                    SELECT 
                        i.*,
                        inv.quantity,
                        inv.slot_type,
                        inv.location
                    FROM rpg_inventory inv
                    JOIN rpg_items i ON inv.item_id = i.id
                    WHERE i.id = $1 AND inv.user_id = $2
                    ''',
                    item_id, self.user_id
                )
                
                if not item:
                    await interaction.response.send_message("❌ 物品不存在！", ephemeral=True)
                    return
                
                embed = await create_item_embed(item)
                
                # 添加動作按鈕
                action_view = discord.ui.View(timeout=30)
                
                if item['item_type'] in ['weapon', 'head', 'body', 'shoes', 'necklace', 'ring', 'backpack']:
                    equip_button = discord.ui.Button(
                        label="裝備",
                        style=discord.ButtonStyle.primary,
                        emoji="⚔️"
                    )
                    
                    async def equip_callback(interaction: discord.Interaction):
                        slot_mapping = {
                            'weapon': 'weapon',
                            'head': 'head',
                            'body': 'body',
                            'shoes': 'shoes',
                            'necklace': 'necklace',
                            'ring': 'ring',
                            'backpack': 'backpack'
                        }
                        
                        slot = slot_mapping.get(item['item_type'])
                        if slot:
                            success = await equip_item(self.user_id, item_id, slot)
                            if success:
                                await interaction.response.send_message(
                                    f"✅ 已裝備 {item['name']}",
                                    ephemeral=True
                                )
                            else:
                                await interaction.response.send_message(
                                    "❌ 裝備失敗，可能是等級不足",
                                    ephemeral=True
                                )
                        else:
                            await interaction.response.send_message(
                                "❌ 無法裝備此類物品",
                                ephemeral=True
                            )
                    
                    equip_button.callback = equip_callback
                    action_view.add_item(equip_button)
                
                if item['item_type'] == 'potion':
                    use_button = discord.ui.Button(
                        label="使用",
                        style=discord.ButtonStyle.success,
                        emoji="💊"
                    )
                    
                    async def use_callback(interaction: discord.Interaction):
                        # 使用藥水邏輯
                        if item['potion_type'] == 'hp':
                            player = await get_rpg_player(self.user_id)
                            if player:
                                heal_amount = item['potion_value']
                                new_hp = min(player['max_hp'], player['current_hp'] + heal_amount)
                                
                                await db.execute(
                                    "UPDATE rpg_players SET current_hp = $1 WHERE user_id = $2",
                                    new_hp, self.user_id
                                )
                                
                                # 減少物品數量
                                if item['quantity'] > 1:
                                    await db.execute(
                                        "UPDATE rpg_inventory SET quantity = quantity - 1 WHERE user_id = $1 AND item_id = $2",
                                        self.user_id, item_id
                                    )
                                else:
                                    await db.execute(
                                        "DELETE FROM rpg_inventory WHERE user_id = $1 AND item_id = $2",
                                        self.user_id, item_id
                                    )
                                
                                await interaction.response.send_message(
                                    f"💚 使用了 {item['name']}，恢復了 {heal_amount} HP",
                                    ephemeral=True
                                )
                        
                        elif item['potion_type'] == 'mp':
                            player = await get_rpg_player(self.user_id)
                            if player:
                                mp_amount = item['potion_value']
                                new_mp = min(player['max_mp'], player['current_mp'] + mp_amount)
                                
                                await db.execute(
                                    "UPDATE rpg_players SET current_mp = $1 WHERE user_id = $2",
                                    new_mp, self.user_id
                                )
                                
                                # 減少物品數量
                                if item['quantity'] > 1:
                                    await db.execute(
                                        "UPDATE rpg_inventory SET quantity = quantity - 1 WHERE user_id = $1 AND item_id = $2",
                                        self.user_id, item_id
                                    )
                                else:
                                    await db.execute(
                                        "DELETE FROM rpg_inventory WHERE user_id = $1 AND item_id = $2",
                                        self.user_id, item_id
                                    )
                                
                                await interaction.response.send_message(
                                    f"🔵 使用了 {item['name']}，恢復了 {mp_amount} MP",
                                    ephemeral=True
                                )
                    
                    use_button.callback = use_callback
                    action_view.add_item(use_button)
                
                # 倉庫轉移按鈕
                if item['location'] == 'personal':
                    transfer_button = discord.ui.Button(
                        label="存到倉庫",
                        style=discord.ButtonStyle.secondary,
                        emoji="🏠"
                    )
                    
                    async def transfer_callback(interaction: discord.Interaction):
                        # 檢查倉庫容量
                        storage_count = await db.fetchval(
                            "SELECT COUNT(*) FROM rpg_inventory WHERE user_id = $1 AND location = 'storage'",
                            self.user_id
                        ) or 0
                        
                        player = await get_rpg_player(self.user_id)
                        if not player:
                            return
                        
                        if storage_count >= player['storage_capacity']:
                            await interaction.response.send_message(
                                "❌ 倉庫已滿！",
                                ephemeral=True
                            )
                            return
                        
                        # 轉移到倉庫
                        await db.execute(
                            """
                            UPDATE rpg_inventory 
                            SET location = 'storage', slot_type = 'storage'
                            WHERE user_id = $1 AND item_id = $2
                            """,
                            self.user_id, item_id
                        )
                        
                        await interaction.response.send_message(
                            f"✅ 已將 {item['name']} 存入倉庫",
                            ephemeral=True
                        )
                    
                    transfer_button.callback = transfer_callback
                    action_view.add_item(transfer_button)
                
                await interaction.response.send_message(
                    embed=embed,
                    view=action_view,
                    ephemeral=True
                )
        
        await interaction.followup.send(embed=embed, view=InventoryView(interaction.user.id, items))
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 讀取背包失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

async def create_item_embed(item):
    """創建物品詳細資訊embed"""
    rarity_colors = {
        'green': 0x00FF00,
        'blue': 0x0000FF,
        'purple': 0x800080,
        'gold': 0xFFD700,
        'normal': 0xCCCCCC
    }
    
    color = rarity_colors.get(item['rarity'], 0x7289DA)
    embed = discord.Embed(
        title=item['name'],
        description=item.get('description', '無描述'),
        color=color
    )
    
    # 基礎資訊
    rarity_text = {
        'green': '綠色',
        'blue': '藍色',
        'purple': '紫色',
        'gold': '金色',
        'normal': '普通'
    }.get(item['rarity'], '普通')
    
    embed.add_field(
        name="📊 基礎資訊",
        value=(
            f"**類型：** {item['item_type']}\n"
            f"**稀有度：** {rarity_text}\n"
            f"**等級要求：** {item['level_requirement']}\n"
            f"**數量：** {item['quantity']}"
        ),
        inline=True
    )
    
    # 屬性加成
    bonuses = []
    if item['vitality_bonus'] > 0:
        bonuses.append(f"❤️ 體力 +{item['vitality_bonus']}")
    if item['speed_bonus'] > 0:
        bonuses.append(f"⚡ 速度 +{item['speed_bonus']}")
    if item['strength_bonus'] > 0:
        bonuses.append(f"💪 力量 +{item['strength_bonus']}")
    if item['intelligence_bonus'] > 0:
        bonuses.append(f"🧠 智慧 +{item['intelligence_bonus']}")
    if item['carrying_capacity_bonus'] > 0:
        bonuses.append(f"🎒 負重 +{item['carrying_capacity_bonus']}")
    
    if bonuses:
        embed.add_field(
            name="✨ 屬性加成",
            value="\n".join(bonuses),
            inline=True
        )
    
    # 特殊詞條
    if item['special_effects']:
        effects = item['special_effects']
        if isinstance(effects, str):
            try:
                effects = json.loads(effects)
            except:
                effects = {}
        
        if effects:
            effect_text = ""
            for effect_name, effect_data in effects.items():
                if isinstance(effect_data, dict):
                    desc = effect_data.get('description', '無描述')
                    effect_text += f"• **{effect_name}**：{desc}\n"
                else:
                    effect_text += f"• **{effect_name}**\n"
            
            if effect_text:
                embed.add_field(
                    name="🌟 特殊詞條",
                    value=effect_text,
                    inline=False
                )
    
    # 武器技能
    if item['weapon_type'] and item['skill_name']:
        embed.add_field(
            name="🔥 武器技能",
            value=(
                f"**技能：** {item['skill_name']}\n"
                f"**MP消耗：** {item['skill_mp_cost']}\n"
                f"**描述：** {item.get('skill_description', '無描述')}"
            ),
            inline=False
        )
    
    # 藥水效果
    if item['potion_type']:
        potion_type_text = {
            'hp': '生命恢復',
            'mp': '魔力恢復',
            'teleport': '傳送',
            'revive': '復活'
        }.get(item['potion_type'], '未知')
        
        embed.add_field(
            name="💊 藥水效果",
            value=(
                f"**類型：** {potion_type_text}\n"
                f"**效果值：** {item['potion_value']}"
            ),
            inline=True
        )
    
    # 耐久度
    if item['max_durability']:
        durability_percent = (item['current_durability'] / item['max_durability']) * 100
        durability_bar = create_progress_bar(durability_percent, 10)
        
        embed.add_field(
            name="⚙️ 耐久度",
            value=(
                f"{durability_bar}\n"
                f"{item['current_durability']}/{item['max_durability']}"
            ),
            inline=True
        )
    
    # 價格
    if item['base_price']:
        embed.add_field(
            name="💰 價格",
            value=f"{item['base_price']} 金幣",
            inline=True
        )
    
    # 位置
    location_text = {
        'personal': '背包',
        'storage': '倉庫',
        'equipped': '已裝備'
    }.get(item['location'], '未知')
    
    embed.add_field(
        name="📍 位置",
        value=location_text,
        inline=True
    )
    
    return embed

# ========== 商店系統 ==========

@tree.command(name="rpg_shop", description="商店系統")
async def rpg_shop_slash(interaction: discord.Interaction):
    """商店系統"""
    await interaction.response.defer()
    
    try:
        if not db.is_connected:
            embed = discord.Embed(
                title="⚠️ 資料庫未連接",
                description="無法訪問商店，請稍後再試。",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        player = await get_rpg_player(interaction.user.id)
        if not player:
            embed = discord.Embed(
                title="❌ 角色不存在",
                description="請先使用 `/rpg_start` 創建角色",
                color=0xFF0000
            )
            await interaction.followup.send(embed=embed)
            return
        
        if not player['is_in_town']:
            embed = discord.Embed(
                title="❌ 不在城鎮",
                description="請先返回城鎮才能訪問商店",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        # 獲取商店物品
        shop_items = await db.fetch(
            '''
            SELECT 
                s.*,
                i.name, i.item_type, i.rarity,
                i.level_requirement, i.description
            FROM rpg_shops s
            JOIN rpg_items i ON s.item_id = i.id
            WHERE s.shop_type = 'general' 
            AND (s.stock > 0 OR s.stock = -1)
            ORDER BY i.rarity DESC, i.level_requirement
            '''
        )
        
        embed = discord.Embed(
            title="🛒 阿爾比恩商店",
            description="歡迎光臨！這裡有各種冒險必需品",
            color=0x00FF00
        )
        
        embed.add_field(
            name="💰 你的金幣",
            value="金幣系統開發中...",
            inline=True
        )
        
        embed.add_field(
            name="🏪 商店類別",
            value=(
                "**綜合商店** - 基礎物品\n"
                "**鐵匠鋪** - 武器裝備\n"
                "**藥水店** - 恢復道具\n"
                "**拍賣行** - 玩家交易"
            ),
            inline=False
        )
        
        # 顯示部分商品
        if shop_items:
            items_text = ""
            for i, item in enumerate(shop_items[:5], 1):
                rarity_emoji = {
                    'green': '🟢',
                    'blue': '🔵',
                    'purple': '🟣',
                    'gold': '🟡',
                    'normal': '⚪'
                }.get(item['rarity'], '⚪')
                
                stock_text = "無限" if item['stock'] == -1 else f"{item['stock']}個"
                items_text += f"{i}. {rarity_emoji} **{item['name']}** - {item['price']}金 ({stock_text})\n"
            
            embed.add_field(
                name="📦 商品列表 (前5項)",
                value=items_text,
                inline=False
            )
        
        embed.set_footer(text="商店系統開發中，敬請期待更多功能！")
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 訪問商店失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

# ========== 組隊系統 ==========

@tree.command(name="rpg_party", description="組隊系統")
async def rpg_party_slash(interaction: discord.Interaction):
    """組隊系統"""
    await interaction.response.defer()
    
    try:
        if not db.is_connected:
            embed = discord.Embed(
                title="⚠️ 資料庫未連接",
                description="無法使用組隊系統，請稍後再試。",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        player = await get_rpg_player(interaction.user.id)
        if not player:
            embed = discord.Embed(
                title="❌ 角色不存在",
                description="請先使用 `/rpg_start` 創建角色",
                color=0xFF0000
            )
            await interaction.followup.send(embed=embed)
            return
        
        # 檢查是否已在隊伍中
        party_info = await db.fetchrow(
            '''
            SELECT 
                p.*,
                pm.role
            FROM rpg_party_members pm
            JOIN rpg_parties p ON pm.party_id = p.id
            WHERE pm.user_id = $1 AND p.status != 'expired'
            ''',
            interaction.user.id
        )
        
        if party_info:
            # 顯示隊伍資訊
            members = await db.fetch(
                '''
                SELECT 
                    pm.user_id,
                    pm.role,
                    pm.is_ready,
                    rp.nickname
                FROM rpg_party_members pm
                JOIN rpg_players rp ON pm.user_id = rp.user_id
                WHERE pm.party_id = $1
                ORDER BY pm.joined_at
                ''',
                party_info['id']
            )
            
            embed = discord.Embed(
                title="👥 隊伍資訊",
                description=f"隊伍ID: {party_info['id']}",
                color=0x7289DA
            )
            
            embed.add_field(
                name="📊 隊伍狀態",
                value=(
                    f"**狀態：** {party_info['status']}\n"
                    f"**人數：** {party_info['current_size']}/{party_info['max_size']}\n"
                    f"**地圖：** {party_info['current_map']}\n"
                    f"**層數：** {party_info['current_layer']}"
                ),
                inline=False
            )
            
            members_text = ""
            for member in members:
                ready_emoji = "✅" if member['is_ready'] else "❌"
                members_text += f"• {member['nickname']} ({member['role']}) {ready_emoji}\n"
            
            embed.add_field(
                name="👤 隊伍成員",
                value=members_text,
                inline=False
            )
            
            # 添加隊伍操作按鈕
            class PartyView(discord.ui.View):
                def __init__(self, user_id, party_id, is_leader):
                    super().__init__(timeout=60)
                    self.user_id = user_id
                    self.party_id = party_id
                    self.is_leader = is_leader
                
                @discord.ui.button(label="準備/取消", style=discord.ButtonStyle.primary, emoji="✅")
                async def toggle_ready(self, interaction: discord.Interaction, button: discord.ui.Button):
                    if interaction.user.id != self.user_id:
                        await interaction.response.send_message("❌ 這不是你的隊伍！", ephemeral=True)
                        return
                    
                    # 切換準備狀態
                    current = await db.fetchval(
                        "SELECT is_ready FROM rpg_party_members WHERE party_id = $1 AND user_id = $2",
                        self.party_id, self.user_id
                    )
                    
                    new_status = not current
                    await db.execute(
                        "UPDATE rpg_party_members SET is_ready = $1 WHERE party_id = $2 AND user_id = $3",
                        new_status, self.party_id, self.user_id
                    )
                    
                    status_text = "已準備" if new_status else "未準備"
                    await interaction.response.send_message(
                        f"✅ 你現在{status_text}",
                        ephemeral=True
                    )
                
                @discord.ui.button(label="離開隊伍", style=discord.ButtonStyle.danger, emoji="❌")
                async def leave_party(self, interaction: discord.Interaction, button: discord.ui.Button):
                    if interaction.user.id != self.user_id:
                        await interaction.response.send_message("❌ 這不是你的隊伍！", ephemeral=True)
                        return
                    
                    # 離開隊伍
                    await db.execute(
                        "DELETE FROM rpg_party_members WHERE party_id = $1 AND user_id = $2",
                        self.party_id, self.user_id
                    )
                    
                    # 更新隊伍人數
                    await db.execute(
                        "UPDATE rpg_parties SET current_size = current_size - 1 WHERE id = $1",
                        self.party_id
                    )
                    
                    # 檢查隊伍是否還有人
                    remaining = await db.fetchval(
                        "SELECT COUNT(*) FROM rpg_party_members WHERE party_id = $1",
                        self.party_id
                    ) or 0
                    
                    if remaining == 0:
                        # 解散隊伍
                        await db.execute(
                            "UPDATE rpg_parties SET status = 'expired' WHERE id = $1",
                            self.party_id
                        )
                    
                    await interaction.response.send_message(
                        "✅ 你已離開隊伍",
                        ephemeral=True
                    )
                    
                    embed = discord.Embed(
                        title="👋 離開隊伍",
                        description="你已成功離開隊伍",
                        color=0x00FF00
                    )
                    await interaction.followup.send(embed=embed, ephemeral=True)
                
                @discord.ui.button(label="開始探索", style=discord.ButtonStyle.success, emoji="🏃‍♂️")
                async def start_exploration(self, interaction: discord.Interaction, button: discord.ui.Button):
                    if interaction.user.id != self.user_id:
                        await interaction.response.send_message("❌ 這不是你的隊伍！", ephemeral=True)
                        return
                    
                    if not self.is_leader:
                        await interaction.response.send_message("❌ 只有隊長可以開始探索！", ephemeral=True)
                        return
                    
                    # 檢查所有成員是否準備
                    members = await db.fetch(
                        "SELECT user_id, is_ready FROM rpg_party_members WHERE party_id = $1",
                        self.party_id
                    )
                    
                    not_ready = [member for member in members if not member['is_ready']]
                    if not_ready:
                        not_ready_names = []
                        for member in not_ready:
                            player = await get_rpg_player(member['user_id'])
                            if player:
                                not_ready_names.append(player['nickname'])
                        
                        await interaction.response.send_message(
                            f"❌ 以下成員尚未準備：{', '.join(not_ready_names)}",
                            ephemeral=True
                        )
                        return
                    
                    # 開始探索
                    await db.execute(
                        "UPDATE rpg_parties SET status = 'exploring' WHERE id = $1",
                        self.party_id
                    )
                    
                    await interaction.response.send_message(
                        "✅ 隊伍開始探索！",
                        ephemeral=True
                    )
            
            is_leader = party_info['leader_id'] == interaction.user.id
            view = PartyView(interaction.user.id, party_info['id'], is_leader)
            
            await interaction.followup.send(embed=embed, view=view)
            
        else:
            # 創建隊伍視圖
            embed = discord.Embed(
                title="👥 組隊系統",
                description="與其他冒險者組隊進行更困難的挑戰",
                color=0x7289DA
            )
            
            embed.add_field(
                name="📊 隊伍好處",
                value=(
                    "• 挑戰更高難度副本\n"
                    "• 獲得更多經驗值\n"
                    "• 掉落稀有裝備機率增加\n"
                    "• 隊伍技能加成"
                ),
                inline=False
            )
            
            embed.add_field(
                name="🎮 職業分工",
                value=(
                    "**坦克 🛡️** - 吸引怪物，保護隊友\n"
                    "**輸出 ⚔️** - 造成大量傷害\n"
                    "**治療 💚** - 回復隊友生命值\n"
                    "**輔助 💛** - 提供增益效果"
                ),
                inline=False
            )
            
            class PartyCreationView(discord.ui.View):
                def __init__(self, user_id):
                    super().__init__(timeout=60)
                    self.user_id = user_id
                
                @discord.ui.button(label="創建隊伍", style=discord.ButtonStyle.primary, emoji="👑")
                async def create_party(self, interaction: discord.Interaction, button: discord.ui.Button):
                    if interaction.user.id != self.user_id:
                        await interaction.response.send_message("❌ 這不是你的操作！", ephemeral=True)
                        return
                    
                    # 檢查是否已有隊伍
                    existing = await db.fetchrow(
                        "SELECT id FROM rpg_party_members WHERE user_id = $1",
                        self.user_id
                    )
                    
                    if existing:
                        await interaction.response.send_message("❌ 你已經在隊伍中！", ephemeral=True)
                        return
                    
                    # 創建隊伍
                    party_id = await db.fetchval(
                        '''
                        INSERT INTO rpg_parties (
                            leader_id, current_size, max_size, status,
                            current_map, current_layer, expires_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                        RETURNING id
                        ''',
                        self.user_id, 1, 5, 'recruiting',
                        '新手森林', 1, datetime.now() + timedelta(hours=2)
                    )
                    
                    # 添加自己到隊伍
                    await db.execute(
                        '''
                        INSERT INTO rpg_party_members (party_id, user_id, role)
                        VALUES ($1, $2, $3)
                        ''',
                        party_id, self.user_id, 'dps'  # 預設為輸出
                    )
                    
                    embed = discord.Embed(
                        title="✅ 隊伍創建成功",
                        description=f"隊伍ID: {party_id}",
                        color=0x00FF00
                    )
                    
                    embed.add_field(
                        name="📝 隊伍資訊",
                        value=(
                            "**隊長：** 你\n"
                            "**人數：** 1/5\n"
                            "**狀態：** 招募中\n"
                            "**預設地圖：** 新手森林"
                        ),
                        inline=False
                    )
                    
                    await interaction.response.edit_message(embed=embed, view=None)
                
                @discord.ui.button(label="搜尋隊伍", style=discord.ButtonStyle.secondary, emoji="🔍")
                async def search_party(self, interaction: discord.Interaction, button: discord.ui.Button):
                    if interaction.user.id != self.user_id:
                        await interaction.response.send_message("❌ 這不是你的操作！", ephemeral=True)
                        return
                    
                    # 搜尋可加入的隊伍
                    parties = await db.fetch(
                        '''
                        SELECT 
                            p.id,
                            p.current_size,
                            p.max_size,
                            p.current_map,
                            rp.nickname as leader_name,
                            COUNT(pm.user_id) as member_count
                        FROM rpg_parties p
                        JOIN rpg_players rp ON p.leader_id = rp.user_id
                        LEFT JOIN rpg_party_members pm ON p.id = pm.party_id
                        WHERE p.status = 'recruiting'
                        AND p.current_size < p.max_size
                        AND p.expires_at > NOW()
                        GROUP BY p.id, rp.nickname
                        ORDER BY p.created_at DESC
                        LIMIT 10
                        '''
                    )
                    
                    if not parties:
                        embed = discord.Embed(
                            title="🔍 搜尋隊伍",
                            description="目前沒有可加入的隊伍",
                            color=0xFFA500
                        )
                        await interaction.response.send_message(embed=embed, ephemeral=True)
                        return
                    
                    embed = discord.Embed(
                        title="🔍 可加入的隊伍",
                        description="選擇一個隊伍加入：",
                        color=0x7289DA
                    )
                    
                    for i, party in enumerate(parties, 1):
                        embed.add_field(
                            name=f"隊伍 {i}",
                            value=(
                                f"**ID：** {party['id']}\n"
                                f"**隊長：** {party['leader_name']}\n"
                                f"**人數：** {party['current_size']}/{party['max_size']}\n"
                                f"**地圖：** {party['current_map']}"
                            ),
                            inline=False
                        )
                    
                    await interaction.response.send_message(embed=embed, ephemeral=True)
            
            view = PartyCreationView(interaction.user.id)
            await interaction.followup.send(embed=embed, view=view)
            
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 組隊系統錯誤",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

# ========== 鍛造系統 ==========

@tree.command(name="rpg_forge", description="鍛造裝備")
async def rpg_forge_slash(interaction: discord.Interaction):
    """鍛造裝備"""
    await interaction.response.defer()
    
    try:
        if not db.is_connected:
            embed = discord.Embed(
                title="⚠️ 資料庫未連接",
                description="無法使用鍛造系統，請稍後再試。",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        player = await get_rpg_player(interaction.user.id)
        if not player:
            embed = discord.Embed(
                title="❌ 角色不存在",
                description="請先使用 `/rpg_start` 創建角色",
                color=0xFF0000
            )
            await interaction.followup.send(embed=embed)
            return
        
        house_info = RPG_CONFIG['HOUSES'].get(player['house_type'], {})
        unlocks = house_info.get('unlocks', [])
        
        has_forge = False
        forge_level = 0
        
        for unlock in unlocks:
            if unlock.startswith('workshop_lv'):
                has_forge = True
                forge_level = int(unlock.split('lv')[1])
                break
        
        if not has_forge:
            embed = discord.Embed(
                title="❌ 沒有鍛造坊",
                description=(
                    "你的房屋沒有鍛造坊\n\n"
                    "**可解鎖鍛造坊的房屋：**\n"
                    "• 領地 (workshop_lv1)\n"
                    "• 城堡 (workshop_lv2)"
                ),
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        # 獲取可鍛造物品
        forge_config = RPG_CONFIG['FORGING'][f'workshop_lv{forge_level}']
        
        embed = discord.Embed(
            title="⚒️ 鍛造坊",
            description=f"等級 {forge_level} 鍛造坊",
            color=0xCD7F32
        )
        
        embed.add_field(
            name="📊 可鍛造稀有度",
            value=(
                f"**武器：** {', '.join(forge_config['weapons'])}\n"
                f"**防具：** {', '.join(forge_config['armors'])}\n"
                f"**飾品：** {', '.join(forge_config['accessories'])}"
            ),
            inline=False
        )
        
        embed.add_field(
            name="💰 所需材料",
            value=(
                "鍛造需要對應的素材\n"
                "素材可以通過擊敗怪物獲得\n"
                "稀有度越高，需要的素材越多"
            ),
            inline=False
        )
        
        embed.add_field(
            name="⚙️ 鍛造機率",
            value=(
                "**成功機率：**\n"
                "• 綠色：90%\n"
                "• 藍色：70%\n"
                "• 紫色：50%\n"
                "• 金色：30%"
            ),
            inline=True
        )
        
        # 檢查玩家素材
        materials = await db.fetch(
            '''
            SELECT 
                i.id, i.name, i.rarity,
                inv.quantity
            FROM rpg_inventory inv
            JOIN rpg_items i ON inv.item_id = i.id
            WHERE inv.user_id = $1 
            AND i.item_type = 'material'
            AND inv.slot_type = 'inventory'
            ''',
            interaction.user.id
        )
        
        if materials:
            materials_text = ""
            for material in materials[:10]:
                rarity_emoji = {
                    'green': '🟢',
                    'blue': '🔵',
                    'purple': '🟣',
                    'gold': '🟡'
                }.get(material['rarity'], '⚪')
                
                materials_text += f"{rarity_emoji} {material['name']} x{material['quantity']}\n"
            
            embed.add_field(
                name="📦 擁有的素材",
                value=materials_text,
                inline=True
            )
        
        embed.set_footer(text="鍛造系統開發中，敬請期待完整功能！")
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 鍛造系統錯誤",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

# ========== 成就系統 ==========

@tree.command(name="rpg_achievements", description="查看成就")
async def rpg_achievements_slash(interaction: discord.Interaction):
    """查看成就"""
    await interaction.response.defer()
    
    try:
        if not db.is_connected:
            embed = discord.Embed(
                title="⚠️ 資料庫未連接",
                description="無法讀取成就，請稍後再試。",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        player = await get_rpg_player(interaction.user.id)
        if not player:
            embed = discord.Embed(
                title="❌ 角色不存在",
                description="請先使用 `/rpg_start` 創建角色",
                color=0xFF0000
            )
            await interaction.followup.send(embed=embed)
            return
        
        # 獲取成就
        achievements = await db.fetch(
            '''
            SELECT 
                achievement_id, achievement_name,
                progress, target, completed,
                completed_at, reward
            FROM rpg_achievements
            WHERE user_id = $1
            ORDER BY 
                completed DESC,
                progress DESC,
                achievement_name
            ''',
            interaction.user.id
        )
        
        embed = discord.Embed(
            title="🏆 成就系統",
            description="完成成就獲得豐厚獎勵！",
            color=0xFFD700
        )
        
        if not achievements:
            embed.add_field(
                name="📝 成就列表",
                value="尚未解鎖任何成就",
                inline=False
            )
        else:
            completed = [a for a in achievements if a['completed']]
            in_progress = [a for a in achievements if not a['completed']]
            
            if completed:
                completed_text = ""
                for achievement in completed[:5]:
                    completed_text += f"✅ **{achievement['achievement_name']}**\n"
                
                embed.add_field(
                    name=f"✅ 已完成 ({len(completed)})",
                    value=completed_text,
                    inline=False
                )
            
            if in_progress:
                in_progress_text = ""
                for achievement in in_progress[:5]:
                    progress_percent = (achievement['progress'] / achievement['target']) * 100
                    progress_bar = create_progress_bar(progress_percent, 10)
                    in_progress_text += f"📊 **{achievement['achievement_name']}**\n{progress_bar} {achievement['progress']}/{achievement['target']}\n"
                
                embed.add_field(
                    name=f"📊 進行中 ({len(in_progress)})",
                    value=in_progress_text,
                    inline=False
                )
        
        # 預定義成就
        predefined_achievements = [
            ("first_kill", "第一滴血", "擊敗第一隻怪物", 1, {"exp": 100, "gold": 50}),
            ("level_10", "初出茅廬", "達到10級", 10, {"exp": 500, "gold": 200}),
            ("level_50", "經驗豐富", "達到50級", 50, {"exp": 5000, "gold": 1000}),
            ("monster_slayer", "怪物殺手", "擊敗100隻怪物", 100, {"exp": 1000, "gold": 500}),
            ("explorer", "探險家", "探索所有地圖", 4, {"exp": 2000, "gold": 1000}),
            ("collector", "收藏家", "收集10件金色裝備", 10, {"exp": 5000, "gold": 5000}),
            ("rich", "富翁", "累積獲得10000金幣", 10000, {"exp": 10000, "gold": 10000}),
            ("party_player", "團隊玩家", "組隊完成10次探索", 10, {"exp": 2000, "gold": 1000}),
            ("craftsman", "工匠大師", "鍛造50件裝備", 50, {"exp": 3000, "gold": 2000}),
            ("survivor", "生存專家", "連續10次探索無死亡", 10, {"exp": 1500, "gold": 800})
        ]
        
        embed.add_field(
            name="🎯 可達成成就",
            value="\n".join([f"• {name}: {desc}" for _, name, desc, _, _ in predefined_achievements[:5]]),
            inline=False
        )
        
        embed.set_footer(text="成就系統開發中，更多成就即將推出！")
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 讀取成就失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

# ========== 自動存檔機制 ==========

async def auto_save_data():
    """自動存檔數據"""
    print("💾 自動存檔中...")
    
    try:
        if db.is_connected:
            # 備份重要數據到JSON文件
            backup_data = {
                "users": [],
                "prizes": [],
                "giveaways": [],
                "events": [],
                "timestamp": datetime.now().isoformat()
            }
            
            # 備份用戶數據
            users = await db.fetch("SELECT * FROM users LIMIT 1000")
            for user in users:
                backup_data["users"].append(dict(user))
            
            # 備份獎品數據
            prizes = await db.fetch("SELECT * FROM prize_pool LIMIT 1000")
            for prize in prizes:
                backup_data["prizes"].append(dict(prize))
            
            # 創建備份文件
            backup_dir = "backups"
            if not os.path.exists(backup_dir):
                os.makedirs(backup_dir)
            
            backup_file = f"{backup_dir}/backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            with open(backup_file, 'w', encoding='utf-8') as f:
                json.dump(backup_data, f, ensure_ascii=False, indent=2, default=str)
            
            print(f"✅ 自動存檔完成: {backup_file}")
            
    except Exception as e:
        print(f"❌ 自動存檔失敗: {e}")

async def periodic_tasks():
    """定期任務"""
    print("🔄 啟動定期任務...")
    
    while True:
        try:
            # 每30分鐘自動存檔一次
            await asyncio.sleep(1800)  # 30分鐘
            await auto_save_data()
            
        except Exception as e:
            print(f"❌ 定期任務錯誤: {e}")
            await asyncio.sleep(60)  # 錯誤後等待1分鐘

# ========== 啟動任務 ==========

@bot.event
async def on_connect():
    """機器人連接時啟動定期任務"""
    print("🔗 機器人已連接，啟動定期任務...")
    asyncio.create_task(periodic_tasks())

# ========== 主程式啟動 ==========

if __name__ == "__main__":
    token = os.getenv('DISCORD_TOKEN')
    
    if not token:
        print("❌ 找不到 DISCORD_TOKEN 環境變數！")
        print("💡 請設定環境變數：")
        print("   1. 在 Railway 中設定 DISCORD_TOKEN")
        print("   2. 或在 .env 檔案中添加 DISCORD_TOKEN=your_bot_token")
        sys.exit(1)
    
    print("🤖 正在啟動機器人...")
    print(f"📋 機器人名稱: {BOT_NAME}")
    print(f"👑 擁有者ID: {OWNER_IDS}")
    print(f"🎮 RPG 地圖數: {len(RPG_CONFIG['MAPS'])}")
    print(f"⚔️ 武器技能數: {sum(len(skills['技能']) for skills in RPG_CONFIG['WEAPON_SKILLS'].values())}")
    
    try:
        bot.run(token)
    except KeyboardInterrupt:
        print("\n🛑 機器人正在關閉...")
    except Exception as e:
        print(f"❌ 機器人啟動失敗: {e}")
        traceback.print_exc()

# 在你的 Bot.py 中新增：
from database import RPGDatabase
from monsters import MonsterSystem
from items import ItemSystem
from combat import CombatSystem
from party import PartySystem

class Bot:
    def __init__(self):
        # ... 現有代碼 ...
        
        # 新增 RPG 系統
        self.rpg_db = RPGDatabase()
        self.rpg_monsters = MonsterSystem(self.rpg_db)
        self.rpg_items = ItemSystem(self.rpg_db)
        self.rpg_combat = CombatSystem(self.rpg_db)
        self.rpg_party = PartySystem(self.rpg_db)
        
    def response(self, user_id: str, msg: str) -> str:
        # ... 現有代碼 ...
        
        # 添加 RPG 指令處理
        if msg.startswith("!rpg"):
            return self.handle_rpg_command(user_id, msg)
            
    def handle_rpg_command(self, user_id: str, msg: str) -> str:
        """處理 RPG 指令"""
        parts = msg.split()
        if len(parts) < 2:
            return "RPG 指令：!rpg start, !rpg status, !rpg party, !rpg adventure"
            
        command = parts[1].lower()
        
        if command == "start":
            return self.rpg_start(user_id)
        elif command == "status":
            return self.rpg_status(user_id)
        elif command == "party":
            return self.rpg_party_status(user_id)
        elif command == "adventure":
            return self.rpg_adventure(user_id)
        else:
            return "未知的 RPG 指令"






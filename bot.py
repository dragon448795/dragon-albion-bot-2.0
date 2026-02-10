#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
小雲ALBION機械人 - PostgreSQL完整版本
包含所有13個原指令 + 新增/blessing指令 + 聊天積分系統
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
                
            print("✅ 資料庫表格初始化完成")
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

# ========== RPG 系統接口 ==========
# 這裡導入分割後的 RPG 系統
try:
    from rpg.player import RPGPlayer
    from rpg.commands import RPGCommands
    print("✅ RPG 系統已成功導入")
except ImportError as e:
    print(f"⚠️ 無法導入 RPG 系統: {e}")
    print("⚠️ 請確保 rpg/ 目錄存在且包含必要的檔案")
    RPGPlayer = None
    RPGCommands = None

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

# ========== RPG 系統初始化 ==========
rpg_player = None
rpg_commands = None

def init_rpg_system():
    """初始化 RPG 系統"""
    global rpg_player, rpg_commands
    
    if not db.is_connected:
        print("⚠️ 資料庫未連接，無法初始化 RPG 系統")
        return False
    
    try:
        # 初始化 RPG 玩家系統
        rpg_player = RPGPlayer(db.pool)
        
        # 初始化 RPG 指令系統
        rpg_commands = RPGCommands(bot, rpg_player)
        rpg_commands.setup_commands()
        
        print("✅ RPG 系統初始化成功")
        return True
        
    except Exception as e:
        print(f"❌ RPG 系統初始化失敗: {e}")
        traceback.print_exc()
        return False

# ========== 現有指令 (保持不變) ==========

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

@tree.command(name="help", description="顯示幫助訊息")
async def help_slash(interaction: discord.Interaction):
    """顯示幫助"""
    embed = discord.Embed(
        title="🤖 小雲機械人 - 幫助中心",
        description="以下是可用指令列表：",
        color=0x7289DA
    )
    
    # 檢查 RPG 系統是否可用
    rpg_status = "✅ 可用" if rpg_player else "⚠️ 暫不可用"
    
    embed.add_field(
        name="🎮 RPG 指令",
        value=(
            "`/rpg_start` - 開始 RPG 冒險\n"
            "`/rpg_status` - 查看角色狀態\n"
            "`/rpg_inventory` - 查看背包\n"
            "`/rpg_equipment` - 查看裝備\n"
            "`/rpg_help` - RPG 系統幫助"
        ),
        inline=False
    )
    
    embed.add_field(
        name="👤 用戶指令",
        value=(
            "`/help` - 顯示此幫助訊息\n"
            "`/profile` - 查看我的數據\n"
            "`/giveaway` - 創建抽獎\n"
            "`/score_draw` - 使用積分抽獎\n"
            "`/score_transfer` - 轉移積分\n"
            "`/prizelist` - 查看彩池列表\n"
            "`/random_team` - 隨機分組\n"
            "`/score_ranking` - 查看積分排行榜\n"
            "`/attendance_ranking` - 查看出席率排行榜\n"
            "`/blessing` - 測試今日運程\n"
            "`/test_chat_score` - 測試聊天積分"
        ),
        inline=False
    )
    
    embed.add_field(
        name="🛠️ 管理員指令",
        value=(
            "`/add_prize` - 調整彩池\n"
            "`/add_score` - 加減積分\n"
            "`/create_event` - 創建評核活動\n"
            "`/activity_stats` - 查看活動統計"
        ),
        inline=False
    )
    
    embed.add_field(
        name="🔧 系統指令",
        value=(
            "`/sync` - 同步指令（擁有者）\n"
            "`/db_status` - 檢查資料庫狀態"
        ),
        inline=False
    )
    
    db_status = "✅ 正常" if db.is_connected else "⚠️ 使用緩存"
    embed.set_footer(text=f"RPG 系統: {rpg_status} | 資料庫狀態: {db_status}")
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
        
        # 初始化 RPG 系統
        if connected:
            rpg_initialized = init_rpg_system()
            if not rpg_initialized:
                print("⚠️ RPG 系統初始化失敗")
        
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

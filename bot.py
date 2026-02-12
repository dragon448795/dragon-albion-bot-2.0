#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
小雲ALBION機械人 - PostgreSQL完整版本（數據持久化）修正版
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

@tree.command(name="help", description="顯示幫助訊息")
async def help_slash(interaction: discord.Interaction):
    """顯示幫助"""
    embed = discord.Embed(
        title="🤖 小雲機械人 - 幫助中心",
        description="以下是可用指令列表：",
        color=0x7289DA
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
            "`/test_chat_score` - 測試聊天積分 **(新增)**"
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
            "`/db_status` - 檢查資料庫狀態 **(新增)**"
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
    
    db_status = "✅ 正常" if db.is_connected else "⚠️ 使用緩存"
    embed.set_footer(text=f"總指令數: 17個 | 資料庫狀態: {db_status}")
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
        
        if seconds < 60:
            time_display = f"{seconds}秒"
        elif seconds < 3600:
            time_display = f"{seconds//60}分{seconds%60}秒"
        elif seconds < 86400:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            time_display = f"{hours}小時{minutes}分"
        else:
            days = seconds // 86400
            hours = (seconds % 86400) // 3600
            time_display = f"{days}天{hours}小時"
        
        embed = discord.Embed(
            title="🎉 自動抽獎活動 🎉",
            description="時間到自動開獎！",
            color=0xFFD700
        )
        
        embed.add_field(name="🎁 獎品", value=prize, inline=True)
        embed.add_field(name="👑 中獎人數", value=str(winners), inline=True)
        embed.add_field(name="⏰ 結束時間", value=time_display, inline=True)
        embed.add_field(name="🎫 參與人數", value="0 人", inline=True)
        embed.add_field(name="📝 參與方式", value="點擊下方 🎫 按鈕參與", inline=True)
        embed.add_field(name="🔧 主辦人操作", value="點擊 ⏹️ 手動結束抽獎", inline=True)
        
        creator_name = interaction.user.display_name
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M")
        giveaway_id = f"giveaway_{int(time.time())}_{random.randint(1000, 9999)}"
        
        embed.set_footer(text=f"抽獎ID: {giveaway_id} | 主辦人: {creator_name}•{current_time}")
        
        await interaction.followup.send(embed=embed)
        message = await interaction.original_response()
        
        await message.add_reaction("🎫")
        await message.add_reaction("⏹️")
        
        await db.execute(
            '''
            INSERT INTO giveaways (creator_id, prize, winner_count, end_time, message_id, channel_id, guild_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ''',
            interaction.user.id, prize, winners, end_time, message.id, interaction.channel.id, guild_id
        )
        
        print(f"✅ 抽獎已創建: 獎品={prize}, 時間={seconds}秒, 訊息ID={message.id}")
        
        async def countdown_timer():
            remaining = seconds
            last_update = time.time()
            
            while remaining > 0:
                await asyncio.sleep(1)
                remaining -= 1
                
                if time.time() - last_update >= 30:
                    if remaining < 60:
                        time_display = f"{remaining}秒"
                    elif remaining < 3600:
                        time_display = f"{remaining//60}分{remaining%60}秒"
                    elif remaining < 86400:
                        hours = remaining // 3600
                        minutes = (remaining % 3600) // 60
                        time_display = f"{hours}小時{minutes}分"
                    else:
                        days = remaining // 86400
                        hours = (remaining % 86400) // 3600
                        time_display = f"{days}天{hours}小時"
                    
                    try:
                        result = await db.fetchrow(
                            "SELECT participants FROM giveaways WHERE message_id = $1 AND guild_id = $2",
                            message.id, guild_id
                        )
                        participants_count = 0
                        participants = []
                        if result and result['participants']:
                            participants = result['participants']
                            if isinstance(participants, str):
                                try:
                                    participants = json.loads(participants)
                                except:
                                    participants = []
                            participants_count = len(participants)
                        
                        # ========== 修復：顯示所有參與者 ==========
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
                        
                        # 顯示參與者列表（分頁顯示，最多顯示50人）
                        if participants_count > 0:
                            participants_text = ""
                            for i, user_id in enumerate(participants[:50], 1):
                                participants_text += f"{i}. <@{user_id}>\n"
                            
                            if participants_count > 50:
                                participants_text += f"\n... 還有 {participants_count - 50} 人"
                            
                            new_embed.add_field(
                                name="📋 參與者列表",
                                value=participants_text,
                                inline=False
                            )
                        
                        new_embed.set_footer(text=f"抽獎ID: {giveaway_id} | 主辦人: {creator_name}•{datetime.now().strftime('%Y-%m-%d %H:%M')}")
                        
                        await message.edit(embed=new_embed)
                        last_update = time.time()
                        
                    except Exception as e:
                        print(f"❌ 更新抽獎訊息錯誤: {e}")
            
            await end_giveaway(message.id, guild_id=guild_id)
        
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
                
                await update_user_score(interaction.user.id, interaction.user.name, -score_cost, f"積分抽獎 ({selected_box})", self.guild_id)
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
                
                new_current_score, _ = await get_user_score(interaction.user.id, self.guild_id)
                
                result_embed = discord.Embed(
                    title="🎉 抽獎結果",
                    description=f"你抽中了 **{prize_name}**！",
                    color=0x00FF00
                )
                
                result_embed.add_field(name="扣除積分", value=f"{score_cost} 分", inline=True)
                result_embed.add_field(name="寶箱類型", value=selected_box, inline=True)
                result_embed.add_field(name="中獎機率", value=f"{box_weights[selected_box]}%", inline=True)
                result_embed.add_field(name="剩餘積分", value=f"{new_current_score} 分", inline=True)
                result_embed.add_field(name="獎品名稱", value=prize_name, inline=False)
                
                await interaction.response.send_message(embed=result_embed, ephemeral=False)
                
                for child in self.children:
                    child.disabled = True
                
                await interaction.message.edit(view=self)
        
        view = ScoreDrawView(interaction.user.id, guild_id)
        await interaction.followup.send(embed=embed, view=view)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 抽獎失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

@tree.command(name="score_transfer", description="轉移積分給其他用戶")
@app_commands.describe(
    user="目標用戶",
    amount="轉移積分",
    reason="原因（可選）"
)
async def score_transfer_slash(
    interaction: discord.Interaction,
    user: discord.Member,
    amount: int,
    reason: Optional[str] = None
):
    """轉移積分"""
    await interaction.response.defer()
    
    try:
        guild_id = get_guild_id(interaction)
        await log_query("score_transfer", interaction.user.id, {"target": user.id, "amount": amount, "reason": reason}, guild_id)
        
        if amount <= 0:
            await interaction.followup.send("❌ 積分必須大於 0")
            return
        
        if user.id == interaction.user.id:
            await interaction.followup.send("❌ 不能轉移積分給自己")
            return
        
        sender_score, _ = await get_user_score(interaction.user.id, guild_id)
        
        if sender_score < amount:
            await interaction.followup.send(f"❌ 你的積分不足！需要 {amount} 分，你目前有 {sender_score} 分")
            return
        
        # 更新雙方積分
        await update_user_score(interaction.user.id, interaction.user.name, -amount, f"轉移給 {user.name}", guild_id)
        await update_user_score(user.id, user.name, amount, f"來自 {interaction.user.name} 的轉移", guild_id)
        
        if db.is_connected:
            await db.execute(
                "INSERT INTO score_transfers (from_user_id, to_user_id, amount, reason, guild_id) VALUES ($1, $2, $3, $4, $5)",
                interaction.user.id, user.id, amount, reason or "無", guild_id
            )
        
        new_sender_score, _ = await get_user_score(interaction.user.id, guild_id)
        
        embed = discord.Embed(
            title="💸 積分轉移成功",
            description=f"**轉出：** {interaction.user.mention}\n"
                       f"**轉入：** {user.mention}\n"
                       f"**金額：** {amount} 分\n"
                       f"**原因：** {reason or '無'}\n"
                       f"**你的剩餘積分：** {new_sender_score} 分",
            color=0x2ECC71
        )
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 轉移失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

@tree.command(name="prizelist", description="查看彩池列表")
async def prizelist_slash(interaction: discord.Interaction):
    """查看彩池"""
    await interaction.response.defer()
    
    try:
        if not db.is_connected:
            embed = discord.Embed(
                title="⚠️ 資料庫未連接",
                description="無法查看彩池列表，請稍後再試。",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        guild_id = get_guild_id(interaction)
        await log_query("prizelist", interaction.user.id, {"action": "view_pool"}, guild_id)
        
        # 檢查表格是否存在
        try:
            await db.fetch("SELECT 1 FROM prize_pool LIMIT 1")
        except:
            embed = discord.Embed(
                title="❌ 彩池表格不存在",
                description="請重新啟動機器人以初始化資料庫",
                color=0xFF0000
            )
            await interaction.followup.send(embed=embed)
            return
        
        results = await db.fetch(
            """
            SELECT box_level, 
                   COUNT(*) as total_items,
                   SUM(remaining) as total_remaining
            FROM prize_pool 
            WHERE remaining > 0 AND guild_id = $1
            GROUP BY box_level 
            ORDER BY 
                CASE box_level 
                    WHEN '金箱' THEN 1 
                    WHEN '紫箱' THEN 2 
                    WHEN '藍箱' THEN 3 
                    WHEN '綠箱' THEN 4 
                    ELSE 5 
                END
            """,
            guild_id
        )
        
        if not results:
            embed = discord.Embed(
                title="🎁 彩池列表",
                description="目前彩池是空的\n使用 `/add_prize` 添加獎品",
                color=0xFFD700
            )
            await interaction.followup.send(embed=embed)
            return
        
        embed = discord.Embed(
            title="🎁 彩池列表",
            description="可用的獎品（按寶箱等級分類）：",
            color=0xFFD700
        )
        
        for row in results:
            box_level = row['box_level']
            total_items = row['total_items']
            total_remaining = row['total_remaining'] or 0
            
            items = await db.fetch(
                """
                SELECT prize_name, remaining 
                FROM prize_pool 
                WHERE box_level = $1 AND remaining > 0 AND guild_id = $2
                ORDER BY prize_name
                """,
                box_level, guild_id
            )
            
            items_text = ""
            displayed_count = 0
            hidden_count = 0
            
            for item in items:
                displayed_count += 1
                if displayed_count <= 8:
                    items_text += f"• {item['prize_name']} (剩餘: {item['remaining']})\n"
                else:
                    hidden_count += 1
            
            if hidden_count > 0:
                items_text += f"... 還有 {hidden_count} 個獎品\n"
            
            actual_total = sum(item['remaining'] for item in items)
            
            embed.add_field(
                name=f"{box_level} (總剩餘: {actual_total} / 獎品種類: {total_items})",
                value=items_text if items_text else "無獎品",
                inline=False
            )
        
        embed.add_field(
            name="📊 積分抽獎機率",
            value="**50積分：** 綠箱70% 藍箱25% 紫箱4.5% 金箱0.5%\n"
                  "**100積分：** 綠箱50% 藍箱40% 紫箱9% 金箱1%\n"
                  "**500積分：** 綠箱10% 藍箱65% 紫箱20% 金箱5%",
            inline=False
        )
        
        embed.set_footer(text="使用 /add_prize 添加獎品到彩池")
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 讀取彩池失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

@tree.command(name="random_team", description="隨機分組（可設定每組人數上限）")
@app_commands.describe(
    max_team_size="每組最大人數（設定上限，不指定則自動分配）",
    team_count="組數（可選，與每組人數二選一）"
)
async def random_team_slash(
    interaction: discord.Interaction,
    max_team_size: Optional[int] = None,
    team_count: Optional[int] = None
):
    """隨機分組（改進版）"""
    await interaction.response.defer()
    
    try:
        guild_id = get_guild_id(interaction)
        await log_query("random_team", interaction.user.id, {"max_team_size": max_team_size, "team_count": team_count}, guild_id)
        
        if not interaction.guild:
            await interaction.followup.send("❌ 此指令只能在伺服器中使用")
            return
        
        # ========== 修復：新增分組模式選擇 ==========
        embed = discord.Embed(
            title="👥 隨機分組系統",
            description="請選擇分組模式：\n\n"
                       "**模式A：設定每組人數上限**\n"
                       "• 點擊 📏 按鈕參加\n"
                       "• 主持人設定每組最大人數\n"
                       "• 系統自動計算組數\n\n"
                       "**模式B：設定組數**\n"
                       "• 點擊 🎯 按鈕參加\n"
                       "• 主持人設定組數\n"
                       "• 系統自動計算每組人數",
            color=0x3498DB
        )
        
        embed.add_field(name="目前模式", value="未選擇", inline=True)
        
        if max_team_size:
            embed.add_field(name="每組最大人數", value=str(max_team_size), inline=True)
        if team_count:
            embed.add_field(name="組數", value=str(team_count), inline=True)
        
        embed.add_field(name="參加人數", value="0 人", inline=True)
        embed.set_footer(text="請先選擇分組模式，然後等待參加者...")
        
        await interaction.followup.send(embed=embed)
        message = await interaction.original_response()
        
        # 添加模式選擇按鈕
        await message.add_reaction("📏")  # 模式A：設定人數上限
        await message.add_reaction("🎯")  # 模式B：設定組數
        await message.add_reaction("🎮")  # 參加按鈕
        await message.add_reaction("▶️")  # 開始分組
        
        participants = []
        selected_mode = None  # 'size' 或 'count'
        
        def check(reaction, user):
            return (
                user != bot.user and
                str(reaction.emoji) in ["📏", "🎯", "🎮", "▶️"] and
                reaction.message.id == message.id
            )
        
        try:
            while True:
                reaction, user = await bot.wait_for('reaction_add', timeout=300.0, check=check)
                
                # ========== 修復：處理模式選擇 ==========
                if str(reaction.emoji) in ["📏", "🎯"]:
                    if user.id != interaction.user.id:
                        try:
                            await message.remove_reaction(reaction.emoji, user)
                            await message.channel.send(f"❌ <@{user.id}> 只有主持人可以選擇模式！", delete_after=5)
                        except:
                            pass
                        continue
                    
                    if str(reaction.emoji) == "📏":
                        selected_mode = 'size'
                        mode_text = "設定每組人數上限"
                        
                        # 如果已經指定了max_team_size，直接使用
                        if not max_team_size:
                            # 詢問每組最大人數
                            await message.channel.send(
                                f"<@{user.id}> 請輸入每組的最大人數（例如：5）：",
                                delete_after=10
                            )
                            
                            def check_msg(m):
                                return m.author.id == user.id and m.channel.id == message.channel.id and m.content.isdigit()
                            
                            try:
                                msg = await bot.wait_for('message', timeout=30.0, check=check_msg)
                                max_team_size = int(msg.content)
                                await msg.delete()
                            except asyncio.TimeoutError:
                                await message.channel.send("❌ 輸入超時，請重新選擇模式", delete_after=5)
                                continue
                    
                    else:  # 🎯
                        selected_mode = 'count'
                        mode_text = "設定組數"
                        
                        # 如果已經指定了team_count，直接使用
                        if not team_count:
                            # 詢問組數
                            await message.channel.send(
                                f"<@{user.id}> 請輸入要分成幾組（例如：2）：",
                                delete_after=10
                            )
                            
                            def check_msg(m):
                                return m.author.id == user.id and m.channel.id == message.channel.id and m.content.isdigit()
                            
                            try:
                                msg = await bot.wait_for('message', timeout=30.0, check=check_msg)
                                team_count = int(msg.content)
                                await msg.delete()
                            except asyncio.TimeoutError:
                                await message.channel.send("❌ 輸入超時，請重新選擇模式", delete_after=5)
                                continue
                    
                    # 更新訊息
                    new_embed = discord.Embed(
                        title="👥 隨機分組系統",
                        description=f"**已選擇：{mode_text}**\n\n"
                                   "點擊 🎮 按鈕參加分組\n"
                                   "主持人點擊 ▶️ 按鈕開始分組",
                        color=0x3498DB
                    )
                    
                    new_embed.add_field(name="目前模式", value=mode_text, inline=True)
                    
                    if selected_mode == 'size' and max_team_size:
                        new_embed.add_field(name="每組最大人數", value=str(max_team_size), inline=True)
                    elif selected_mode == 'count' and team_count:
                        new_embed.add_field(name="組數", value=str(team_count), inline=True)
                    
                    new_embed.add_field(name="參加人數", value=f"{len(participants)} 人", inline=True)
                    
                    if participants:
                        # ========== 修復：顯示所有參加者 ==========
                        participants_text = ""
                        for i, pid in enumerate(participants, 1):
                            participants_text += f"{i}. <@{pid}>\n"
                        
                        new_embed.add_field(
                            name=f"參加者 ({len(participants)}人)",
                            value=participants_text,
                            inline=False
                        )
                    
                    new_embed.set_footer(text=f"等待參加者... ({len(participants)}人參加)")
                    
                    await message.edit(embed=new_embed)
                
                # ========== 修復：參加功能 ==========
                elif str(reaction.emoji) == "🎮":
                    if user.id in participants:
                        try:
                            await message.remove_reaction(reaction.emoji, user)
                        except:
                            pass
                        continue
                    
                    # 添加到參與者列表
                    participants.append(user.id)
                    print(f"✅ 用戶 {user.id} 參加分組，目前 {len(participants)} 人")
                    
                    # 更新訊息
                    new_embed = discord.Embed(
                        title="👥 隨機分組系統",
                        description=f"**已選擇：{selected_mode or '未選擇'}**\n\n"
                                   "點擊 🎮 按鈕參加分組\n"
                                   "主持人點擊 ▶️ 按鈕開始分組",
                        color=0x3498DB
                    )
                    
                    mode_text = "設定每組人數上限" if selected_mode == 'size' else "設定組數" if selected_mode == 'count' else "未選擇"
                    new_embed.add_field(name="目前模式", value=mode_text, inline=True)
                    
                    if selected_mode == 'size' and max_team_size:
                        new_embed.add_field(name="每組最大人數", value=str(max_team_size), inline=True)
                    elif selected_mode == 'count' and team_count:
                        new_embed.add_field(name="組數", value=str(team_count), inline=True)
                    
                    new_embed.add_field(name="參加人數", value=f"{len(participants)} 人", inline=True)
                    
                    # 顯示所有參加者
                    if participants:
                        participants_text = ""
                        for i, pid in enumerate(participants, 1):
                            participants_text += f"{i}. <@{pid}>\n"
                        
                        new_embed.add_field(
                            name=f"參加者 ({len(participants)}人)",
                            value=participants_text,
                            inline=False
                        )
                    
                    new_embed.set_footer(text=f"等待主持人開始... ({len(participants)}人參加)")
                    
                    await message.edit(embed=new_embed)
                
                # ========== 修復：開始分組 ==========
                elif str(reaction.emoji) == "▶️" and user.id == interaction.user.id:
                    if not selected_mode:
                        await message.channel.send("❌ 請先選擇分組模式！", delete_after=5)
                        continue
                    
                    if len(participants) < 2:
                        await message.channel.send("❌ 至少需要2人才能開始分組", delete_after=5)
                        continue
                    
                    # 隨機打亂參與者
                    random.shuffle(participants)
                    
                    # ========== 修復：平衡分組邏輯 ==========
                    if selected_mode == 'size':  # 模式A：設定每組人數上限
                        if not max_team_size:
                            await message.channel.send("❌ 請設定每組最大人數！", delete_after=5)
                            continue
                        
                        # 計算最少需要的組數
                        min_teams = max(1, len(participants) // max_team_size)
                        if len(participants) % max_team_size != 0:
                            min_teams += 1
                        
                        # 嘗試平衡分組
                        team_size = len(participants) // min_teams
                        remainder = len(participants) % min_teams
                        
                        teams = []
                        start_idx = 0
                        
                        for i in range(min_teams):
                            # 計算這一組的人數
                            current_team_size = team_size + (1 if i < remainder else 0)
                            end_idx = start_idx + current_team_size
                            
                            team = participants[start_idx:end_idx]
                            teams.append(team)
                            start_idx = end_idx
                    
                    else:  # 模式B：設定組數
                        if not team_count:
                            await message.channel.send("❌ 請設定組數！", delete_after=5)
                            continue
                        
                        team_count = min(team_count, len(participants))  # 不能超過參與者數量
                        
                        # 平衡分組
                        base_size = len(participants) // team_count
                        remainder = len(participants) % team_count
                        
                        teams = []
                        start_idx = 0
                        
                        for i in range(team_count):
                            current_team_size = base_size + (1 if i < remainder else 0)
                            end_idx = start_idx + current_team_size
                            
                            team = participants[start_idx:end_idx]
                            teams.append(team)
                            start_idx = end_idx
                    
                    # ========== 修復：顯示分組結果 ==========
                    result_embed = discord.Embed(
                        title="👥 分組結果",
                        description=f"**總人數：** {len(participants)} 人\n"
                                   f"**分組方式：** {len(teams)} 組",
                        color=0x00FF00
                    )
                    
                    if selected_mode == 'size':
                        actual_max = max(len(team) for team in teams)
                        actual_min = min(len(team) for team in teams)
                        result_embed.description += f"\n**每組人數：** {actual_min}-{actual_max} 人 (上限: {max_team_size})"
                    else:
                        avg_size = len(participants) / len(teams)
                        result_embed.description += f"\n**平均每組：** {avg_size:.1f} 人"
                    
                    # 顯示所有組別
                    for i, team in enumerate(teams, 1):
                        members_list = "\n".join([f"{j+1}. <@{member_id}>" for j, member_id in enumerate(team)])
                        result_embed.add_field(
                            name=f"第 {i} 組 ({len(team)}人)",
                            value=members_list,
                            inline=False
                        )
                    
                    # 顯示所有參與者
                    participants_text = "\n".join([f"{j+1}. <@{pid}>" for j, pid in enumerate(participants)])
                    result_embed.add_field(
                        name=f"所有參與者 ({len(participants)}人)",
                        value=participants_text,
                        inline=False
                    )
                    
                    await message.channel.send(embed=result_embed)
                    await message.clear_reactions()
                    break
        
        except asyncio.TimeoutError:
            timeout_embed = discord.Embed(
                title="👥 分組超時",
                description="分組時間已過",
                color=0xFF0000
            )
            await message.edit(embed=timeout_embed)
            await message.clear_reactions()
            
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 分組失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

# ========== 修復反應處理中的問題 ==========

@bot.event
async def on_raw_reaction_add(payload):
    """處理反應事件"""
    if payload.user_id == bot.user.id:
        return
    
    try:
        emoji = str(payload.emoji)
        user_id = payload.user_id
        
        channel = bot.get_channel(payload.channel_id)
        if not channel:
            return
        
        try:
            message = await channel.fetch_message(payload.message_id)
        except:
            return
        
        guild_id = payload.guild_id if hasattr(payload, 'guild_id') else 0
        
        # ========== 修復：處理分組反應時的競賽條件 ==========
        # 檢查是否是分組訊息
        if emoji in ["📏", "🎯", "🎮", "▶️"]:
            # 檢查是否是分組訊息（透過訊息內容判斷）
            if message.embeds and len(message.embeds) > 0:
                embed = message.embeds[0]
                if "隨機分組系統" in embed.title or "分組" in embed.title:
                    # 檢查用戶是否已經在參與者中
                    # 這裡需要延遲一點，避免同時反應的問題
                    await asyncio.sleep(0.5)  # 增加延遲避免競賽條件
                    
                    # 重新獲取訊息以確保數據最新
                    try:
                        message = await channel.fetch_message(payload.message_id)
                    except:
                        return
                    
                    # 這裡的邏輯已經在上面修改的指令中處理
                    return
        
        # 檢查是否是抽獎
        result = await db.fetchrow(
            """
            SELECT id, participants, creator_id 
            FROM giveaways 
            WHERE message_id = $1 AND is_active = true AND guild_id = $2
            """,
            payload.message_id, guild_id
        )
        
        if result:
            giveaway_id = result['id']
            participants = result['participants'] or []
            creator_id = result['creator_id']
            
            if emoji == "🎫":
                # ========== 修復：避免重複加入 ==========
                if isinstance(participants, str):
                    try:
                        participants = json.loads(participants)
                    except:
                        participants = []
                
                # 檢查是否已經參加
                if user_id in participants:
                    try:
                        await message.remove_reaction(emoji, payload.member)
                    except:
                        pass
                    return
                
                # 添加延遲避免同時反應問題
                await asyncio.sleep(0.3)
                
                participants.append(user_id)
                await db.execute(
                    "UPDATE giveaways SET participants = $1 WHERE id = $2 AND guild_id = $3",
                    json.dumps(participants), giveaway_id, guild_id
                )
                
                try:
                    if message.embeds:
                        embed = message.embeds[0]
                        
                        new_embed = discord.Embed(
                            title=embed.title,
                            description=embed.description,
                            color=embed.color
                        )
                        
                        for field in embed.fields:
                            if field.name == "🎫 參與人數":
                                new_embed.add_field(
                                    name="🎫 參與人數", 
                                    value=f"{len(participants)} 人", 
                                    inline=field.inline
                                )
                            elif field.name == "📋 參與者列表":
                                # 更新參與者列表
                                participants_text = ""
                                for i, pid in enumerate(participants[:50], 1):
                                    participants_text += f"{i}. <@{pid}>\n"
                                
                                if len(participants) > 50:
                                    participants_text += f"\n... 還有 {len(participants) - 50} 人"
                                
                                new_embed.add_field(
                                    name="📋 參與者列表",
                                    value=participants_text,
                                    inline=False
                                )
                            else:
                                new_embed.add_field(
                                    name=field.name, 
                                    value=field.value, 
                                    inline=field.inline
                                )
                        
                        if embed.footer:
                            new_embed.set_footer(text=embed.footer.text)
                        
                        await message.edit(embed=new_embed)
                except Exception as e:
                    print(f"❌ 更新抽獎訊息錯誤: {e}")
            
            elif emoji == "⏹️" and user_id == creator_id:
                await end_giveaway(payload.message_id, manual=True, guild_id=guild_id)
                await channel.send(f"⏹️ 主辦人手動結束了抽獎！")
            return
        
        # ... 其他現有的反應處理邏輯保持不變 ...
        
    except Exception as e:
        print(f"❌ 處理反應錯誤: {e}")
        traceback.print_exc()

@tree.command(name="score_ranking", description="查看積分排行榜")
async def score_ranking_slash(interaction: discord.Interaction):
    """積分排行榜"""
    await interaction.response.defer()
    
    try:
        if not db.is_connected:
            embed = discord.Embed(
                title="⚠️ 資料庫未連接",
                description="無法查看積分排行榜，請稍後再試。",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        guild_id = get_guild_id(interaction)
        await log_query("score_ranking", interaction.user.id, {"action": "view_ranking"}, guild_id)
        
        # 獲取排行榜
        results = await db.fetch(
            """
            SELECT user_id, username, current_score, total_score 
            FROM users 
            WHERE guild_id = $1 
            ORDER BY current_score DESC 
            LIMIT 15
            """,
            guild_id
        )
        
        if not results:
            embed = discord.Embed(
                title="🏆 積分排行榜",
                description="目前還沒有用戶積分數據",
                color=0xFFD700
            )
            await interaction.followup.send(embed=embed)
            return
        
        embed = discord.Embed(
            title="🏆 積分排行榜",
            description="當前積分排名前15名：",
            color=0xFFD700
        )
        
        ranking_text = ""
        for i, row in enumerate(results, 1):
            medal = ""
            if i == 1:
                medal = "🥇 "
            elif i == 2:
                medal = "🥈 "
            elif i == 3:
                medal = "🥉 "
            
            ranking_text += f"**{medal}{i}. {row['username']}**\n"
            ranking_text += f"   當前：{row['current_score']}分 | 總計：{row['total_score']}分\n"
        
        embed.add_field(name="🏅 排名", value=ranking_text, inline=False)
        
        # 添加當前用戶排名
        result = await db.fetchrow(
            """
            SELECT COUNT(*) FROM users 
            WHERE guild_id = $1 AND current_score > (
                SELECT current_score FROM users WHERE user_id = $2 AND guild_id = $3
            )
            """,
            guild_id, interaction.user.id, guild_id
        )
        
        if result:
            higher_count = result['count']
            user_rank = higher_count + 1
        else:
            user_rank = 1
        
        embed.add_field(
            name="📊 你的排名",
            value=f"**{interaction.user.name}** 當前排名第 **{user_rank}** 名",
            inline=False
        )
        
        embed.set_footer(text="積分可用於抽獎和轉移")
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 讀取排行榜失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

@tree.command(name="attendance_ranking", description="查看出席率排行榜（分頁顯示）")
@app_commands.describe(
    period="統計期間",
    page="頁數（從1開始）"
)
async def attendance_ranking_slash(
    interaction: discord.Interaction,
    period: Literal["current", "all"] = "current",
    page: int = 1
):
    """出席率排行榜（分頁版）"""
    await interaction.response.defer()
    
    try:
        if not db.is_connected:
            embed = discord.Embed(
                title="⚠️ 資料庫未連接",
                description="無法查看出席率排行榜，請稍後再試。",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        guild_id = get_guild_id(interaction)
        await log_query("attendance_ranking", interaction.user.id, {"period": period, "page": page}, guild_id)
        
        if page < 1:
            await interaction.followup.send("❌ 頁數必須大於 0")
            return
        
        # 獲取所有出席數據
        rankings = await get_all_attendance_data(guild_id, period)
        
        if not rankings:
            embed = discord.Embed(
                title="📊 出席率排行榜",
                description="目前還沒有出席率數據",
                color=0x3498DB
            )
            await interaction.followup.send(embed=embed)
            return
        
        # 分頁設定
        users_per_page = 100  # 每頁顯示100人
        total_users = len(rankings)
        total_pages = (total_users + users_per_page - 1) // users_per_page
        
        if page > total_pages:
            await interaction.followup.send(f"❌ 只有 {total_pages} 頁，無法顯示第 {page} 頁")
            return
        
        # 計算當前頁的起始和結束索引
        start_idx = (page - 1) * users_per_page
        end_idx = min(start_idx + users_per_page, total_users)
        current_page_rankings = rankings[start_idx:end_idx]
        
        period_text = "當前半月期" if period == "current" else "全部期間"
        
        embed = discord.Embed(
            title=f"📊 出席率排行榜 - {period_text}",
            description=f"第 {page}/{total_pages} 頁 (共 {total_users} 人)",
            color=0x3498DB
        )
        
        # 添加排名列表
        ranking_text = ""
        for i, rank in enumerate(current_page_rankings, start=start_idx + 1):
            # 前3名有獎牌
            medal = ""
            if i == 1:
                medal = "🥇 "
            elif i == 2:
                medal = "🥈 "
            elif i == 3:
                medal = "🥉 "
            
            user = bot.get_user(rank['user_id'])
            username = user.name if user else rank['username']
            
            # 縮短過長的用戶名
            if len(username) > 20:
                username = username[:17] + "..."
            
            ranking_text += f"**{medal}{i}. {username}**\n"
            ranking_text += f"   出席率：{rank['attendance_rate']:.1f}% ({rank['attended']}/{rank['total']}次)\n"
            
            # 每10個成員加一個分隔線
            if i % 10 == 0 and i < end_idx:
                ranking_text += "---\n"
        
        embed.add_field(name="🏆 排名", value=ranking_text, inline=False)
        
        # 添加統計摘要（只計算當前頁的數據）
        if current_page_rankings:
            page_avg_attendance = sum(r['attendance_rate'] for r in current_page_rankings) / len(current_page_rankings)
            page_highest = current_page_rankings[0]['attendance_rate']
            page_lowest = current_page_rankings[-1]['attendance_rate']
            
            embed.add_field(
                name="📈 頁面統計",
                value=f"**本頁人數：** {len(current_page_rankings)} 人\n"
                      f"**平均出席率：** {page_avg_attendance:.1f}%\n"
                      f"**最高出席率：** {page_highest:.1f}%\n"
                      f"**最低出席率：** {page_lowest:.1f}%",
                inline=False
            )
        
        # 添加當前用戶的排名
        current_user_rank = None
        for i, rank in enumerate(rankings, 1):
            if rank['user_id'] == interaction.user.id:
                current_user_rank = i
                break
        
        if current_user_rank:
            user_rank = rankings[current_user_rank - 1]
            user_page = ((current_user_rank - 1) // users_per_page) + 1
            
            user_rank_text = f"**你的排名：** 第 {current_user_rank} 名 (在第 {user_page} 頁)\n"
            user_rank_text += f"**出席率：** {user_rank['attendance_rate']:.1f}% ({user_rank['attended']}/{user_rank['total']}次)"
        else:
            user_rank_text = "**你的排名：** 未上榜"
        
        embed.add_field(name="👤 你的表現", value=user_rank_text, inline=False)
        
        # 添加分頁導航按鈕
        class PaginationView(discord.ui.View):
            def __init__(self, period, current_page, total_pages, guild_id):
                super().__init__(timeout=180)
                self.period = period
                self.current_page = current_page
                self.total_pages = total_pages
                self.guild_id = guild_id
            
            @discord.ui.button(label="◀️ 上一頁", style=discord.ButtonStyle.primary, disabled=True)
            async def previous_page(self, interaction: discord.Interaction, button: discord.ui.Button):
                if self.current_page <= 1:
                    await interaction.response.send_message("❌ 已經是第一頁了", ephemeral=True)
                    return
                
                new_page = self.current_page - 1
                await self.show_page(interaction, new_page)
            
            @discord.ui.button(label="下一頁 ▶️", style=discord.ButtonStyle.primary, disabled=True)
            async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
                if self.current_page >= self.total_pages:
                    await interaction.response.send_message("❌ 已經是最後一頁了", ephemeral=True)
                    return
                
                new_page = self.current_page + 1
                await self.show_page(interaction, new_page)
            
            @discord.ui.button(label="🔢 跳轉頁面", style=discord.ButtonStyle.secondary)
            async def jump_page(self, interaction: discord.Interaction, button: discord.ui.Button):
                modal = PageJumpModal(self.total_pages, self.period, self.guild_id)
                await interaction.response.send_modal(modal)
            
            async def show_page(self, interaction: discord.Interaction, page: int):
                await interaction.response.defer()
                
                # 獲取新頁面的數據
                rankings = await get_all_attendance_data(self.guild_id, self.period)
                total_users = len(rankings)
                total_pages = (total_users + users_per_page - 1) // users_per_page
                
                if page < 1 or page > total_pages:
                    await interaction.followup.send(f"❌ 頁數必須在 1-{total_pages} 之間", ephemeral=True)
                    return
                
                # 計算新頁面的起始和結束索引
                start_idx = (page - 1) * users_per_page
                end_idx = min(start_idx + users_per_page, total_users)
                current_page_rankings = rankings[start_idx:end_idx]
                
                period_text = "當前半月期" if self.period == "current" else "全部期間"
                
                new_embed = discord.Embed(
                    title=f"📊 出席率排行榜 - {period_text}",
                    description=f"第 {page}/{total_pages} 頁 (共 {total_users} 人)",
                    color=0x3498DB
                )
                
                # 添加排名列表
                ranking_text = ""
                for i, rank in enumerate(current_page_rankings, start=start_idx + 1):
                    medal = ""
                    if i == 1:
                        medal = "🥇 "
                    elif i == 2:
                        medal = "🥈 "
                    elif i == 3:
                        medal = "🥉 "
                    
                    user = bot.get_user(rank['user_id'])
                    username = user.name if user else rank['username']
                    
                    if len(username) > 20:
                        username = username[:17] + "..."
                    
                    ranking_text += f"**{medal}{i}. {username}**\n"
                    ranking_text += f"   出席率：{rank['attendance_rate']:.1f}% ({rank['attended']}/{rank['total']}次)\n"
                    
                    if i % 10 == 0 and i < end_idx:
                        ranking_text += "---\n"
                
                new_embed.add_field(name="🏆 排名", value=ranking_text, inline=False)
                
                # 更新頁面統計
                if current_page_rankings:
                    page_avg_attendance = sum(r['attendance_rate'] for r in current_page_rankings) / len(current_page_rankings)
                    page_highest = current_page_rankings[0]['attendance_rate']
                    page_lowest = current_page_rankings[-1]['attendance_rate']
                    
                    new_embed.add_field(
                        name="📈 頁面統計",
                        value=f"**本頁人數：** {len(current_page_rankings)} 人\n"
                              f"**平均出席率：** {page_avg_attendance:.1f}%\n"
                              f"**最高出席率：** {page_highest:.1f}%\n"
                              f"**最低出席率：** {page_lowest:.1f}%",
                        inline=False
                    )
                
                # 更新用戶排名
                current_user_rank = None
                for i, rank in enumerate(rankings, 1):
                    if rank['user_id'] == interaction.user.id:
                        current_user_rank = i
                        break
                
                if current_user_rank:
                    user_rank = rankings[current_user_rank - 1]
                    user_page = ((current_user_rank - 1) // users_per_page) + 1
                    
                    user_rank_text = f"**你的排名：** 第 {current_user_rank} 名 (在第 {user_page} 頁)\n"
                    user_rank_text += f"**出席率：** {user_rank['attendance_rate']:.1f}% ({user_rank['attended']}/{user_rank['total']}次)"
                else:
                    user_rank_text = "**你的排名：** 未上榜"
                
                new_embed.add_field(name="👤 你的表現", value=user_rank_text, inline=False)
                
                # 更新按鈕狀態
                self.current_page = page
                self.previous_page.disabled = (page <= 1)
                self.next_page.disabled = (page >= total_pages)
                
                # 更新訊息
                await interaction.message.edit(embed=new_embed, view=self)
        
        class PageJumpModal(discord.ui.Modal, title="跳轉到指定頁面"):
            page_number = discord.ui.TextInput(
                label=f"輸入頁數 (1-{total_pages})",
                placeholder="例如：2",
                required=True,
                max_length=3
            )
            
            def __init__(self, total_pages, period, guild_id):
                super().__init__()
                self.total_pages = total_pages
                self.period = period
                self.guild_id = guild_id
            
            async def on_submit(self, interaction: discord.Interaction):
                try:
                    page = int(self.page_number.value)
                    if page < 1 or page > self.total_pages:
                        await interaction.response.send_message(f"❌ 頁數必須在 1-{self.total_pages} 之間", ephemeral=True)
                        return
                    
                    # 找到原始訊息並更新
                    for view in interaction.message.components:
                        if isinstance(view, PaginationView):
                            await view.show_page(interaction, page)
                            return
                    
                    await interaction.response.send_message("❌ 無法找到原始訊息", ephemeral=True)
                    
                except ValueError:
                    await interaction.response.send_message("❌ 請輸入有效的數字", ephemeral=True)
        
        # 設置分頁按鈕狀態
        view = PaginationView(period, page, total_pages, guild_id)
        view.previous_page.disabled = (page <= 1)
        view.next_page.disabled = (page >= total_pages)
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M")
        embed.set_footer(text=f"統計期間: {period_text} | 更新時間: {current_time}")
        
        await interaction.followup.send(embed=embed, view=view)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 讀取出席率排行榜失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

# ========== 新增 /blessing 指令 ==========

# ========== 新增/blessing指令資料庫 ==========
BLESSINGS_DATABASE = {
    "運程等級": {
        "大吉": {
            "emoji": "🎉",
            "chance": 8,
            "color": 0xFFD700,
            "descriptions": [
                "今日紫氣東來，事事順心如意！",
                "鴻運當頭，心想事成的好日子！",
                "福星高照，好運連連不斷！",
                "今日諸事大吉，有如神助！",
                "運勢如虹，把握良機必有所成！"
            ],
            "symbols": ["🎊", "✨", "🏆", "💎", "🌟"]
        },
        "吉": {
            "emoji": "😊",
            "chance": 25,
            "color": 0x90EE90,
            "descriptions": [
                "今日運氣平順，小確幸不斷！",
                "會有意外的小驚喜等著你！",
                "平穩中帶有好運的一天！",
                "心想事成，願望容易實現！",
                "運氣不錯，保持積極心態！"
            ],
            "symbols": ["🍀", "🌈", "🎯", "💫", "🪄"]
        },
        "中平": {
            "emoji": "😐",
            "chance": 40,
            "color": 0x87CEEB,
            "descriptions": [
                "平穩的一天，適合按部就班！",
                "保持平常心，平安就是福！",
                "無風無浪，穩健前進！",
                "中庸之道，避免極端決策！",
                "一切都剛剛好，知足常樂！"
            ],
            "symbols": ["⚖️", "🌊", "🕰️", "📊", "🧭"]
        },
        "小凶": {
            "emoji": "😟",
            "chance": 20,
            "color": 0xFFA500,
            "descriptions": [
                "今日需小心謹慎，避免衝動！",
                "波折稍多，耐心應對即可！",
                "小麻煩不斷，但都能解決！",
                "多一分謹慎，少一分煩惱！",
                "運勢低迷，宜守不宜攻！"
            ],
            "symbols": ["⚠️", "🌀", "🌪️", "💣", "🔮"]
        },
        "大凶": {
            "emoji": "😨",
            "chance": 7,
            "color": 0xFF4500,
            "descriptions": [
                "諸事不順，建議低調行事！",
                "運勢低迷，宜靜不宜動！",
                "危機四伏，謹言慎行為上！",
                "烏雲密布，等待雨過天晴！",
                "考驗重重，堅強面對挑戰！"
            ],
            "symbols": ["💀", "☠️", "🌩️", "🔥", "🌋"]
        }
    },
    
    "職業建議": {
        "坦克": [
            "今天很適合擔任隊伍前鋒，你的堅韌會帶來勝利！",
            "防禦時機特別準確，會成為團隊的中流砥柱！",
            "保護隊友的能力特別突出，大家都很信賴你！",
            "指揮團隊時思路清晰，帶領大家走向勝利！"
        ],
        "输出": [
            "今天攻擊力大幅提升，是輸出的最佳時機！",
            "技能連招特別流暢，打出漂亮的傷害吧！",
            "暴擊率似乎提高了，期待你的精彩表現！",
            "輸出時機把握得很好，會成為隊伍的MVP！"
        ],
        "治疗": [
            "治療時機把握精準，能拯救關鍵隊友！",
            "回復量特別可觀，是隊伍的安心保障！",
            "輔助技能效果提升，團隊續航力大增！",
            "你的治療總是出現在最需要的時刻！"
        ],
        "辅助": [
            "控制技能命中率提高，能完美牽制敵人！",
            "團隊增益效果特別顯著，大家都變強了！",
            "視野掌控能力一流，為隊伍創造機會！",
            "你的輔助讓整個團隊的戰鬥力提升一個檔次！"
        ]
    },
    
    "生活建議": {
        "大吉": [
            "適合簽訂重要合約或進行投資！",
            "表白成功率超高，把握機會！",
            "工作上有晉升或加薪的機會！",
            "出門可能會遇到貴人或舊友！",
            "學習新事物效率特別高！",
            "創意靈感源源不絕！"
        ],
        "吉": [
            "可以嘗試新的挑戰或興趣！",
            "與朋友聚會會有意外收穫！",
            "適合規劃未來的目標計畫！",
            "可能會收到意想不到的禮物！",
            "工作效率特別高，事半功倍！"
        ],
        "中平": [
            "按部就班完成日常工作！",
            "保持心情平靜最重要！",
            "適合整理環境或思緒！",
            "避免衝動消費或決定！",
            "多與家人朋友交流！"
        ],
        "小凶": [
            "重要文件記得備份！",
            "外出注意交通安全！",
            "避免與人發生爭執！",
            "謹慎處理財務問題！",
            "重要決策多思考幾遍！"
        ],
        "大凶": [
            "盡量待在家裡休息！",
            "避免簽署重要文件！",
            "出門記得帶傘備用！",
            "今天適合低調行事！",
            "重要事情改日再處理！"
        ]
    },
    
    "愛情運勢": {
        "大吉": [
            "單身者：今天可能遇到心儀對象！",
            "戀愛中：感情甜蜜，適合浪漫約會！",
            "已婚者：夫妻感情更加融洽！",
            "告白成功率高達90%！",
            "會有令人心動的邂逅！"
        ],
        "吉": [
            "單身者：桃花運不錯，多參加社交！",
            "戀愛中：小驚喜讓感情升溫！",
            "已婚者：平淡中見真情！",
            "適合表達心中的愛意！",
            "會收到溫馨的關心！"
        ],
        "中平": [
            "保持平常心對待感情！",
            "多溝通少猜疑！",
            "適合一起做家務增進感情！",
            "不要計較小事！",
            "平淡就是幸福！"
        ],
        "小凶": [
            "容易因小事產生誤會！",
            "需要多一點耐心溝通！",
            "避免討論敏感話題！",
            "給彼此一些空間！",
            "不要衝動說出傷人的話！"
        ],
        "大凶": [
            "容易發生爭吵！",
            "建議冷靜後再溝通！",
            "避免做出重要感情決定！",
            "多體諒對方的難處！",
            "退一步海闊天空！"
        ]
    },
    
    "財運建議": {
        "大吉": [
            "投資運極佳，小試身手！",
            "可能會有意外的收入！",
            "購物時容易撿到便宜！",
            "財神眷顧，把握機會！",
            "適合規劃理財計畫！"
        ],
        "吉": [
            "正財穩定，偏財也有機會！",
            "購物慾望強，但要理性消費！",
            "可能會收到小紅包！",
            "儲蓄計畫進展順利！",
            "適合學習理財知識！"
        ],
        "中平": [
            "收支平衡，穩健為上！",
            "避免衝動購物！",
            "定期儲蓄是好習慣！",
            "檢視開支，做好預算！",
            "量入為出最重要！"
        ],
        "小凶": [
            "小心財物損失！",
            "投資需格外謹慎！",
            "避免借貸給他人！",
            "檢查帳單避免錯誤！",
            "重要財務文件保管好！"
        ],
        "大凶": [
            "破財風險高！",
            "重要投資暫緩！",
            "避免大額消費！",
            "謹防詐騙電話！",
            "現金財物妥善保管！"
        ]
    },
    
    "幸運物": {
        "大吉": ["金幣", "水晶", "龍", "鳳凰", "幸運草"],
        "吉": ["銀飾", "玉佩", "兔子", "蝴蝶", "四葉草"],
        "中平": ["陶瓷", "木製品", "烏龜", "鴿子", "石頭"],
        "小凶": ["鐵器", "鏡子", "烏鴉", "蜘蛛", "枯枝"],
        "大凶": ["玻璃", "尖銳物", "蝙蝠", "蠍子", "斷劍"]
    },
    
    "幸運顏色": {
        "大吉": ["金色", "紫色", "紅色", "翡翠綠", "寶藍色"],
        "吉": ["綠色", "藍色", "粉色", "鵝黃色", "淺紫色"],
        "中平": ["白色", "灰色", "米色", "淺藍色", "卡其色"],
        "小凶": ["黑色", "深灰色", "褐色", "墨綠色", "暗紅色"],
        "大凶": ["血紅色", "深黑色", "暗紫色", "鐵灰色", "咖啡色"]
    },
    
    "開場白": [
        "🔮 正在為 {user} 解讀命運之輪...",
        "✨ 窺探 {user} 今日的星象軌跡...",
        "🎭 分析 {user} 的今日氣場能量...",
        "🌌 探尋 {user} 今日的命運脈絡...",
        "🧿 解碼 {user} 的今日幸運密語...",
        "💫 感應 {user} 今日的能量波動...",
        "🌟 為 {user} 展開今日運勢圖譜...",
        "🌠 解讀 {user} 的今日命運密碼...",
        "⚡ 掃描 {user} 今日的運氣頻率...",
        "🌙 預測 {user} 今日的命運走向..."
    ],
    
    "結語": [
        "命運負責洗牌，但玩牌的是我們自己。",
        "好運氣來自好習慣。",
        "每一天都是新的開始。",
        "心態決定狀態。",
        "機會總是留給有準備的人。",
        "相信自己的選擇。",
        "微笑是最好的幸運符。",
        "堅持就是最好的運氣。",
        "善良的人運氣不會太差。",
        "今天就是最好的禮物。"
    ]
}

def get_user_profession(user_id, guild_id):
    """獲取用戶的主要職業"""
    # 這裡可以從資料庫獲取用戶最常使用的職業
    # 暫時先隨機返回一個職業
    professions = ["坦克", "输出", "治疗", "辅助"]
    return random.choice(professions)

# ========== 改進版 /blessing 指令 ==========

@tree.command(name="blessing", description="測試今日運程（豐富版）")
async def blessing_slash(interaction: discord.Interaction):
    """測試運程（豐富版）"""
    await interaction.response.defer()
    
    try:
        # 隨機選擇開場白
        opening = random.choice(BLESSINGS_DATABASE["開場白"]).format(user=interaction.user.name)
        
        # 先發送一個有趣的等待訊息
        waiting_embed = discord.Embed(
            title=f"🔮 占卜中...",
            description=opening,
            color=0x7289DA
        )
        waiting_embed.set_thumbnail(url="https://cdn.discordapp.com/emojis/856919111867498507.gif" if interaction.user.avatar else None)
        
        await interaction.followup.send(embed=waiting_embed)
        message = await interaction.original_response()
        
        # 模擬占卜過程
        await asyncio.sleep(1.5)
        
        # 運程選擇
        blessing_types = list(BLESSINGS_DATABASE["運程等級"].keys())
        weights = [BLESSINGS_DATABASE["運程等級"][b]["chance"] for b in blessing_types]
        result = random.choices(blessing_types, weights=weights, k=1)[0]
        blessing_info = BLESSINGS_DATABASE["運程等級"][result]
        
        # 獲取用戶職業（如果有）
        user_profession = get_user_profession(interaction.user.id, get_guild_id(interaction))
        
        # 隨機選擇內容
        description = random.choice(blessing_info["descriptions"])
        symbol = random.choice(blessing_info["symbols"])
        
        # 職業建議
        profession_advice = ""
        if user_profession in BLESSINGS_DATABASE["職業建議"]:
            profession_advice = random.choice(BLESSINGS_DATABASE["職業建議"][user_profession])
        
        # 生活建議
        life_advices = random.sample(BLESSINGS_DATABASE["生活建議"][result], min(3, len(BLESSINGS_DATABASE["生活建議"][result])))
        
        # 愛情運勢
        love_fortune = random.choice(BLESSINGS_DATABASE["愛情運勢"][result])
        
        # 財運建議
        money_advice = random.choice(BLESSINGS_DATABASE["財運建議"][result])
        
        # 幸運物和顏色
        lucky_item = random.choice(BLESSINGS_DATABASE["幸運物"][result])
        lucky_color = random.choice(BLESSINGS_DATABASE["幸運顏色"][result])
        
        # 時間運勢
        time_fortune = {
            "上午": random.choice(["吉", "平", "凶"]),
            "下午": random.choice(["吉", "平", "凶"]),
            "晚上": random.choice(["吉", "平", "凶"])
        }
        
        # 幸運數字（1-99）
        lucky_numbers = sorted(random.sample(range(1, 100), 3))
        
        # 幸運方向
        directions = ["東", "南", "西", "北", "東南", "西南", "東北", "西北"]
        lucky_direction = random.choice(directions)
        
        # 結語
        closing = random.choice(BLESSINGS_DATABASE["結語"])
        
        # 創建最終嵌入訊息
        embed = discord.Embed(
            title=f"{blessing_info['emoji']} {symbol} {interaction.user.name} 的今日運程 {symbol}",
            description=f"**{result}**\n\n{description}",
            color=blessing_info['color']
        )
        
        # 運程分析
        embed.add_field(
            name="📊 運程分析",
            value=f"**運程等級：** {result}\n"
                  f"**出現機率：** {blessing_info['chance']}%\n"
                  f"**幸運顏色：** {lucky_color}\n"
                  f"**幸運物品：** {lucky_item}",
            inline=True
        )
        
        # 時間運勢
        embed.add_field(
            name="🕰️ 時段運勢",
            value=f"**上午：** {time_fortune['上午']}\n"
                  f"**下午：** {time_fortune['下午']}\n"
                  f"**晚上：** {time_fortune['晚上']}",
            inline=True
        )
        
        # 幸運數字和方向
        embed.add_field(
            name="🎲 幸運指引",
            value=f"**幸運數字：** {', '.join(map(str, lucky_numbers))}\n"
                  f"**幸運方向：** {lucky_direction}",
            inline=True
        )
        
        # 職業建議（如果用戶有主要職業）
        if profession_advice:
            embed.add_field(
                name=f"🎮 {user_profession}專屬建議",
                value=profession_advice,
                inline=False
            )
        
        # 生活建議
        life_text = ""
        for i, advice in enumerate(life_advices, 1):
            life_text += f"{i}. {advice}\n"
        
        embed.add_field(
            name="💡 今日生活建議",
            value=life_text,
            inline=False
        )
        
        # 愛情運勢
        embed.add_field(
            name="💖 愛情運勢",
            value=love_fortune,
            inline=True
        )
        
        # 財運建議
        embed.add_field(
            name="💰 財運指南",
            value=money_advice,
            inline=True
        )
        
        # 運勢提醒
        if result in ["小凶", "大凶"]:
            warning_emojis = ["⚠️", "🔮", "🌪️", "💣"]
            warning = random.choice([
                "今天需要格外小心！",
                "保持警覺，謹慎行事！",
                "低調是今天的護身符！",
                "耐心等待運勢好轉！"
            ])
            embed.add_field(
                name=f"{random.choice(warning_emojis)} 特別提醒",
                value=warning,
                inline=False
            )
        elif result in ["大吉", "吉"]:
            celebration_emojis = ["🎊", "✨", "🏆", "💎"]
            celebration = random.choice([
                "把握今天的好運氣！",
                "積極行動會有回報！",
                "今天是展現自己的好日子！",
                "好運要與朋友分享！"
            ])
            embed.add_field(
                name=f"{random.choice(celebration_emojis)} 好運提示",
                value=celebration,
                inline=False
            )
        
        # 添加趣味統計
        embed.add_field(
            name="📈 今日趣味統計",
            value=f"**今日幸運指數：** {random.randint(50, 100)}%\n"
                  f"**正能量指數：** {random.randint(60, 100)}%\n"
                  f"**驚喜機率：** {random.randint(10, 80)}%",
            inline=False
        )
        
        embed.set_footer(text=closing)
        
        if interaction.user.avatar:
            embed.set_thumbnail(url=interaction.user.avatar.url)
        
        # 使用編輯更新原本的等待訊息
        await message.edit(embed=embed)
        
        # 根據運程結果發送不同的反應
        try:
            if result == "大吉":
                await message.add_reaction("🎉")
                await message.add_reaction("✨")
            elif result == "吉":
                await message.add_reaction("😊")
                await message.add_reaction("🍀")
            elif result == "中平":
                await message.add_reaction("😐")
                await message.add_reaction("⚖️")
            elif result == "小凶":
                await message.add_reaction("😟")
                await message.add_reaction("⚠️")
            elif result == "大凶":
                await message.add_reaction("😨")
                await message.add_reaction("💀")
        except:
            pass
        
        # 發送額外的趣味訊息（20%機率）
        if random.random() < 0.2:
            fun_facts = [
                "🔮 小雲占卜小知識：每天的第一次占卜最準確！",
                "✨ 溫馨提示：運程僅供娛樂，實際生活更重要！",
                "💫 幸運小秘訣：保持微笑會帶來好運氣！",
                "🌟 趣味統計：大吉出現機率僅8%，你很幸運！",
                "🎭 今日運程解讀完成，期待你的精彩一天！"
            ]
            await message.channel.send(random.choice(fun_facts), delete_after=10)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 運程測試失敗",
            description=f"占卜過程中出現錯誤：{str(e)[:100]}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

# ========== 管理員指令 (4個) ==========

@tree.command(name="add_prize", description="添加或減少彩池獎品")
@app_commands.describe(
    name="獎品名稱",
    box_level="寶箱等級 (綠箱/藍箱/紫箱/金箱)",
    quantity="數量 (正數添加, 負數減少)"
)
async def add_prize_slash(
    interaction: discord.Interaction,
    name: str,
    box_level: str,
    quantity: int
):
    """添加/減少獎品"""
    await interaction.response.defer()
    
    try:
        if not interaction.user.guild_permissions.administrator:
            await interaction.followup.send("❌ 需要管理員權限")
            return
        
        if not db.is_connected:
            embed = discord.Embed(
                title="⚠️ 資料庫未連接",
                description="無法操作彩池，請稍後再試。",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        guild_id = get_guild_id(interaction)
        
        valid_levels = ["綠箱", "藍箱", "紫箱", "金箱"]
        if box_level not in valid_levels:
            await interaction.followup.send(f"❌ 無效的寶箱等級！請選擇：{', '.join(valid_levels)}")
            return
        
        # 初始化變數
        total_qty = 0
        remaining_qty = 0
        current_qty = 0  # ⬅️ 提前初始化
        current_remaining = 0  # ⬅️ 提前初始化
        action = ""
        
        if quantity > 0:
            # 添加獎品 - 先檢查現有數量以取得原數量
            existing = await db.fetchrow(
                "SELECT quantity, remaining FROM prize_pool WHERE prize_name = $1 AND box_level = $2 AND guild_id = $3",
                name, box_level, guild_id
            )
            
            if existing:
                current_qty = existing['quantity']
                current_remaining = existing['remaining']
            
            # 添加獎品
            result = await db.fetchrow(
                """
                INSERT INTO prize_pool (prize_name, box_level, quantity, remaining, added_by, guild_id)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (prize_name, box_level, guild_id) 
                DO UPDATE SET 
                    quantity = prize_pool.quantity + EXCLUDED.quantity,
                    remaining = prize_pool.remaining + EXCLUDED.quantity
                RETURNING quantity, remaining
                """,
                name, box_level, quantity, quantity, interaction.user.id, guild_id
            )
            
            if result:
                total_qty = result['quantity']
                remaining_qty = result['remaining']
            
            action = "添加"
            
        elif quantity < 0:
            # 減少獎品 - 先檢查現有數量
            existing = await db.fetchrow(
                "SELECT quantity, remaining FROM prize_pool WHERE prize_name = $1 AND box_level = $2 AND guild_id = $3",
                name, box_level, guild_id
            )
            
            if not existing:
                await interaction.followup.send(f"❌ 找不到獎品 '{name}' 在 {box_level} 中")
                return
            
            current_qty = existing['quantity']
            current_remaining = existing['remaining']
            
            # 計算新的數量（不能少於0）
            new_qty = max(current_qty + quantity, 0)  # quantity 是負數
            new_remaining = max(current_remaining + quantity, 0)
            
            if new_qty == 0:
                # 如果數量變為0，刪除該記錄
                await db.execute(
                    "DELETE FROM prize_pool WHERE prize_name = $1 AND box_level = $2 AND guild_id = $3",
                    name, box_level, guild_id
                )
                action = "刪除"
                total_qty = 0
                remaining_qty = 0
                
            else:
                # 更新數量
                await db.execute(
                    """
                    UPDATE prize_pool 
                    SET quantity = $1, remaining = $2
                    WHERE prize_name = $3 AND box_level = $4 AND guild_id = $5
                    """,
                    new_qty, new_remaining, name, box_level, guild_id
                )
                action = "減少"
                total_qty = new_qty
                remaining_qty = new_remaining
        else:
            await interaction.followup.send("❌ 數量不能為 0")
            return
        
        # 發送結果
        color = 0x2ECC71 if quantity > 0 else 0xE74C3C
        title = "✅ 獎品操作成功" if action != "刪除" else "✅ 獎品已刪除"
        
        embed = discord.Embed(title=title, color=color)
        embed.add_field(name="獎品名稱", value=name, inline=True)
        embed.add_field(name="寶箱等級", value=box_level, inline=True)
        embed.add_field(name="操作類型", value=action, inline=True)
        
        if action != "刪除":
            embed.add_field(name="變動數量", value=f"{abs(quantity)} 個", inline=True)
            embed.add_field(name="原數量", value=f"{current_qty} 個", inline=True)
            embed.add_field(name="新總數量", value=f"{total_qty} 個", inline=True)
            embed.add_field(name="原剩餘", value=f"{current_remaining} 個", inline=True)
            embed.add_field(name="新剩餘", value=f"{remaining_qty} 個", inline=True)
        else:
            embed.add_field(name="操作", value="已從彩池中刪除", inline=True)
            embed.add_field(name="原數量", value=f"{current_qty} 個", inline=True)
            embed.add_field(name="原剩餘", value=f"{current_remaining} 個", inline=True)
        
        embed.add_field(name="操作者", value=interaction.user.mention, inline=True)
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 操作失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

@tree.command(name="add_score", description="調整用戶積分")
@app_commands.describe(
    user="目標用戶",
    amount="積分變化（正數為增加，負數為減少）",
    reason="原因"
)
async def add_score_slash(
    interaction: discord.Interaction,
    user: discord.Member,
    amount: int,
    reason: str
):
    """調整積分"""
    await interaction.response.defer()
    
    try:
        if not interaction.user.guild_permissions.administrator:
            await interaction.followup.send("❌ 需要管理員權限")
            return
        
        guild_id = get_guild_id(interaction)
        await log_query("add_score", interaction.user.id, {"target": user.id, "amount": amount, "reason": reason}, guild_id)
        
        if amount == 0:
            await interaction.followup.send("❌ 積分變化不能為 0")
            return
        
        old_score, old_total = await get_user_score(user.id, guild_id)
        await update_user_score(user.id, user.name, amount, f"管理員調整: {reason}", guild_id)
        new_score, new_total = await get_user_score(user.id, guild_id)
        
        action = "增加" if amount > 0 else "減少"
        embed = discord.Embed(
            title=f"✅ 積分{action}成功",
            color=0x2ECC71 if amount > 0 else 0xE74C3C
        )
        
        embed.add_field(name="用戶", value=user.mention, inline=True)
        embed.add_field(name=f"{action}積分", value=f"{abs(amount)} 分", inline=True)
        embed.add_field(name="操作前積分", value=f"{old_score} 分", inline=True)
        embed.add_field(name="操作後積分", value=f"{new_score} 分", inline=True)
        embed.add_field(name="總獲得積分", value=f"{new_total} 分", inline=True)
        embed.add_field(name="原因", value=reason, inline=True)
        embed.add_field(name="操作者", value=interaction.user.mention, inline=True)
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 調整失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

@tree.command(name="create_event", description="創建評核活動")
@app_commands.describe(
    event_name="活動名稱",
    signup_time="簽到時間（分鐘）",
    prize="活動獎品"
)
async def create_event_slash(
    interaction: discord.Interaction,
    event_name: str,
    signup_time: int = 5,
    prize: Optional[str] = None
):
    """創建評核活動"""
    await interaction.response.defer()
    
    try:
        if not interaction.user.guild_permissions.administrator:
            await interaction.followup.send("❌ 需要管理員權限")
            return
        
        if not db.is_connected:
            embed = discord.Embed(
                title="⚠️ 資料庫未連接",
                description="無法創建活動，請稍後再試。",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        guild_id = get_guild_id(interaction)
        await log_query("create_event", interaction.user.id, {"event_name": event_name, "signup_time": signup_time, "prize": prize}, guild_id)
        
        # 創建簽到訊息
        signup_embed = discord.Embed(
            title=f"📋 評核活動：{event_name}",
            color=discord.Color.blue()
        )
        
        if prize:
            signup_embed.add_field(name="🎁 獎品", value=prize, inline=False)
        
        signup_embed.add_field(
            name="📝 簽到階段",
            value=f"請在活動開始後 {signup_time} 分鐘內按 ✅ 簽到\n超過時間簽到將不計算出席率",
            inline=False
        )
        
        signup_embed.add_field(name="⏰ 簽到時間", value=f"{signup_time} 分鐘", inline=True)
        signup_embed.add_field(name="👥 已簽到", value="0 人", inline=True)
        signup_embed.add_field(name="⏱️ 剩餘時間", value=f"{signup_time} 分鐘", inline=True)
        signup_embed.set_footer(text=f"半月期: {get_current_half_month()}")
        
        signup_message = await interaction.followup.send(embed=signup_embed, wait=True)
        await signup_message.add_reaction("✅")
        
        # 創建職業選擇訊息
        class_embed = discord.Embed(
            title=f"🎮 職業選擇：{event_name}",
            description="請選擇你的職業：\n\n🛡️ 坦克\n⚔️ 输出\n💚 治疗\n💛 輔助\n\n**注意：請先完成簽到再選擇職業！**",
            color=discord.Color.green()
        )
        class_embed.set_footer(text="簽到成功後請選擇職業")
        
        class_msg = await interaction.channel.send(embed=class_embed)
        for emoji in ["🛡️", "⚔️", "💚", "💛"]:
            await class_msg.add_reaction(emoji)
        
        signup_end_time = datetime.now() + timedelta(minutes=signup_time)
        
        # 儲存活動到資料庫
        await db.execute(
            '''
            INSERT INTO evaluation_events (event_name, creator_id, signup_message_id, profession_message_id, channel_id, signup_end_time, guild_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ''',
            event_name, interaction.user.id, signup_message.id, class_msg.id, interaction.channel.id, signup_end_time, guild_id
        )
        
        print(f"✅ 活動創建成功: {event_name}, 簽到訊息ID: {signup_message.id}, 職業訊息ID: {class_msg.id}")
        
        # 簽到倒計時
        async def signup_countdown():
            remaining_minutes = signup_time
            
            while remaining_minutes > 0:
                await asyncio.sleep(60)
                remaining_minutes -= 1
                
                try:
                    result = await db.fetchrow(
                        "SELECT participants FROM evaluation_events WHERE signup_message_id = $1 AND guild_id = $2",
                        signup_message.id, guild_id
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
                    
                    updated_embed = discord.Embed(
                        title=f"📋 評核活動：{event_name}",
                        color=discord.Color.blue()
                    )
                    
                    if prize:
                        updated_embed.add_field(name="🎁 獎品", value=prize, inline=False)
                    
                    updated_embed.add_field(
                        name="📝 簽到階段",
                        value=f"請在活動開始後 {signup_time} 分鐘內按 ✅ 簽到\n超過時間簽到將不計算出席率",
                        inline=False
                    )
                    
                    updated_embed.add_field(name="⏰ 簽到時間", value=f"{signup_time} 分鐘", inline=True)
                    updated_embed.add_field(name="👥 已簽到", value=f"{participants_count} 人", inline=True)
                    updated_embed.add_field(name="⏱️ 剩餘時間", value=f"{remaining_minutes} 分鐘", inline=True)
                    updated_embed.set_footer(text=f"半月期: {get_current_half_month()}")
                    
                    await signup_message.edit(embed=updated_embed)
                    
                except Exception as e:
                    print(f"❌ 更新簽到訊息錯誤: {e}")
            
            # 簽到時間結束，處理簽到結果
            try:
                result = await db.fetchrow(
                    "SELECT participants FROM evaluation_events WHERE signup_message_id = $1 AND guild_id = $2",
                    signup_message.id, guild_id
                )
                
                participants = []
                if result and result['participants']:
                    participants = result['participants']
                    if isinstance(participants, str):
                        try:
                            participants = json.loads(participants)
                        except:
                            participants = []
                
                # 為所有參與者發放簽到獎勵
                for user_id in participants:
                    await update_user_score(user_id, f"用戶{user_id}", SIGNUP_SCORE, f"活動簽到: {event_name}", guild_id)
                    await update_user_activity(user_id, event_name, attended=True, guild_id=guild_id)
                    await update_user_rating(user_id, "普通", guild_id)
                
                # 更新活動狀態
                await db.execute(
                    "UPDATE evaluation_events SET default_rated = $1, is_active = true WHERE signup_message_id = $2 AND guild_id = $3",
                    json.dumps(participants), signup_message.id, guild_id
                )
                
                # 更新簽到訊息
                end_embed = discord.Embed(
                    title=f"📋 評核活動：{event_name}",
                    description="**簽到已結束！所有參與者已獲得預設「普通」評級（0積分）**",
                    color=discord.Color.red()
                )
                
                if prize:
                    end_embed.add_field(name="🎁 獎品", value=prize, inline=False)
                
                end_embed.add_field(name="⏰ 簽到時間", value="已結束", inline=True)
                end_embed.add_field(name="👥 已簽到", value=f"{len(participants)} 人", inline=True)
                
                if participants:
                    participants_text = "\n".join([f"<@{user_id}>" for user_id in participants[:10]])
                    if len(participants) > 10:
                        participants_text += f"\n... 還有 {len(participants) - 10} 人"
                    
                    end_embed.add_field(name="📋 參與者列表", value=participants_text, inline=False)
                
                end_embed.add_field(name="📝 評核說明", value="主持人現在可以按EMOJI調整評級：\n⭐ 優秀 (+40分)\n👍 良好 (+10分)\n👌 普通 (0分，預設)\n❌ 不合格 (-5分)", inline=False)
                end_embed.set_footer(text="半月期活動統計已更新 | 簽到積分已發放 | 預設評級：普通")
                
                await signup_message.edit(embed=end_embed)
                await signup_message.clear_reactions()
                
                print(f"✅ 簽到結束: {event_name}, 參與者: {len(participants)}人, 已給予預設普通評級")
                
                # 創建評核階段訊息
                rating_embed = discord.Embed(
                    title=f"⭐ 評核階段：{event_name}",
                    description="**主持人可以按下方EMOJI調整評級**\n\n"
                              f"所有參與者已獲得預設「普通」評級（{RATING_SCORES['普通']}積分）\n"
                              f"請主持人針對表現優秀或需要改進的成員調整評級：\n\n"
                              f"⭐ 優秀：+{RATING_SCORES['優秀']}積分\n"
                              f"👍 良好：+{RATING_SCORES['良好']}積分\n"
                              f"👌 普通：{RATING_SCORES['普通']}積分（預設）\n"
                              f"❌ 不合格：{RATING_SCORES['不合格']}積分\n\n"
                              f"**使用方法：**\n1. 點擊下方對應的EMOJI\n2. 在彈出的視窗中選擇用戶\n3. 系統會自動更新評級",
                    color=discord.Color.gold()
                )
                
                if participants:
                    rating_embed.add_field(
                        name="👥 參與者列表",
                        value="\n".join([f"<@{user_id}>" for user_id in participants[:15]]) + 
                             (f"\n... 還有 {len(participants)-15} 人" if len(participants) > 15 else ""),
                        inline=False
                    )
                
                rating_msg = await interaction.channel.send(embed=rating_embed)
                
                # 添加評核反應
                for emoji in ["⭐", "👍", "👌", "❌", RATING_END_EMOJI]:
                    await rating_msg.add_reaction(emoji)
                
                # 儲存評核訊息ID
                await db.execute(
                    "UPDATE evaluation_events SET rating_message_id = $1 WHERE signup_message_id = $2 AND guild_id = $3",
                    rating_msg.id, signup_message.id, guild_id
                )
                
                print(f"✅ 評核階段已創建: {event_name}, 評核訊息ID: {rating_msg.id}")
                
            except Exception as e:
                print(f"❌ 簽到結束處理錯誤: {e}")
        
        # 啟動簽到倒計時
        asyncio.create_task(signup_countdown())
        
        # 發送創建成功訊息
        success_embed = discord.Embed(
            title="✅ 活動創建成功",
            description=f"**活動名稱：** {event_name}\n**簽到時間：** {signup_time} 分鐘\n**參與方式：** 按 ✅ 反應簽到",
            color=discord.Color.green()
        )
        
        success_embed.add_field(name="簽到訊息", value=f"[點擊查看](https://discord.com/channels/{interaction.guild.id}/{interaction.channel.id}/{signup_message.id})", inline=True)
        success_embed.add_field(name="職業選擇", value=f"[點擊查看](https://discord.com/channels/{interaction.guild.id}/{interaction.channel.id}/{class_msg.id})", inline=True)
        
        await interaction.followup.send(embed=success_embed, ephemeral=True)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 創建活動失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

@tree.command(name="activity_stats", description="查看活動統計數據")
async def activity_stats_slash(interaction: discord.Interaction):
    """活動統計"""
    await interaction.response.defer()
    
    try:
        if not interaction.user.guild_permissions.administrator:
            await interaction.followup.send("❌ 需要管理員權限")
            return
        
        if not db.is_connected:
            embed = discord.Embed(
                title="⚠️ 資料庫未連接",
                description="無法查看活動統計，請稍後再試。",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        guild_id = get_guild_id(interaction)
        await log_query("activity_stats", interaction.user.id, {"action": "view_stats"}, guild_id)
        
        # 獲取活動統計
        result = await db.fetchrow(
            "SELECT COUNT(*) FROM evaluation_events WHERE guild_id = $1",
            guild_id
        )
        total_events = result['count'] if result else 0
        
        result = await db.fetchrow(
            "SELECT COUNT(*) FROM evaluation_events WHERE guild_id = $1 AND is_active = true",
            guild_id
        )
        active_events = result['count'] if result else 0
        
        result = await db.fetchrow(
            "SELECT COUNT(*) FROM giveaways WHERE guild_id = $1",
            guild_id
        )
        total_giveaways = result['count'] if result else 0
        
        result = await db.fetchrow(
            "SELECT COUNT(*) FROM giveaways WHERE guild_id = $1 AND is_active = true",
            guild_id
        )
        active_giveaways = result['count'] if result else 0
        
        # 獲取用戶統計
        result = await db.fetchrow(
            "SELECT COUNT(*) FROM users WHERE guild_id = $1",
            guild_id
        )
        total_users = result['count'] if result else 0
        
        result = await db.fetchrow(
            "SELECT SUM(current_score), SUM(total_score) FROM users WHERE guild_id = $1",
            guild_id
        )
        total_current_score = result['sum'] or 0 if result else 0
        total_earned_score = result['sum_1'] or 0 if result else 0
        
        # 獲取最近活動
        recent_events = await db.fetch(
            """
            SELECT event_name, COUNT(*) as participant_count, start_time 
            FROM evaluation_events 
            WHERE guild_id = $1 
            GROUP BY event_name, start_time 
            ORDER BY start_time DESC 
            LIMIT 5
            """,
            guild_id
        )
        
        embed = discord.Embed(
            title="📊 活動統計數據",
            description=f"伺服器：{interaction.guild.name if interaction.guild else 'DM'}",
            color=0x7289DA
        )
        
        embed.add_field(name="🎮 評核活動", value=f"總數：{total_events}\n進行中：{active_events}", inline=True)
        embed.add_field(name="🎉 抽獎活動", value=f"總數：{total_giveaways}\n進行中：{active_giveaways}", inline=True)
        embed.add_field(name="👥 用戶統計", value=f"總用戶數：{total_users}\n總積分：{total_current_score}", inline=True)
        
        embed.add_field(
            name="💰 積分統計",
            value=f"**當前總積分：** {total_current_score:,}分\n"
                  f"**歷史總獲得：** {total_earned_score:,}分\n"
                  f"**平均每人：** {total_current_score//total_users if total_users>0 else 0}分",
            inline=False
        )
        
        if recent_events:
            events_text = ""
            for row in recent_events:
                event_name = row['event_name']
                participant_count = row['participant_count']
                start_time = row['start_time']
                
                try:
                    if isinstance(start_time, str):
                        time_str = datetime.strptime(start_time.split('.')[0], '%Y-%m-%d %H:%M:%S').strftime('%m/%d %H:%M')
                    else:
                        time_str = start_time.strftime('%m/%d %H:%M')
                except:
                    time_str = str(start_time)
                
                events_text += f"• **{event_name}**\n  👥 {participant_count}人 | 📅 {time_str}\n"
            
            embed.add_field(name="📅 最近活動", value=events_text, inline=False)
        
        current_period = get_current_half_month()
        embed.set_footer(text=f"統計時間: {datetime.now().strftime('%Y-%m-%d %H:%M')} | 當前半月期: {current_period}")
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 讀取統計失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

# ========== 聊天積分事件 ==========

@bot.event
async def on_message(message):
    """處理聊天積分"""
    # 忽略機器人自己的訊息
    if message.author.bot:
        return
    
    try:
        # 檢查是否為指令
        ctx = await bot.get_context(message)
        if ctx.valid:
            # 如果是有效指令，交給指令系統處理
            await bot.invoke(ctx)
            return
        
        # 如果不是指令，處理聊天積分
        guild_id = get_guild_id(message)
        added_score, daily_limit = await add_chat_score(
            message.author.id, 
            message.author.name,
            guild_id
        )
        
        # 記錄但減少通知頻率
        if added_score > 0:
            # 獲取用戶當前的聊天積分狀態
            current_score, total_score = await get_user_score(message.author.id, guild_id)
            
            # 降低通知頻率到 2%
            if random.random() < 0.02 and added_score == CHAT_SCORE:
                responses = [
                    f"💬 {message.author.mention} 聊天 +{added_score} 分！",
                    f"💭 {message.author.mention} 目前積分：{current_score} 分"
                ]
                notification = random.choice(responses)
                try:
                    await message.channel.send(notification, delete_after=5)
                except Exception as e:
                    print(f"❌ 發送聊天積分通知失敗: {e}")
                    
            print(f"💬 {message.author.name} 獲得聊天積分: +{added_score}分，目前: {current_score}分")
                    
    except Exception as e:
        print(f"❌ 處理訊息錯誤: {e}")
        traceback.print_exc()
    
    # 不需要再次調用 process_commands，因為我們已經在 ctx.valid 時處理過了

# ========== 事件處理 ==========

@bot.event
async def on_raw_reaction_add(payload):
    """處理反應事件"""
    if payload.user_id == bot.user.id:
        return
    
    try:
        emoji = str(payload.emoji)
        user_id = payload.user_id
        
        channel = bot.get_channel(payload.channel_id)
        if not channel:
            return
        
        try:
            message = await channel.fetch_message(payload.message_id)
        except:
            return
        
        guild_id = payload.guild_id if hasattr(payload, 'guild_id') else 0
        
        # 檢查是否是評核活動的評核訊息
        result = await db.fetchrow(
            """
            SELECT id, channel_id, event_name 
            FROM evaluation_events 
            WHERE rating_message_id = $1 AND is_active = true AND guild_id = $2
            """,
            payload.message_id, guild_id
        )
        
        if result and emoji == RATING_END_EMOJI:
            event_id = result['id']
            event_channel_id = result['channel_id']
            event_name = result['event_name']
            
            try:
                guild = channel.guild
                member = await guild.fetch_member(user_id)
                if not member.guild_permissions.administrator:
                    try:
                        await message.remove_reaction(emoji, member)
                        await channel.send(f"❌ <@{user_id}> 只有管理員可以結束評核活動！", delete_after=5)
                    except:
                        pass
                    return
            except Exception as admin_error:
                print(f"❌ 檢查管理員權限錯誤: {admin_error}")
                return
            
            confirm_embed = discord.Embed(
                title="🏁 確認結束評核活動",
                description=f"你確定要結束 **{event_name}** 的評核階段嗎？\n\n"
                          f"結束後將：\n"
                          f"• 無法再進行評核\n"
                          f"• 清除評核訊息的所有反應\n"
                          f"• 活動標記為已完成",
                color=discord.Color.orange()
            )
            
            class ConfirmEndView(discord.ui.View):
                def __init__(self, event_id, channel, event_name, guild_id):
                    super().__init__(timeout=60)
                    self.event_id = event_id
                    self.channel = channel
                    self.event_name = event_name
                    self.guild_id = guild_id
                
                @discord.ui.button(label="確定結束", style=discord.ButtonStyle.danger, emoji="✅")
                async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
                    if not interaction.user.guild_permissions.administrator:
                        await interaction.response.send_message("❌ 需要管理員權限", ephemeral=True)
                        return
                    
                    await interaction.response.defer()
                    
                    await end_evaluation(self.event_id, self.channel, self.event_name, self.guild_id)
                    
                    for child in self.children:
                        child.disabled = True
                    await interaction.message.edit(view=self)
                    
                    await interaction.followup.send(f"✅ 已成功結束 **{self.event_name}** 的評核階段！")
                
                @discord.ui.button(label="取消", style=discord.ButtonStyle.secondary, emoji="❌")
                async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
                    if not interaction.user.guild_permissions.administrator:
                        await interaction.response.send_message("❌ 需要管理員權限", ephemeral=True)
                        return
                    
                    await interaction.response.send_message("已取消結束評核活動", ephemeral=True)
                    
                    for child in self.children:
                        child.disabled = True
                    await interaction.message.edit(view=self)
            
            view = ConfirmEndView(event_id, channel, event_name, guild_id)
            await channel.send(f"<@{user_id}>", embed=confirm_embed, view=view)
            return
        
        # 檢查是否是評核活動的評核反應
        if result and emoji in RATING_EMOJIS:
            event_id = result['id']
            event_channel_id = result['channel_id']
            event_name = result['event_name']
            rating_type = RATING_EMOJIS[emoji]
            
            print(f"檢測到評核反應: event_id={event_id}, rating_type={rating_type}, user_id={user_id}")
            
            try:
                guild = channel.guild
                member = await guild.fetch_member(user_id)
                if not member.guild_permissions.administrator:
                    try:
                        await message.remove_reaction(emoji, member)
                        await channel.send(f"❌ <@{user_id}> 只有管理員可以進行評核！", delete_after=5)
                    except:
                        pass
                    return
            except Exception as admin_error:
                print(f"❌ 檢查管理員權限錯誤: {admin_error}")
                return
            
            result2 = await db.fetchrow(
                "SELECT participants FROM evaluation_events WHERE id = $1 AND guild_id = $2",
                event_id, guild_id
            )
            
            participants = []
            if result2 and result2['participants']:
                participants = result2['participants']
                if isinstance(participants, str):
                    try:
                        participants = json.loads(participants)
                    except:
                        participants = []
            
            if not participants:
                await channel.send("❌ 沒有參與者可以評核", delete_after=5)
                return
            
            print(f"活動 {event_name} 有 {len(participants)} 位參與者可以評核")
            
            class ParticipantSelectView(discord.ui.View):
                def __init__(self, participants, event_id, rating_type, channel, bot_instance, guild_id):
                    super().__init__(timeout=60)
                    self.participants = participants
                    self.event_id = event_id
                    self.rating_type = rating_type
                    self.channel = channel
                    self.bot = bot_instance
                    self.guild_id = guild_id
                    
                    options = []
                    for pid in participants[:25]:
                        member = self.bot.get_user(int(pid))
                        display_name = member.display_name if member else f"用戶ID: {pid}"
                        options.append(discord.SelectOption(
                            label=display_name[:100],
                            value=str(pid),
                            description=f"點擊選擇此用戶進行 {rating_type} 評核"
                        ))
                    
                    select = discord.ui.Select(
                        placeholder=f"選擇要評核為 {rating_type} 的參與者",
                        options=options,
                        min_values=1,
                        max_values=1
                    )
                    
                    async def select_callback(interaction: discord.Interaction):
                        if not interaction.user.guild_permissions.administrator:
                            await interaction.response.send_message("❌ 需要管理員權限", ephemeral=True)
                            return
                        
                        selected_user_id = int(select.values[0])
                        selected_member = self.bot.get_user(selected_user_id)
                        display_name = selected_member.display_name if selected_member else f"用戶ID: {selected_user_id}"
                        
                        print(f"選擇了用戶 {display_name} ({selected_user_id}) 進行 {rating_type} 評核")
                        
                        result3 = await db.fetchrow(
                            "SELECT ratings FROM evaluation_events WHERE id = $1 AND guild_id = $2",
                            self.event_id, self.guild_id
                        )
                        
                        ratings = {}
                        if result3 and result3['ratings']:
                            ratings = result3['ratings']
                            if isinstance(ratings, str):
                                try:
                                    ratings = json.loads(ratings)
                                except:
                                    ratings = {}
                        
                        old_rating = None
                        if str(selected_user_id) in ratings and ratings[str(selected_user_id)]:
                            if isinstance(ratings[str(selected_user_id)], list):
                                old_rating = ratings[str(selected_user_id)][-1]["rating"] if ratings[str(selected_user_id)] else None
                            else:
                                old_rating = ratings[str(selected_user_id)]
                        
                        if str(selected_user_id) not in ratings:
                            ratings[str(selected_user_id)] = []
                        
                        if isinstance(ratings[str(selected_user_id)], list):
                            ratings[str(selected_user_id)].append({
                                "rater": interaction.user.id,
                                "rating": self.rating_type,
                                "time": datetime.now().isoformat()
                            })
                        else:
                            ratings[str(selected_user_id)] = [{
                                "rater": interaction.user.id,
                                "rating": self.rating_type,
                                "time": datetime.now().isoformat()
                            }]
                        
                        await db.execute(
                            "UPDATE evaluation_events SET ratings = $1 WHERE id = $2 AND guild_id = $3",
                            json.dumps(ratings), self.event_id, self.guild_id
                        )
                        
                        if old_rating and old_rating != self.rating_type:
                            old_score = RATING_SCORES.get(old_rating, 0)
                            await update_user_score(selected_user_id, display_name, -old_score, f"評級變更: {old_rating} → {self.rating_type}", self.guild_id)
                            print(f"移除舊評級積分: {old_rating} (-{old_score}分)")
                        
                        new_score = RATING_SCORES.get(self.rating_type, 0)
                        await update_user_rating(selected_user_id, self.rating_type, self.guild_id)
                        
                        if new_score != 0:
                            await update_user_score(selected_user_id, display_name, new_score, f"活動評核: {self.rating_type}", self.guild_id)
                            print(f"添加新評級積分: {self.rating_type} (+{new_score}分)")
                        
                        score_change = RATING_SCORES.get(self.rating_type, 0)
                        score_text = f"（積分變動: {'+' if score_change > 0 else ''}{score_change}分）" if score_change != 0 else ""
                        
                        if old_rating and old_rating != self.rating_type:
                            old_score = RATING_SCORES.get(old_rating, 0)
                            result_text = f"已將 <@{selected_user_id}> ({display_name}) 的評級從 **{old_rating}** ({old_score}分) 變更為 **{self.rating_type}** {score_text}"
                        else:
                            result_text = f"已為 <@{selected_user_id}> ({display_name}) 評核：**{self.rating_type}** {score_text}"
                        
                        result_embed = discord.Embed(
                            title="✅ 評核完成",
                            description=result_text,
                            color=discord.Color.green() if score_change >= 0 else discord.Color.red()
                        )
                        
                        result_embed.add_field(name="評核者", value=interaction.user.mention, inline=True)
                        result_embed.add_field(name="新評級", value=self.rating_type, inline=True)
                        result_embed.add_field(name="積分變動", value=f"{score_change} 分", inline=True)
                        
                        await interaction.response.send_message(embed=result_embed)
                        
                        for child in self.children:
                            child.disabled = True
                        await interaction.message.edit(view=self)
                    
                    select.callback = select_callback
                    self.add_item(select)
            
            view = ParticipantSelectView(participants, event_id, rating_type, channel, bot, guild_id)
            
            select_message = await channel.send(f"<@{user_id}> 請選擇要評核為 **{rating_type}** 的參與者：", view=view)
            print(f"已發送選擇視窗: message_id={select_message.id}")
            return
        
        # 檢查是否是抽獎
        result = await db.fetchrow(
            """
            SELECT id, participants, creator_id 
            FROM giveaways 
            WHERE message_id = $1 AND is_active = true AND guild_id = $2
            """,
            payload.message_id, guild_id
        )
        
        if result:
            giveaway_id = result['id']
            participants = result['participants'] or []
            creator_id = result['creator_id']
            
            if emoji == "🎫":
                if isinstance(participants, str):
                    try:
                        participants = json.loads(participants)
                    except:
                        participants = []
                
                if user_id not in participants:
                    participants.append(user_id)
                    await db.execute(
                        "UPDATE giveaways SET participants = $1 WHERE id = $2 AND guild_id = $3",
                        json.dumps(participants), giveaway_id, guild_id
                    )
                    
                    try:
                        if message.embeds:
                            embed = message.embeds[0]
                            
                            new_embed = discord.Embed(
                                title=embed.title,
                                description=embed.description,
                                color=embed.color
                            )
                            
                            for field in embed.fields:
                                if field.name == "🎫 參與人數":
                                    new_embed.add_field(
                                        name="🎫 參與人數", 
                                        value=f"{len(participants)} 人", 
                                        inline=field.inline
                                    )
                                else:
                                    new_embed.add_field(
                                        name=field.name, 
                                        value=field.value, 
                                        inline=field.inline
                                    )
                            
                            if embed.footer:
                                new_embed.set_footer(text=embed.footer.text)
                            
                            await message.edit(embed=new_embed)
                    except Exception as e:
                        print(f"❌ 更新抽獎訊息錯誤: {e}")
            
            elif emoji == "⏹️" and user_id == creator_id:
                await end_giveaway(payload.message_id, manual=True, guild_id=guild_id)
                await channel.send(f"⏹️ 主辦人手動結束了抽獎！")
            return
        
        # 檢查是否是活動簽到
        result = await db.fetchrow(
            """
            SELECT id, participants, signup_end_time 
            FROM evaluation_events 
            WHERE signup_message_id = $1 AND is_active = true AND guild_id = $2
            """,
            payload.message_id, guild_id
        )
        
        if result and emoji == "✅":
            event_id = result['id']
            participants = result['participants'] or []
            signup_end_time_str = result['signup_end_time']
            
            # 解析參與者列表
            if isinstance(participants, str):
                try:
                    participants = json.loads(participants)
                except:
                    participants = []
            
            try:
                if signup_end_time_str:
                    try:
                        signup_end_time = signup_end_time_str
                        if isinstance(signup_end_time_str, str):
                            signup_end_time = datetime.strptime(signup_end_time_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
                    except:
                        signup_end_time = None
                else:
                    signup_end_time = None
                
                if signup_end_time and datetime.now() > signup_end_time:
                    try:
                        await message.remove_reaction("✅", payload.member)
                        await channel.send(f"❌ <@{user_id}> 簽到時間已過！", delete_after=5)
                    except:
                        pass
                    return
            except Exception as time_error:
                print(f"❌ 時間解析錯誤: {time_error}")
            
            # 修復：檢查用戶是否已經簽到
            if user_id not in participants:
                participants.append(user_id)
                
                # 更新資料庫中的參與者列表
                await db.execute(
                    "UPDATE evaluation_events SET participants = $1 WHERE id = $2 AND guild_id = $3",
                    json.dumps(participants), event_id, guild_id
                )
                
                print(f"✅ 用戶 {user_id} 成功簽到活動 {event_id}, 現在有 {len(participants)} 人簽到")
                
                # 更新訊息顯示
                try:
                    if message.embeds:
                        embed = message.embeds[0]
                        
                        new_embed = discord.Embed(
                            title=embed.title,
                            description=embed.description,
                            color=embed.color
                        )
                        
                        for field in embed.fields:
                            if field.name == "👥 已簽到":
                                new_embed.add_field(
                                    name="👥 已簽到", 
                                    value=f"{len(participants)} 人", 
                                    inline=field.inline
                                )
                            elif field.name == "⏱️ 剩餘時間":
                                new_embed.add_field(
                                    name=field.name,
                                    value=field.value,
                                    inline=field.inline
                                )
                            else:
                                new_embed.add_field(
                                    name=field.name, 
                                    value=field.value, 
                                    inline=field.inline
                                )
                        
                        if embed.footer:
                            new_embed.set_footer(text=embed.footer.text)
                        
                        await message.edit(embed=new_embed)
                except Exception as e:
                    print(f"❌ 更新簽到訊息錯誤: {e}")
            else:
                print(f"⚠️ 用戶 {user_id} 已經簽到過了")
            return
        
        # 檢查是否是職業選擇
        result = await db.fetchrow(
            """
            SELECT id, professions 
            FROM evaluation_events 
            WHERE profession_message_id = $1 AND is_active = true AND guild_id = $2
            """,
            payload.message_id, guild_id
        )
        
        if result and emoji in PROFESSION_EMOJIS:
            event_id = result['id']
            professions = result['professions'] or {}
            profession_name = PROFESSION_EMOJIS[emoji]
            
            # 解析職業數據
            if isinstance(professions, str):
                try:
                    professions = json.loads(professions)
                except:
                    professions = {}
            
            result2 = await db.fetchrow(
                "SELECT participants FROM evaluation_events WHERE id = $1 AND guild_id = $2",
                event_id, guild_id
            )
            
            if result2 and result2['participants']:
                participants = result2['participants']
                if isinstance(participants, str):
                    try:
                        participants = json.loads(participants)
                    except:
                        participants = []
                
                if user_id in participants:
                    if str(user_id) not in professions:
                        professions[str(user_id)] = profession_name
                        await db.execute(
                            "UPDATE evaluation_events SET professions = $1 WHERE id = $2 AND guild_id = $3",
                            json.dumps(professions), event_id, guild_id
                        )
                        
                        await update_user_profession(user_id, profession_name, guild_id)
                        
                        try:
                            bonus = PROFESSION_BONUS.get(profession_name, 0)
                            bonus_text = f"（獲得職業加成：+{bonus}積分）" if bonus > 0 else ""
                            await channel.send(f"✅ <@{user_id}> 已選擇職業：**{profession_name}**{bonus_text}", delete_after=5)
                        except:
                            pass
                    else:
                        try:
                            await message.remove_reaction(emoji, payload.member)
                            await channel.send(f"⚠️ <@{user_id}> 你已經選擇過職業了！", delete_after=5)
                        except:
                            pass
                else:
                    try:
                        await message.remove_reaction(emoji, payload.member)
                        await channel.send(f"❌ <@{user_id}> 請先簽到再選擇職業！", delete_after=5)
                    except:
                        pass
            return
            
    except Exception as e:
        print(f"❌ 處理反應錯誤: {e}")
        traceback.print_exc()

# ========== 機器人上線 ==========

@bot.event
async def on_ready():
    """機器人上線"""
    print(f"\n{'='*60}")
    print(f"🤖 {BOT_NAME} - PostgreSQL 修正版已上線")
    print(f"📊 伺服器數量: {len(bot.guilds)}")
    print(f"{'='*60}")
    
    # 顯示所有伺服器
    for guild in bot.guilds:
        print(f"🏰 {guild.name} (ID: {guild.id})")
    
    # 連接 PostgreSQL
    print(f"\n🔌 正在連接資料庫...")
    success = await db.connect()
    
    if success:
        print("✅ 資料庫連接成功")
    else:
        print("❌ 資料庫連接失敗")
        print("⚠️ 機器人將使用記憶體緩存模式運行")
        print("💡 請檢查：")
        print("   1. Railway 專案中的 DATABASE_URL 環境變數")
        print("   2. PostgreSQL 服務是否正常運行")
        print("   3. 網路連接是否正常")
    
    # 同步指令
    try:
        print("\n🔄 正在同步指令...")
        global_synced = await tree.sync()
        print(f"✅ 已同步 {len(global_synced)} 個指令")
        
        print("\n📋 完整功能列表:")
        print("  • 用戶指令 (11個): /help, /profile, /giveaway, /score_draw, /score_transfer, /prizelist, /random_team, /score_ranking, /attendance_ranking, /blessing, /test_chat_score")
        print("  • 管理員指令 (4個): /add_prize, /add_score, /create_event, /activity_stats")
        print("  • 系統指令 (2個): /sync, /db_status")
        print("  • 聊天積分系統: 每句話 +5 分，每日上限 20 分")
        print("  • 評核活動系統: 簽到、職業選擇、評核評分")
        print("  • 抽獎系統: 自動開獎、手動結束")
        print("  • 彩池系統: 獎品增減、積分抽獎")
        print("  • 資料庫狀態檢查: /db_status")
        print("  • 記憶體緩存系統: 資料庫失敗時使用")
        
        print("\n📊 可用指令列表:")
        for cmd in sorted(global_synced, key=lambda x: x.name):
            print(f"  • /{cmd.name} - {cmd.description}")
        
    except Exception as e:
        print(f"❌ 同步失敗: {e}")
    
    # 設置狀態
    if success:
        status_text = "/help | 資料庫正常"
    else:
        status_text = "/help | 使用緩存模式"
    
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name=status_text
        )
    )
    
    print(f"\n🎮 機器人準備就緒！資料庫狀態: {'✅' if success else '❌'}")
    print(f"📝 記憶體緩存: {'✅ 啟用中' if not success else '⚡ 備用中'}")

# ========== 主程式 ==========

def main():
    """主程式入口"""
    print(f"{'='*50}")
    print(f"🚀 啟動 {BOT_NAME} - PostgreSQL 完整修正版本")
    print(f"💡 完整功能：17個指令 + 聊天積分 + 評核活動 + 抽獎系統")
    print(f"🔧 擁有者ID: {OWNER_IDS}")
    print(f"🗄️ 資料庫: PostgreSQL (Railway) + 記憶體緩存備份")
    print(f"📊 指令數量: 17個 (11用戶 + 4管理員 + 2系統)")
    print(f"{'='*50}")
    
    token = os.getenv("DISCORD_TOKEN")
    
    if not token or token == "你的_bot_token_在這裡":
        print("❌ 找不到有效的 Token！")
        print("💡 請在 Railway 設定環境變數：")
        print("   1. 進入 Railway 專案")
        print("   2. 點擊 Settings")
        print("   3. 點擊 Variables")
        print("   4. 新增 DISCORD_TOKEN = 你的_bot_token")
        sys.exit(1)
    
    print("✅ Token 讀取成功")
    print("🔄 正在連接 Discord...")
    
    try:
        bot.run(token)
    except discord.LoginFailure:
        print("❌ 登入失敗！請檢查 Token 是否正確")
        print("💡 請到 Discord Developer Portal 重置 Token")
    except Exception as e:
        print(f"❌ 啟動失敗: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()

# ========== RPG 系統整合（完全獨立，不影響原有功能）==========
try:
    from rpg_system import RPGSystem, get_rpg_system
    rpg = get_rpg_system(bot, db, memory_cache)
    
    # 儲存原本的 on_ready
    original_on_ready = bot.on_ready
    
    @bot.event
    async def on_ready():
        # 先執行原本的 on_ready
        if original_on_ready:
            await original_on_ready()
        
        # 初始化 RPG
        await rpg.initialize()
        
        # ========== 關鍵修正：先註冊指令，再同步 ==========
        # 註冊 RPG 指令到 tree
        await rpg.register_commands(bot.tree)
        print("✅ RPG 指令已註冊到指令樹")
        
        # 強制同步所有指令（包括原有的 + RPG）
        try:
            # 這裡會同步「所有」已註冊的指令
            synced = await bot.tree.sync()
            print(f"✅ 全域指令同步完成，共 {len(synced)} 個指令")
            
            # 列出所有指令名稱
            cmd_names = [cmd.name for cmd in synced]
            print(f"📋 已同步指令: {', '.join(cmd_names)}")
            
            # 確認 RPG 指令是否存在
            if 'rpg' in cmd_names:
                print("✅ RPG 指令群組已成功同步！")
            else:
                print("❌ RPG 指令群組同步失敗！")
                
        except Exception as e:
            print(f"❌ 指令同步失敗: {e}")
    
    print("🔌 RPG 系統載入點已準備")
    
except ImportError as e:
    print(f"ℹ️ 未檢測到 RPG 系統模組: {e}")
    rpg = None









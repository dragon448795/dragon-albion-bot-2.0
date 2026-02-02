#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
小雲ALBION機械人 - PostgreSQL完整版本（數據持久化）
包含所有13個原指令 + 新增/blessing指令 + 聊天積分系統
修復獎品增減功能，使用Railway PostgreSQL
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
CHAT_SCORE = 5     # 每句話積分
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
    
    async def connect(self):
        """連接 PostgreSQL 資料庫"""
        try:
            # Railway 會自動提供 DATABASE_URL 環境變數
            database_url = os.getenv('DATABASE_URL')
            
            if not database_url:
                print("⚠️ 找不到 DATABASE_URL，將無法保存數據！")
                return False
            
            # 修正URL格式
            if database_url.startswith('postgres://'):
                database_url = database_url.replace('postgres://', 'postgresql://', 1)
            
            print(f"🔄 正在連接 PostgreSQL...")
            self.pool = await asyncpg.create_pool(
                database_url, 
                min_size=5, 
                max_size=20,
                command_timeout=60
            )
            
            await self.init_db()
            print("✅ PostgreSQL 連接成功")
            return True
            
        except Exception as e:
            print(f"❌ PostgreSQL 連接失敗: {e}")
            return False
    
    async def init_db(self):
        """初始化資料庫表格"""
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
    
    async def execute(self, query, *args):
        """執行 SQL 查詢"""
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)
    
    async def fetch(self, query, *args):
        """執行查詢並返回結果"""
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *args)
    
    async def fetchrow(self, query, *args):
        """執行查詢並返回單行結果"""
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    
    async def fetchval(self, query, *args):
        """執行查詢並返回單個值"""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, *args)

db = Database()

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
    result = await db.fetchrow(
        "SELECT current_score, total_score FROM users WHERE user_id = $1 AND guild_id = $2",
        user_id, guild_id
    )
    
    if result:
        return result['current_score'], result['total_score']
    return 0, 0

async def update_user_score(user_id, username, amount, reason="", guild_id=0):
    """更新用戶積分"""
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
            
    except Exception as e:
        print(f"更新用戶積分錯誤: {e}")

async def add_chat_score(user_id, username, guild_id=0):
    """添加聊天積分"""
    try:
        today = datetime.now().date()
        
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
                
    except Exception as e:
        print(f"添加聊天積分錯誤: {e}")
        return 0, DAILY_CHAT_LIMIT

async def get_user_profile(user_id, guild_id=0):
    """獲取用戶完整資料"""
    result = await db.fetchrow(
        "SELECT current_score, total_score, join_date, profession_counts, activity_stats, rating_stats FROM users WHERE user_id = $1 AND guild_id = $2",
        user_id, guild_id
    )
    
    if result:
        current_score = result['current_score']
        total_score = result['total_score']
        join_date = result['join_date']
        profession_counts = result['profession_counts'] or {}
        activity_stats = result['activity_stats'] or {}
        rating_stats = result['rating_stats'] or {}
        
        try:
            if isinstance(join_date, str):
                join_date_str = datetime.strptime(join_date.split('.')[0], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
            else:
                join_date_str = join_date.strftime('%Y-%m-%d')
        except:
            join_date_str = str(join_date)
        
        return {
            'user_id': user_id,
            'current_score': current_score,
            'total_score': total_score,
            'join_date': join_date_str,
            'profession_counts': profession_counts,
            'activity_stats': activity_stats,
            'rating_stats': rating_stats
        }
    
    return None

async def update_user_profession(user_id, profession, guild_id=0):
    """更新用戶職業統計"""
    try:
        result = await db.fetchrow(
            "SELECT profession_counts, username FROM users WHERE user_id = $1 AND guild_id = $2",
            user_id, guild_id
        )
        
        if result:
            profession_counts = result['profession_counts'] or {}
            username = result['username']
            
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
                profession_counts, user_id, guild_id
            )
            
    except Exception as e:
        print(f"更新職業統計錯誤: {e}")

async def update_user_activity(user_id, event_name, attended=True, guild_id=0):
    """更新用戶活動統計"""
    try:
        result = await db.fetchrow(
            "SELECT activity_stats FROM users WHERE user_id = $1 AND guild_id = $2",
            user_id, guild_id
        )
        
        if result:
            activity_stats = result['activity_stats'] or {}
            
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
                activity_stats, user_id, guild_id
            )
            
    except Exception as e:
        print(f"更新活動統計錯誤: {e}")

async def update_user_rating(user_id, rating_type, guild_id=0):
    """更新用戶評核統計"""
    try:
        result = await db.fetchrow(
            "SELECT rating_stats FROM users WHERE user_id = $1 AND guild_id = $2",
            user_id, guild_id
        )
        
        if result:
            rating_stats = result['rating_stats'] or {}
            
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
                rating_stats, user_id, guild_id
            )
            
    except Exception as e:
        print(f"更新評核統計錯誤: {e}")

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
        
        channel = bot.get_channel(channel_id)
        
        if not channel:
            return
        
        try:
            message = await channel.fetch_message(message_id)
        except:
            return
        
        if participants:
            if len(participants) <= winner_count:
                winners_list = participants
            else:
                winners_list = random.sample(participants, winner_count)
            
            await db.execute(
                "UPDATE giveaways SET winners = $1, is_active = false WHERE id = $2",
                winners_list, giveaway_id
            )
            
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
            
            for winner_id in winners_list:
                await channel.send(f"🎉 恭喜 <@{winner_id}> 獲得了 **{prize}**！")
        else:
            new_embed = discord.Embed(
                title="🎉 抽獎已結束",
                description="無人參與抽獎" + ("（手動結束）" if manual else ""),
                color=0xFF0000
            )
            await message.edit(embed=new_embed)
            await message.clear_reactions()
        
    except Exception as e:
        print(f"結束抽獎錯誤: {e}")

async def end_evaluation(event_id, channel, event_name, guild_id=0):
    """結束評核活動"""
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
                    latest_rating = rating_list[-1]["rating"]
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
            print(f"更新評核訊息錯誤: {e}")
        
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
        print(f"結束評核活動錯誤: {e}")

async def log_query(query_type: str, user_id: int, parameters: dict, guild_id: int = 0):
    """記錄查詢日誌"""
    try:
        await db.execute(
            "INSERT INTO query_logs (query_type, user_id, parameters, guild_id) VALUES ($1, $2, $3, $4)",
            query_type, user_id, json.dumps(parameters), guild_id
        )
    except Exception as e:
        print(f"記錄查詢日誌錯誤: {e}")

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
        name="👤 用戶指令 (10個)",
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
            "`/blessing` - 測試今日運程 **(新增)**"
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
        name="💰 積分系統",
        value=(
            "**聊天獎勵：** 每句話 +5 分，每日上限 20 分 **(新增)**\n"
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
    
    embed.set_footer(text=f"總指令數: 14個 | 版本: PostgreSQL 持久化版本")
    await interaction.response.send_message(embed=embed)

@tree.command(name="profile", description="查看我的數據")
async def profile_slash(interaction: discord.Interaction):
    """查看用戶資料"""
    await interaction.response.defer()
    
    try:
        user_id = interaction.user.id
        username = interaction.user.name
        guild_id = get_guild_id(interaction)
        
        await log_query("profile", user_id, {"action": "view_profile"}, guild_id)
        
        profile = await get_user_profile(user_id, guild_id)
        
        if not profile:
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
                'rating_stats': {}
            }
        
        current_score = profile['current_score']
        total_score = profile['total_score']
        join_date_str = profile['join_date']
        profession_counts = profile['profession_counts']
        activity_stats = profile['activity_stats']
        rating_stats = profile['rating_stats']
        
        current_period = get_current_half_month()
        period_data = activity_stats.get(current_period, {})
        total_events = period_data.get('total', 0)
        attended_events = period_data.get('attended', 0)
        attendance_rate = (attended_events / total_events * 100) if total_events > 0 else 0.0
        
        embed = discord.Embed(
            title=f"📊 {username} 的評核數據",
            color=0x43B581
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
        
        score_info = f"**當前積分：** {current_score} 分\n"
        score_info += f"**總獲得積分：** {total_score} 分\n"
        score_info += f"**可用積分：** {current_score} 分\n\n"
        score_info += f"**積分規則：**\n"
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
                        if result and result['participants']:
                            participants = result['participants']
                            participants_count = len(participants)
                        
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
                        
                        new_embed.set_footer(text=f"抽獎ID: {giveaway_id} | 主辦人: {creator_name}•{datetime.now().strftime('%Y-%m-%d %H:%M')}")
                        
                        await message.edit(embed=new_embed)
                        last_update = time.time()
                        
                    except Exception as e:
                        print(f"更新抽獎訊息錯誤: {e}")
            
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
        
        async with aiosqlite.connect(DB_NAME) as conn:
            await update_user_score(interaction.user.id, interaction.user.name, -amount, f"轉移給 {user.name}", guild_id)
            await update_user_score(user.id, user.name, amount, f"來自 {interaction.user.name} 的轉移", guild_id)
            
            await conn.execute('''
                INSERT INTO score_transfers (from_user_id, to_user_id, amount, reason, guild_id)
                VALUES (?, ?, ?, ?, ?)
            ''', (interaction.user.id, user.id, amount, reason or "無", guild_id))
            
            await conn.commit()
        
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

@tree.command(name="random_team", description="隨機分組")
@app_commands.describe(
    team_size="每組人數",
    team_count="組數"
)
async def random_team_slash(
    interaction: discord.Interaction,
    team_size: Optional[int] = None,
    team_count: Optional[int] = None
):
    """隨機分組"""
    await interaction.response.defer()
    
    try:
        guild_id = get_guild_id(interaction)
        await log_query("random_team", interaction.user.id, {"team_size": team_size, "team_count": team_count}, guild_id)
        
        if not interaction.guild:
            await interaction.followup.send("❌ 此指令只能在伺服器中使用")
            return
        
        embed = discord.Embed(
            title="👥 隨機分組",
            description="點擊 🎮 按鈕參加分組\n主持人點擊 ▶️ 按鈕開始分組",
            color=0x3498DB
        )
        
        if team_size:
            embed.add_field(name="每組人數", value=str(team_size), inline=True)
        if team_count:
            embed.add_field(name="組數", value=str(team_count), inline=True)
        
        embed.add_field(name="參加人數", value="0 人", inline=True)
        embed.set_footer(text="等待參加者...")
        
        await interaction.followup.send(embed=embed)
        message = await interaction.original_response()
        
        await message.add_reaction("🎮")
        await message.add_reaction("▶️")
        
        participants = []
        
        def check(reaction, user):
            return (
                user != bot.user and
                str(reaction.emoji) in ["🎮", "▶️"] and
                reaction.message.id == message.id
            )
        
        try:
            while True:
                reaction, user = await bot.wait_for('reaction_add', timeout=300.0, check=check)
                
                if str(reaction.emoji) == "🎮":
                    if user.id not in participants:
                        participants.append(user.id)
                        
                        new_embed = discord.Embed(
                            title="👥 隨機分組",
                            description="點擊 🎮 按鈕參加分組\n主持人點擊 ▶️ 按鈕開始分組",
                            color=0x3498DB
                        )
                        
                        if team_size:
                            new_embed.add_field(name="每組人數", value=str(team_size), inline=True)
                        if team_count:
                            new_embed.add_field(name="組數", value=str(team_count), inline=True)
                        
                        new_embed.add_field(name="參加人數", value=f"{len(participants)} 人", inline=True)
                        
                        if participants:
                            participants_text = ""
                            for i, pid in enumerate(participants[:10], 1):
                                participants_text += f"{i}. <@{pid}>\n"
                            if len(participants) > 10:
                                participants_text += f"\n... 還有 {len(participants) - 10} 人"
                            
                            new_embed.add_field(name="參加者", value=participants_text, inline=False)
                        
                        new_embed.set_footer(text=f"等待主持人開始... ({len(participants)}人參加)")
                        
                        await message.edit(embed=new_embed)
                        
                elif str(reaction.emoji) == "▶️" and user.id == interaction.user.id:
                    if len(participants) < 2:
                        await message.channel.send("❌ 至少需要2人才能開始分組", delete_after=5)
                        continue
                    
                    random.shuffle(participants)
                    
                    if team_size:
                        team_count = len(participants) // team_size
                        if len(participants) % team_size != 0:
                            team_count += 1
                    elif team_count:
                        team_size = len(participants) // team_count
                        if len(participants) % team_count != 0:
                            team_size += 1
                    else:
                        if len(participants) <= 4:
                            team_size = 2
                        elif len(participants) <= 8:
                            team_size = 4
                        else:
                            team_size = 5
                        
                        team_count = len(participants) // team_size
                        if len(participants) % team_size != 0:
                            team_count += 1
                    
                    teams = []
                    for i in range(team_count):
                        start_idx = i * team_size
                        end_idx = min((i + 1) * team_size, len(participants))
                        if start_idx < len(participants):
                            teams.append(participants[start_idx:end_idx])
                    
                    result_embed = discord.Embed(
                        title="👥 分組結果",
                        description=f"總人數：{len(participants)} 人\n"
                                   f"分組方式：{team_count} 組，每組約 {team_size} 人",
                        color=0x00FF00
                    )
                    
                    for i, team in enumerate(teams, 1):
                        members_list = "\n".join([f"{j+1}. <@{member_id}>" for j, member_id in enumerate(team)])
                        result_embed.add_field(
                            name=f"第 {i} 組 ({len(team)}人)",
                            value=members_list,
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

@tree.command(name="score_ranking", description="查看積分排行榜")
async def score_ranking_slash(interaction: discord.Interaction):
    """積分排行榜"""
    await interaction.response.defer()
    
    try:
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

@tree.command(name="blessing", description="測試今日運程")
async def blessing_slash(interaction: discord.Interaction):
    """測試運程"""
    await interaction.response.defer()
    
    try:
        # 運程選項和機率
        blessings = {
            "大吉": {"emoji": "🎉", "chance": 10, "color": 0xFFD700, "description": "今日事事順利，心想事成！"},
            "吉": {"emoji": "😊", "chance": 25, "color": 0x90EE90, "description": "今日運氣不錯，會有好事發生。"},
            "中尚": {"emoji": "😐", "chance": 40, "color": 0x87CEEB, "description": "平平安安的一天，穩紮穩打。"},
            "凶": {"emoji": "😟", "chance": 20, "color": 0xFFA500, "description": "今日需小心謹慎，避免重大決定。"},
            "大凶": {"emoji": "😨", "chance": 5, "color": 0xFF4500, "description": "諸事不順，建議低調行事。"}
        }
        
        # 隨機選擇
        choices = list(blessings.keys())
        weights = [blessings[b]["chance"] for b in choices]
        result = random.choices(choices, weights=weights, k=1)[0]
        blessing_info = blessings[result]
        
        # 生成隨機建議
        suggestions = {
            "大吉": [
                "適合進行投資或重要決策",
                "告白成功率大幅提升",
                "工作上會有意外收穫",
                "出門可能會遇到貴人"
            ],
            "吉": [
                "可以嘗試新的挑戰",
                "與朋友聚會會有驚喜",
                "學習效率特別高",
                "適合規劃未來計畫"
            ],
            "中尚": [
                "按部就班完成工作",
                "保持平常心最重要",
                "適合整理環境或思緒",
                "避免衝動消費"
            ],
            "凶": [
                "重要文件記得備份",
                "外出注意交通安全",
                "避免與人發生爭執",
                "謹慎處理財務問題"
            ],
            "大凶": [
                "盡量待在家裡休息",
                "避免簽署重要文件",
                "出門記得帶傘",
                "今天適合低調行事"
            ]
        }
        
        user_suggestions = random.sample(suggestions[result], min(3, len(suggestions[result])))
        
        # 創建嵌入訊息
        embed = discord.Embed(
            title=f"{blessing_info['emoji']} {interaction.user.name} 的今日運程",
            description=f"**{result}**\n{blessing_info['description']}",
            color=blessing_info['color']
        )
        
        # 添加詳細信息
        embed.add_field(
            name="📊 運程分析",
            value=f"**運程等級：** {result}\n"
                  f"**出現機率：** {blessing_info['chance']}%\n"
                  f"**幸運顏色：** {'金色' if result == '大吉' else '綠色' if result == '吉' else '藍色' if result == '中尚' else '橙色' if result == '凶' else '紅色'}",
            inline=True
        )
        
        embed.add_field(
            name="🕰️ 最佳時段",
            value=f"**上午：** {random.choice(['吉', '平', '凶'])}\n"
                  f"**下午：** {random.choice(['吉', '平', '凶'])}\n"
                  f"**晚上：** {random.choice(['吉', '平', '凶'])}",
            inline=True
        )
        
        # 添加建議
        suggestions_text = ""
        for i, suggestion in enumerate(user_suggestions, 1):
            suggestions_text += f"{i}. {suggestion}\n"
        
        embed.add_field(
            name="💡 今日建議",
            value=suggestions_text,
            inline=False
        )
        
        # 添加幸運數字和方向
        embed.add_field(
            name="🎲 幸運數字",
            value=f"{random.randint(1, 9)}",
            inline=True
        )
        
        embed.add_field(
            name="🧭 幸運方向",
            value=random.choice(["東", "南", "西", "北"]),
            inline=True
        )
        
        # 隨機名言
        quotes = [
            "命運負責洗牌，但玩牌的是我們自己。",
            "好運氣來自好習慣。",
            "每一天都是新的開始。",
            "心態決定狀態。"
        ]
        
        embed.set_footer(text=random.choice(quotes))
        embed.set_thumbnail(url=interaction.user.avatar.url if interaction.user.avatar else None)
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 運程測試失敗",
            description=f"錯誤：{str(e)}",
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
        
        guild_id = get_guild_id(interaction)
        
        valid_levels = ["綠箱", "藍箱", "紫箱", "金箱"]
        if box_level not in valid_levels:
            await interaction.followup.send(f"❌ 無效的寶箱等級！請選擇：{', '.join(valid_levels)}")
            return
        
        if quantity > 0:
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
            new_qty = max(current_qty + quantity, 0)
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
        
        if quantity > 0 or (quantity < 0 and new_qty > 0):
            total_qty = result['quantity']
            remaining_qty = result['remaining']
        
        # 發送結果
        color = 0x2ECC71 if quantity > 0 else 0xE74C3C
        title = "✅ 獎品操作成功" if action != "刪除" else "✅ 獎品已刪除"
        
        embed = discord.Embed(title=title, color=color)
        embed.add_field(name="獎品名稱", value=name, inline=True)
        embed.add_field(name="寶箱等級", value=box_level, inline=True)
        
        if action != "刪除":
            embed.add_field(name=f"{action}數量", value=f"{abs(quantity)} 個", inline=True)
            embed.add_field(name="總數量", value=f"{total_qty} 個", inline=True)
            embed.add_field(name="剩餘數量", value=f"{remaining_qty} 個", inline=True)
        else:
            embed.add_field(name="操作", value="已從彩池中刪除", inline=True)
            embed.add_field(name="原數量", value=f"{current_qty} 個", inline=True)
        
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
                    print(f"更新簽到訊息錯誤: {e}")
            
            # 簽到時間結束，處理簽到結果
            try:
                result = await db.fetchrow(
                    "SELECT participants FROM evaluation_events WHERE signup_message_id = $1 AND guild_id = $2",
                    signup_message.id, guild_id
                )
                
                participants = []
                if result and result['participants']:
                    participants = result['participants']
                
                # 為所有參與者發放簽到獎勵
                for user_id in participants:
                    await update_user_score(user_id, f"用戶{user_id}", SIGNUP_SCORE, f"活動簽到: {event_name}", guild_id)
                    await update_user_activity(user_id, event_name, attended=True, guild_id=guild_id)
                    await update_user_rating(user_id, "普通", guild_id)
                
                # 更新活動狀態
                await db.execute(
                    "UPDATE evaluation_events SET default_rated = $1, is_active = true WHERE signup_message_id = $2 AND guild_id = $3",
                    participants, signup_message.id, guild_id
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
                print(f"簽到結束處理錯誤: {e}")
        
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
    
    # 忽略指令訊息
    if message.content.startswith(('!', '/', bot.command_prefix)):
        await bot.process_commands(message)
        return
    
    try:
        # 添加聊天積分
        guild_id = get_guild_id(message)
        added_score, daily_limit = await add_chat_score(
            message.author.id, 
            message.author.name,
            guild_id
        )
        
        # 隨機發送鼓勵訊息（機率 1%）
        if added_score > 0 and random.random() < 0.01:
            responses = [
                f"💬 聊天 +{added_score} 分！繼續保持～",
                f"✨ 活躍獎勵 +{added_score} 分！",
                f"🎯 發言獲得 +{added_score} 分！"
            ]
            await message.channel.send(random.choice(responses), delete_after=10)
            
    except Exception as e:
        print(f"處理聊天積分錯誤: {e}")
    
    # 繼續處理其他事件
    await bot.process_commands(message)

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
                print(f"檢查管理員權限錯誤: {admin_error}")
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
                print(f"檢查管理員權限錯誤: {admin_error}")
                return
            
            result2 = await db.fetchrow(
                "SELECT participants FROM evaluation_events WHERE id = $1 AND guild_id = $2",
                event_id, guild_id
            )
            
            participants = []
            if result2 and result2['participants']:
                participants = result2['participants']
            
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
                        
                        old_rating = None
                        if str(selected_user_id) in ratings and ratings[str(selected_user_id)]:
                            old_rating = ratings[str(selected_user_id)][-1]["rating"] if ratings[str(selected_user_id)] else None
                        
                        if str(selected_user_id) not in ratings:
                            ratings[str(selected_user_id)] = []
                        
                        ratings[str(selected_user_id)].append({
                            "rater": interaction.user.id,
                            "rating": self.rating_type,
                            "time": datetime.now().isoformat()
                        })
                        
                        await db.execute(
                            "UPDATE evaluation_events SET ratings = $1 WHERE id = $2 AND guild_id = $3",
                            ratings, self.event_id, self.guild_id
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
                if user_id not in participants:
                    participants.append(user_id)
                    await db.execute(
                        "UPDATE giveaways SET participants = $1 WHERE id = $2 AND guild_id = $3",
                        participants, giveaway_id, guild_id
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
                        print(f"更新抽獎訊息錯誤: {e}")
            
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
                print(f"時間解析錯誤: {time_error}")
            
            # 修復：檢查用戶是否已經簽到
            if user_id not in participants:
                participants.append(user_id)
                
                # 更新資料庫中的參與者列表
                await db.execute(
                    "UPDATE evaluation_events SET participants = $1 WHERE id = $2 AND guild_id = $3",
                    participants, event_id, guild_id
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
                    print(f"更新簽到訊息錯誤: {e}")
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
            
            result2 = await db.fetchrow(
                "SELECT participants FROM evaluation_events WHERE id = $1 AND guild_id = $2",
                event_id, guild_id
            )
            
            if result2 and result2['participants']:
                participants = result2['participants']
                
                if user_id in participants:
                    if str(user_id) not in professions:
                        professions[str(user_id)] = profession_name
                        await db.execute(
                            "UPDATE evaluation_events SET professions = $1 WHERE id = $2 AND guild_id = $3",
                            professions, event_id, guild_id
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
        print(f"處理反應錯誤: {e}")
        import traceback
        traceback.print_exc()

# ========== 機器人上線 ==========

@bot.event
async def on_ready():
    """機器人上線"""
    print(f"\n{'='*60}")
    print(f"🤖 {BOT_NAME} - PostgreSQL 版本已上線")
    print(f"📊 伺服器數量: {len(bot.guilds)}")
    print(f"{'='*60}")
    
    # 連接 PostgreSQL
    success = await db.connect()
    if not success:
        print("⚠️ PostgreSQL 連接失敗，機器人將繼續運行但數據可能不會保存")
    
    try:
        print("\n🔄 正在同步指令...")
        global_synced = await tree.sync()
        print(f"✅ 已同步 {len(global_synced)} 個指令")
        
        print("\n📋 新增功能:")
        print("  • /blessing - 測試運程功能")
        print("  • 聊天積分系統 - 每句 +5 分，每日上限 20 分")
        print("  • PostgreSQL 數據持久化")
        print("  • 獎品增減功能修復")
        
        print("\n📊 可用指令 (14個):")
        for cmd in global_synced:
            print(f"  • /{cmd.name} - {cmd.description}")
        
    except Exception as e:
        print(f"❌ 同步失敗: {e}")
    
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name="/help 查看14個指令"
        )
    )
    
    print(f"\n🎮 機器人準備就緒！指令數: 14")

# ========== 主程式 ==========

def main():
    """主程式入口"""
    print(f"{'='*50}")
    print(f"🚀 啟動 {BOT_NAME} - PostgreSQL 持久化版本")
    print(f"💡 新增功能：測試運程、聊天積分")
    print(f"🔧 擁有者ID: {OWNER_IDS}")
    print(f"🗄️ 資料庫: PostgreSQL (Railway)")
    print(f"📊 指令數量: 14個 (10用戶 + 4管理員)")
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

if __name__ == "__main__":
    main()

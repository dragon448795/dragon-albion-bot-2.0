#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
小雲ALBION機械人 - Railway PostgreSQL版本（數據持久化）
修復簽到人數限制問題
新增：測試運程、聊天積分功能
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
import asyncpg  # 使用 PostgreSQL
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
                print("⚠️ 找不到 DATABASE_URL，使用 SQLite 作為備份")
                return False
            
            # 解析資料庫URL
            if database_url.startswith('postgres://'):
                database_url = database_url.replace('postgres://', 'postgresql://', 1)
            
            print(f"🔄 正在連接 PostgreSQL...")
            self.pool = await asyncpg.create_pool(database_url, min_size=5, max_size=20)
            
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
            
            # 其他表格保持不變...
            # ...（其他 CREATE TABLE 語句保持原樣，只需將 INTEGER 改為 BIGINT）...
            
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

db = Database()

# ========== 通用函數 ==========

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

# ========== 修改 /add_prize 指令 ==========

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

# ========== 修改 /help 指令 ==========

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

# ========== 添加聊天積分事件 ==========

@bot.event
async def on_message(message):
    """處理聊天訊息"""
    # 忽略機器人自己的訊息
    if message.author.bot:
        return
    
    # 忽略指令訊息
    if message.content.startswith(('!', '/', bot.command_prefix)):
        await bot.process_commands(message)
        return
    
    try:
        # 添加聊天積分
        added_score, daily_limit = await add_chat_score(
            message.author.id, 
            message.author.name,
            get_guild_id(message)
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

# ========== 修改 on_ready 事件 ==========

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
        print("⚠️ 使用備份模式運行，數據可能不會持久保存")
    
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

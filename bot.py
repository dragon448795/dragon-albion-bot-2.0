#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
小雲ALBION機械人 - 完整功能版本
13個指令全部可用
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
from typing import Optional, List
import sqlite3
import time

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

# ========== 積分設定 ==========
SIGNUP_SCORE = 40  # 簽到積分
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

# ========== 資料庫設定 ==========
DB_NAME = "bot_data.db"

def init_db():
    """初始化資料庫"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # 用戶資料表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        total_score INTEGER DEFAULT 0,
        current_score INTEGER DEFAULT 0,
        join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        profession_counts TEXT DEFAULT '{}',
        activity_stats TEXT DEFAULT '{}',
        rating_stats TEXT DEFAULT '{}'
    )
    ''')
    
    # 彩池表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS prize_pool (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        prize_name TEXT NOT NULL,
        box_level TEXT NOT NULL,
        quantity INTEGER DEFAULT 1,
        remaining INTEGER DEFAULT 1,
        added_by INTEGER,
        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(prize_name, box_level)
    )
    ''')
    
    # 抽獎表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS giveaways (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        creator_id INTEGER,
        prize TEXT,
        winner_count INTEGER DEFAULT 1,
        participants TEXT DEFAULT '[]',
        winners TEXT DEFAULT '[]',
        end_time TIMESTAMP,
        message_id INTEGER,
        channel_id INTEGER,
        is_active BOOLEAN DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # 積分抽獎表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS score_draws (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        creator_id INTEGER,
        score_cost INTEGER,
        box_level TEXT,
        participants TEXT DEFAULT '[]',
        winner_prize TEXT,
        winner_id INTEGER,
        is_active BOOLEAN DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # 積分轉移紀錄
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS score_transfers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_user_id INTEGER,
        to_user_id INTEGER,
        amount INTEGER,
        reason TEXT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # 評核活動
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS evaluation_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_name TEXT,
        creator_id INTEGER,
        signup_message_id INTEGER,
        profession_message_id INTEGER,
        rating_message_id INTEGER,
        channel_id INTEGER,
        participants TEXT DEFAULT '[]',
        default_rated TEXT DEFAULT '[]',
        professions TEXT DEFAULT '{}',
        ratings TEXT DEFAULT '{}',
        is_active BOOLEAN DEFAULT 1,
        start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        signup_end_time TIMESTAMP
    )
    ''')
    
    # 添加示例獎品
    sample_prizes = [
        ("普通武器", "綠箱", 20),
        ("普通裝備", "綠箱", 15),
        ("初級藥水", "綠箱", 30),
        ("中級武器", "藍箱", 10),
        ("中級裝備", "藍箱", 8),
        ("中級藥水", "藍箱", 15),
        ("高級武器", "紫箱", 5),
        ("高級裝備", "紫箱", 4),
        ("高級藥水", "紫箱", 6),
        ("傳奇武器", "金箱", 2),
        ("傳奇裝備", "金箱", 1),
        ("傳說藥水", "金箱", 3),
    ]
    
    for prize_name, box_level, quantity in sample_prizes:
        cursor.execute('''
            INSERT OR IGNORE INTO prize_pool (prize_name, box_level, quantity, remaining)
            VALUES (?, ?, ?, ?)
        ''', (prize_name, box_level, quantity, quantity))
    
    conn.commit()
    conn.close()
    print("✅ 資料庫初始化完成")

# ========== 通用函數 ==========

def get_user_score(user_id):
    """取得用戶積分"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT current_score, total_score FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    conn.close()
    
    if result:
        return result[0], result[1]
    return 0, 0

def update_user_score(user_id, username, amount, reason=""):
    """更新用戶積分"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
    if not cursor.fetchone():
        cursor.execute(
            "INSERT INTO users (user_id, username, current_score, total_score) VALUES (?, ?, ?, ?)",
            (user_id, username, max(amount, 0), max(amount, 0))
        )
    else:
        cursor.execute("UPDATE users SET current_score = current_score + ? WHERE user_id = ?", (amount, user_id))
        if amount > 0:
            cursor.execute("UPDATE users SET total_score = total_score + ? WHERE user_id = ?", (amount, user_id))
        cursor.execute("UPDATE users SET last_active = CURRENT_TIMESTAMP WHERE user_id = ?", (user_id,))
    
    if amount < 0 or reason:
        cursor.execute(
            "INSERT INTO score_transfers (from_user_id, to_user_id, amount, reason) VALUES (?, ?, ?, ?)",
            (user_id if amount < 0 else None, 
             user_id if amount > 0 else None, 
             abs(amount), 
             reason if reason else ("系統扣除" if amount < 0 else "系統增加"))
        )
    
    conn.commit()
    conn.close()

def get_user_profile(user_id):
    """獲取用戶完整資料"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT current_score, total_score, join_date, profession_counts, activity_stats, rating_stats FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    conn.close()
    
    if result:
        current_score, total_score, join_date, profession_str, activity_str, rating_str = result
        
        try:
            join_date_str = datetime.strptime(join_date.split('.')[0], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
        except:
            join_date_str = join_date
        
        profession_counts = json.loads(profession_str) if profession_str else {}
        activity_stats = json.loads(activity_str) if activity_str else {}
        rating_stats = json.loads(rating_str) if rating_str else {}
        
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

def update_user_profession(user_id, profession):
    """更新用戶職業統計"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute("SELECT profession_counts FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    
    if result:
        profession_str = result[0]
        profession_counts = json.loads(profession_str) if profession_str else {}
        
        if profession in profession_counts:
            profession_counts[profession] += 1
        else:
            profession_counts[profession] = 1
        
        bonus_score = PROFESSION_BONUS.get(profession, 0)
        if bonus_score > 0:
            cursor.execute("SELECT username FROM users WHERE user_id = ?", (user_id,))
            user_result = cursor.fetchone()
            username = user_result[0] if user_result else "未知用戶"
            
            cursor.execute("UPDATE users SET current_score = current_score + ?, total_score = total_score + ? WHERE user_id = ?", 
                         (bonus_score, bonus_score, user_id))
        
        cursor.execute("UPDATE users SET profession_counts = ? WHERE user_id = ?", 
                      (json.dumps(profession_counts), user_id))
        
        conn.commit()
    
    conn.close()

def update_user_activity(user_id, event_name, attended=True):
    """更新用戶活動統計"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute("SELECT activity_stats FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    
    if result:
        activity_str = result[0]
        activity_stats = json.loads(activity_str) if activity_str else {}
        
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
        
        cursor.execute("UPDATE users SET activity_stats = ? WHERE user_id = ?", 
                      (json.dumps(activity_stats), user_id))
        
        conn.commit()
    
    conn.close()

def update_user_rating(user_id, rating_type):
    """更新用戶評核統計"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute("SELECT rating_stats FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    
    if result:
        rating_str = result[0]
        rating_stats = json.loads(rating_str) if rating_str else {}
        
        if rating_type in rating_stats:
            rating_stats[rating_type] += 1
        else:
            rating_stats[rating_type] = 1
        
        score = RATING_SCORES.get(rating_type, 0)
        
        if score != 0:
            cursor.execute("SELECT username FROM users WHERE user_id = ?", (user_id,))
            user_result = cursor.fetchone()
            username = user_result[0] if user_result else "未知用戶"
            
            cursor.execute("""
                UPDATE users 
                SET current_score = current_score + ?, 
                    total_score = CASE 
                                    WHEN total_score + ? > 0 THEN total_score + ?
                                    ELSE 0
                                  END
                WHERE user_id = ?
            """, (score, score, score, user_id))
        
        cursor.execute("UPDATE users SET rating_stats = ? WHERE user_id = ?", 
                      (json.dumps(rating_stats), user_id))
        
        conn.commit()
    
    conn.close()

def get_current_half_month():
    """獲取當前半月期"""
    now = datetime.now()
    year_month = now.strftime("%Y-%m")
    day = now.day
    
    if day <= 15:
        return f"{year_month}-上半"
    else:
        return f"{year_month}-下半"

async def end_giveaway(message_id: int, manual: bool = False):
    """結束抽獎"""
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, creator_id, prize, winner_count, participants, winners, channel_id 
            FROM giveaways 
            WHERE message_id = ? AND is_active = 1
        """, (message_id,))
        result = cursor.fetchone()
        
        if not result:
            conn.close()
            return
        
        giveaway_id, creator_id, prize, winner_count, participants_json, winners_json, channel_id = result
        
        participants = json.loads(participants_json) if participants_json else []
        channel = bot.get_channel(channel_id)
        
        if not channel:
            conn.close()
            return
        
        try:
            message = await channel.fetch_message(message_id)
        except:
            conn.close()
            return
        
        if participants:
            if len(participants) <= winner_count:
                winners_list = participants
            else:
                winners_list = random.sample(participants, winner_count)
            
            cursor.execute("UPDATE giveaways SET winners = ?, is_active = 0 WHERE id = ?", 
                         (json.dumps(winners_list), giveaway_id))
            conn.commit()
            
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
        
        conn.close()
        
    except Exception as e:
        print(f"結束抽獎錯誤: {e}")

async def end_evaluation(event_id, channel, event_name):
    """結束評核活動"""
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT participants, professions, ratings, rating_message_id 
            FROM evaluation_events 
            WHERE id = ?
        """, (event_id,))
        result = cursor.fetchone()
        
        if not result:
            conn.close()
            return
        
        participants_json, professions_json, ratings_json, rating_message_id = result
        
        participants = json.loads(participants_json) if participants_json else []
        professions = json.loads(professions_json) if professions_json else {}
        ratings = json.loads(ratings_json) if ratings_json else {}
        
        cursor.execute("UPDATE evaluation_events SET is_active = 0 WHERE id = ?", (event_id,))
        conn.commit()
        conn.close()
        
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

# ========== 事件處理 ==========

@bot.event
async def setup_hook():
    """機器人設置鉤子"""
    print("🔄 正在設置指令樹...")
    try:
        synced = await bot.tree.sync()
        print(f"✅ 指令樹設置完成，已同步 {len(synced)} 個指令")
    except Exception as e:
        print(f"❌ 設置指令樹失敗: {e}")

@bot.event
async def on_ready():
    """機器人上線"""
    print(f"\n{'='*60}")
    print(f"🤖 {BOT_NAME} 已上線")
    print(f"📊 伺服器數量: {len(bot.guilds)}")
    print(f"{'='*60}")
    
    init_db()
    print("✅ 資料庫初始化完成")
    
    # 等待一下再同步指令
    await asyncio.sleep(2)
    
    try:
        print("🔄 正在同步指令...")
        
        # 先清除所有現有指令
        bot.tree.clear_commands(guild=None)
        
        # 重新同步全局指令
        synced = await bot.tree.sync()
        
        print(f"✅ 已同步 {len(synced)} 個指令")
        
        # 顯示可用指令
        if synced:
            print("\n📋 可用指令:")
            for cmd in synced:
                print(f"  • /{cmd.name} - {cmd.description}")
        
    except Exception as e:
        print(f"❌ 同步失敗: {e}")
        # 如果失敗，嘗試延遲後再試一次
        try:
            await asyncio.sleep(3)
            synced = await bot.tree.sync()
            print(f"✅ 重試後已同步 {len(synced)} 個指令")
        except Exception as e2:
            print(f"❌ 重試也失敗: {e2}")
    
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name="/help 查看指令"
        )
    )
    
    print(f"\n🎮 機器人準備就緒！")

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
        
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # 檢查是否為評核結束反應
        cursor.execute("""
            SELECT id, channel_id, event_name 
            FROM evaluation_events 
            WHERE rating_message_id = ? AND is_active = 1
        """, (payload.message_id,))
        rating_event = cursor.fetchone()
        
        if rating_event and emoji == RATING_END_EMOJI:
            event_id, event_channel_id, event_name = rating_event
            
            try:
                guild = channel.guild
                member = await guild.fetch_member(user_id)
                if not member.guild_permissions.administrator:
                    try:
                        await message.remove_reaction(emoji, member)
                        await channel.send(f"❌ <@{user_id}> 只有管理員可以結束評核活動！", delete_after=5)
                    except:
                        pass
                    conn.close()
                    return
            except Exception as admin_error:
                print(f"檢查管理員權限錯誤: {admin_error}")
                conn.close()
                return
            
            confirm_embed = discord.Embed(
                title="🏁 確認結束評核活動",
                description=f"你確定要結束 **{event_name}** 的評核階段嗎？",
                color=discord.Color.orange()
            )
            
            class ConfirmEndView(discord.ui.View):
                def __init__(self, event_id, channel, event_name):
                    super().__init__(timeout=60)
                    self.event_id = event_id
                    self.channel = channel
                    self.event_name = event_name
                
                @discord.ui.button(label="確定結束", style=discord.ButtonStyle.danger, emoji="✅")
                async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
                    if not interaction.user.guild_permissions.administrator:
                        await interaction.response.send_message("❌ 需要管理員權限", ephemeral=True)
                        return
                    
                    await interaction.response.defer()
                    await end_evaluation(self.event_id, self.channel, self.event_name)
                    
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
            
            view = ConfirmEndView(event_id, channel, event_name)
            await channel.send(f"<@{user_id}>", embed=confirm_embed, view=view)
            
            conn.close()
            return
        
        # 檢查是否為評核反應
        if rating_event and emoji in RATING_EMOJIS:
            event_id, event_channel_id, event_name = rating_event
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
                    conn.close()
                    return
            except Exception as admin_error:
                print(f"檢查管理員權限錯誤: {admin_error}")
                conn.close()
                return
            
            cursor.execute("SELECT participants FROM evaluation_events WHERE id = ?", (event_id,))
            result = cursor.fetchone()
            
            participants = []
            if result and result[0]:
                participants = json.loads(result[0])
            
            if not participants:
                await channel.send("❌ 沒有參與者可以評核", delete_after=5)
                conn.close()
                return
            
            print(f"活動 {event_name} 有 {len(participants)} 位參與者可以評核")
            
            class ParticipantSelectView(discord.ui.View):
                def __init__(self, participants, event_id, rating_type, channel, bot_instance):
                    super().__init__(timeout=60)
                    self.participants = participants
                    self.event_id = event_id
                    self.rating_type = rating_type
                    self.channel = channel
                    self.bot = bot_instance
                    
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
                        
                        conn = sqlite3.connect(DB_NAME)
                        cursor = conn.cursor()
                        
                        cursor.execute("SELECT ratings FROM evaluation_events WHERE id = ?", (self.event_id,))
                        result = cursor.fetchone()
                        
                        ratings = {}
                        if result and result[0]:
                            ratings = json.loads(result[0])
                        
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
                        
                        cursor.execute("UPDATE evaluation_events SET ratings = ? WHERE id = ?", 
                                     (json.dumps(ratings), self.event_id))
                        conn.commit()
                        conn.close()
                        
                        if old_rating and old_rating != self.rating_type:
                            old_score = RATING_SCORES.get(old_rating, 0)
                            update_user_score(selected_user_id, display_name, -old_score, f"評級變更: {old_rating} → {self.rating_type}")
                            print(f"移除舊評級積分: {old_rating} (-{old_score}分)")
                        
                        new_score = RATING_SCORES.get(self.rating_type, 0)
                        update_user_rating(selected_user_id, self.rating_type)
                        
                        if new_score != 0:
                            update_user_score(selected_user_id, display_name, new_score, f"活動評核: {self.rating_type}")
                            print(f"添加新評級積分: {self.rating_type} (+{new_score}分)")
                        
                        score_change = RATING_SCORES.get(self.rating_type, 0)
                        
                        if old_rating and old_rating != self.rating_type:
                            old_score = RATING_SCORES.get(old_rating, 0)
                            result_text = f"已將 <@{selected_user_id}> ({display_name}) 的評級從 **{old_rating}** ({old_score}分) 變更為 **{self.rating_type}** ({'+' if score_change > 0 else ''}{score_change}分)"
                        else:
                            result_text = f"已為 <@{selected_user_id}> ({display_name}) 評核：**{self.rating_type}** ({'+' if score_change > 0 else ''}{score_change}分)"
                        
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
            
            view = ParticipantSelectView(participants, event_id, rating_type, channel, bot)
            select_message = await channel.send(f"<@{user_id}> 請選擇要評核為 **{rating_type}** 的參與者：", view=view)
            print(f"已發送選擇視窗: message_id={select_message.id}")
            
            conn.close()
            return
        
        # 檢查是否為抽獎訊息
        cursor.execute("""
            SELECT id, participants, creator_id 
            FROM giveaways 
            WHERE message_id = ? AND is_active = 1
        """, (payload.message_id,))
        giveaway = cursor.fetchone()
        
        if giveaway:
            giveaway_id, participants_json, creator_id = giveaway
            
            if emoji == "🎫":
                participants = json.loads(participants_json) if participants_json else []
                
                if user_id not in participants:
                    participants.append(user_id)
                    cursor.execute("UPDATE giveaways SET participants = ? WHERE id = ?", 
                                 (json.dumps(participants), giveaway_id))
                    conn.commit()
                    
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
                await end_giveaway(payload.message_id, manual=True)
                await channel.send(f"⏹️ 主辦人手動結束了抽獎！")
        
        # 處理評核活動簽到
        cursor.execute("""
            SELECT id, participants, signup_end_time 
            FROM evaluation_events 
            WHERE signup_message_id = ? AND is_active = 1
        """, (payload.message_id,))
        signup_event = cursor.fetchone()
        
        if signup_event and emoji == "✅":
            event_id, participants_json, signup_end_time_str = signup_event
            
            try:
                if signup_end_time_str:
                    try:
                        signup_end_time = datetime.strptime(signup_end_time_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
                    except:
                        try:
                            signup_end_time = datetime.strptime(signup_end_time_str, '%Y-%m-%d %H:%M:%S.%f')
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
                    conn.close()
                    return
            except Exception as time_error:
                print(f"時間解析錯誤: {time_error}")
            
            participants = json.loads(participants_json) if participants_json else []
            
            if user_id not in participants:
                participants.append(user_id)
                cursor.execute("UPDATE evaluation_events SET participants = ? WHERE id = ?", 
                             (json.dumps(participants), event_id))
                conn.commit()
                
                print(f"✅ 用戶 {user_id} 成功簽到活動 {event_id}")
                
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
        
        # 處理職業選擇
        cursor.execute("""
            SELECT id, professions 
            FROM evaluation_events 
            WHERE profession_message_id = ? AND is_active = 1
        """, (payload.message_id,))
        profession_event = cursor.fetchone()
        
        if profession_event and emoji in PROFESSION_EMOJIS:
            event_id, professions_json = profession_event
            profession_name = PROFESSION_EMOJIS[emoji]
            
            cursor.execute("SELECT participants FROM evaluation_events WHERE id = ?", (event_id,))
            result = cursor.fetchone()
            
            if result and result[0]:
                participants = json.loads(result[0])
                
                if user_id in participants:
                    professions = json.loads(professions_json) if professions_json else {}
                    
                    if str(user_id) not in professions:
                        professions[str(user_id)] = profession_name
                        cursor.execute("UPDATE evaluation_events SET professions = ? WHERE id = ?", 
                                     (json.dumps(professions), event_id))
                        conn.commit()
                        
                        update_user_profession(user_id, profession_name)
                        
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
        
        conn.close()
        
    except Exception as e:
        print(f"處理反應錯誤: {e}")
        import traceback
        traceback.print_exc()

# ========== 斜槓指令 ==========

# 指令 1: help
@bot.tree.command(name="help", description="顯示幫助訊息")
async def help_slash(interaction: discord.Interaction):
    """顯示幫助"""
    embed = discord.Embed(
        title="🤖 小雲機械人 - 幫助中心",
        description="以下是可用指令列表：",
        color=0x7289DA
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
            "`/random_team` - 隨機分組"
        ),
        inline=False
    )
    
    embed.add_field(
        name="🛠️ 管理員指令",
        value=(
            "`/add_prize` - 調整彩池\n"
            "`/add_score` - 加減積分\n"
            "`/create_event` - 創建評核活動\n"
            "`/all_profiles` - 查看所有用戶資料\n"
            "`/attendance_stats` - 查看出席率統計\n"
            "`/sync` - 同步指令（擁有者）\n"
            "`/ping` - 測試機器人延遲"
        ),
        inline=False
    )
    
    embed.set_footer(text="共13個指令 | 使用 / 開頭輸入指令")
    await interaction.response.send_message(embed=embed)

# 指令 2: sync
@bot.tree.command(name="sync", description="同步斜槓指令（擁有者）")
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
        print("🔄 手動同步指令中...")
        bot.tree.clear_commands(guild=None)
        synced = await bot.tree.sync()
        
        embed = discord.Embed(
            title="🔄 指令同步完成",
            description=f"已同步 {len(synced)} 個指令到所有伺服器",
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

# 指令 3: profile
@bot.tree.command(name="profile", description="查看我的數據")
async def profile_slash(interaction: discord.Interaction):
    """查看用戶資料"""
    await interaction.response.defer()
    
    try:
        user_id = interaction.user.id
        username = interaction.user.name
        
        profile = get_user_profile(user_id)
        
        if not profile:
            conn = sqlite3.connect(DB_NAME)
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO users (user_id, username, current_score, total_score) VALUES (?, ?, ?, ?)",
                (user_id, username, 0, 0)
            )
            conn.commit()
            conn.close()
            
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
            f"**出席率：** {attendance_rate:.1f}%\n"
        )
        
        embed.add_field(
            name="📅 半月期出席率",
            value=attendance_info,
            inline=False
        )
        
        score_info = f"**當前積分：** {current_score} 分\n"
        score_info += f"**總獲得積分：** {total_score} 分\n\n"
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

# 指令 4: giveaway
@bot.tree.command(name="giveaway", description="創建抽獎活動")
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
        
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO giveaways (creator_id, prize, winner_count, end_time, message_id, channel_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (interaction.user.id, prize, winners, end_time, message.id, interaction.channel.id))
        conn.commit()
        conn.close()
        
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
                        conn = sqlite3.connect(DB_NAME)
                        cursor = conn.cursor()
                        cursor.execute("SELECT participants FROM giveaways WHERE message_id = ?", (message.id,))
                        result = cursor.fetchone()
                        participants_count = 0
                        if result and result[0]:
                            participants = json.loads(result[0])
                            participants_count = len(participants)
                        conn.close()
                        
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
            
            await end_giveaway(message.id)
        
        asyncio.create_task(countdown_timer())
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 創建抽獎失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

# 指令 5: score_draw
@bot.tree.command(name="score_draw", description="使用積分抽獎")
async def score_draw_slash(interaction: discord.Interaction):
    """積分抽獎"""
    await interaction.response.defer()
    
    try:
        current_score, _ = get_user_score(interaction.user.id)
        
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
            def __init__(self, user_id):
                super().__init__(timeout=60)
                self.user_id = user_id
            
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
                
                current_score, _ = get_user_score(interaction.user.id)
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
                
                conn = sqlite3.connect(DB_NAME)
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id, prize_name FROM prize_pool WHERE box_level = ? AND remaining > 0 ORDER BY RANDOM() LIMIT 1",
                    (selected_box,)
                )
                result = cursor.fetchone()
                
                if not result:
                    await interaction.response.send_message(f"❌ {selected_box}中沒有可用獎品！", ephemeral=True)
                    conn.close()
                    return
                
                prize_id, prize_name = result
                
                update_user_score(interaction.user.id, interaction.user.name, -score_cost, f"積分抽獎 ({selected_box})")
                cursor.execute("UPDATE prize_pool SET remaining = remaining - 1 WHERE id = ?", (prize_id,))
                
                cursor.execute('''
                    INSERT INTO score_draws (creator_id, score_cost, box_level, winner_prize, winner_id)
                    VALUES (?, ?, ?, ?, ?)
                ''', (interaction.user.id, score_cost, selected_box, prize_name, interaction.user.id))
                
                conn.commit()
                conn.close()
                
                new_current_score, _ = get_user_score(interaction.user.id)
                
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
        
        view = ScoreDrawView(interaction.user.id)
        await interaction.followup.send(embed=embed, view=view)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 抽獎失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

# 指令 6: score_transfer
@bot.tree.command(name="score_transfer", description="轉移積分給其他用戶")
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
        if amount <= 0:
            await interaction.followup.send("❌ 積分必須大於 0")
            return
        
        if user.id == interaction.user.id:
            await interaction.followup.send("❌ 不能轉移積分給自己")
            return
        
        sender_score, _ = get_user_score(interaction.user.id)
        
        if sender_score < amount:
            await interaction.followup.send(f"❌ 你的積分不足！需要 {amount} 分，你目前有 {sender_score} 分")
            return
        
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        update_user_score(interaction.user.id, interaction.user.name, -amount, f"轉移給 {user.name}")
        update_user_score(user.id, user.name, amount, f"來自 {interaction.user.name} 的轉移")
        
        cursor.execute('''
            INSERT INTO score_transfers (from_user_id, to_user_id, amount, reason)
            VALUES (?, ?, ?, ?)
        ''', (interaction.user.id, user.id, amount, reason or "無"))
        
        conn.commit()
        conn.close()
        
        new_sender_score, _ = get_user_score(interaction.user.id)
        
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

# 指令 7: prizelist
@bot.tree.command(name="prizelist", description="查看彩池列表")
async def prizelist_slash(interaction: discord.Interaction):
    """查看彩池"""
    await interaction.response.defer()
    
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT box_level, 
                   COUNT(*) as total_items,
                   SUM(remaining) as total_remaining
            FROM prize_pool 
            WHERE remaining > 0 
            GROUP BY box_level 
            ORDER BY 
                CASE box_level 
                    WHEN '金箱' THEN 1 
                    WHEN '紫箱' THEN 2 
                    WHEN '藍箱' THEN 3 
                    WHEN '綠箱' THEN 4 
                    ELSE 5 
                END
        """)
        
        results = cursor.fetchall()
        
        if not results:
            embed = discord.Embed(
                title="🎁 彩池列表",
                description="目前彩池是空的",
                color=0xFFD700
            )
            await interaction.followup.send(embed=embed)
            conn.close()
            return
        
        embed = discord.Embed(
            title="🎁 彩池列表",
            description="可用的獎品（按寶箱等級分類）：",
            color=0xFFD700
        )
        
        for box_level, total_items, total_remaining in results:
            cursor.execute("""
                SELECT prize_name, remaining 
                FROM prize_pool 
                WHERE box_level = ? AND remaining > 0 
                ORDER BY prize_name
            """, (box_level,))
            
            items = cursor.fetchall()
            
            items_text = ""
            displayed_count = 0
            hidden_count = 0
            
            for prize_name, remaining in items:
                displayed_count += 1
                if displayed_count <= 8:
                    items_text += f"• {prize_name} (剩餘: {remaining})\n"
                else:
                    hidden_count += 1
            
            if hidden_count > 0:
                items_text += f"... 還有 {hidden_count} 個獎品\n"
            
            actual_total = sum(item[1] for item in items)
            
            embed.add_field(
                name=f"{box_level} (總剩餘: {actual_total} / 獎品種類: {total_items})",
                value=items_text if items_text else "無獎品",
                inline=False
            )
        
        conn.close()
        
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

# 指令 8: random_team
@bot.tree.command(name="random_team", description="隨機分組")
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

# 指令 9: add_prize (管理員)
@bot.tree.command(name="add_prize", description="添加獎品到彩池")
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
        
        valid_levels = ["綠箱", "藍箱", "紫箱", "金箱"]
        if box_level not in valid_levels:
            await interaction.followup.send(f"❌ 無效的寶箱等級！請選擇：{', '.join(valid_levels)}")
            return
        
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        if quantity > 0:
            cursor.execute('''
                INSERT INTO prize_pool (prize_name, box_level, quantity, remaining, added_by)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(prize_name, box_level) 
                DO UPDATE SET 
                    quantity = quantity + excluded.quantity,
                    remaining = remaining + excluded.quantity
            ''', (name, box_level, quantity, quantity, interaction.user.id))
            
            action = "添加"
        elif quantity < 0:
            cursor.execute('''
                UPDATE prize_pool 
                SET quantity = quantity + ?,
                    remaining = CASE 
                                    WHEN remaining + ? > 0 THEN remaining + ?
                                    ELSE 0
                                END
                WHERE prize_name = ? AND box_level = ?
            ''', (quantity, quantity, quantity, name, box_level))
            
            if cursor.rowcount == 0:
                await interaction.followup.send(f"❌ 找不到獎品 '{name}' 在 {box_level} 中")
                conn.close()
                return
            
            action = "減少"
        else:
            await interaction.followup.send("❌ 數量不能為 0")
            conn.close()
            return
        
        cursor.execute("SELECT quantity, remaining FROM prize_pool WHERE prize_name = ? AND box_level = ?", 
                      (name, box_level))
        result = cursor.fetchone()
        
        if result:
            total_qty, remaining_qty = result
            
            embed = discord.Embed(
                title=f"✅ 獎品{action}成功",
                color=0x2ECC71 if quantity > 0 else 0xE74C3C
            )
            
            embed.add_field(name="獎品名稱", value=name, inline=True)
            embed.add_field(name="寶箱等級", value=box_level, inline=True)
            embed.add_field(name=f"{action}數量", value=f"{abs(quantity)} 個", inline=True)
            embed.add_field(name="總數量", value=f"{total_qty} 個", inline=True)
            embed.add_field(name="剩餘數量", value=f"{remaining_qty} 個", inline=True)
            embed.add_field(name="操作者", value=interaction.user.mention, inline=True)
            
            await interaction.followup.send(embed=embed)
        else:
            await interaction.followup.send(f"❌ 操作失敗")
        
        conn.commit()
        conn.close()
        
    except sqlite3.OperationalError as e:
        if "no such column" in str(e) or "no such table" in str(e):
            error_embed = discord.Embed(
                title="❌ 資料庫結構錯誤",
                description="請刪除 bot_data.db 檔案後重新啟動機器人",
                color=0xFF0000
            )
            await interaction.followup.send(embed=error_embed)
        else:
            error_embed = discord.Embed(
                title="❌ 操作失敗",
                description=f"資料庫錯誤：{str(e)}",
                color=0xFF0000
            )
            await interaction.followup.send(embed=error_embed)
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 操作失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

# 指令 10: add_score (管理員)
@bot.tree.command(name="add_score", description="調整用戶積分")
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
        
        if amount == 0:
            await interaction.followup.send("❌ 積分變化不能為 0")
            return
        
        old_score, old_total = get_user_score(user.id)
        update_user_score(user.id, user.name, amount, f"管理員調整: {reason}")
        new_score, new_total = get_user_score(user.id)
        
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

# 指令 11: create_event (管理員)
@bot.tree.command(name="create_event", description="創建評核活動")
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
        
        signup_embed = discord.Embed(
            title=f"📋 評核活動：{event_name}",
            color=discord.Color.blue()
        )
        
        if prize:
            signup_embed.add_field(name="🎁 獎品", value=prize, inline=False)
        
        signup_embed.add_field(
            name="📝 簽到階段",
            value=f"請在活動開始後 {signup_time} 分鐘內按 ✅ 簽到",
            inline=False
        )
        
        signup_embed.add_field(name="⏰ 簽到時間", value=f"{signup_time} 分鐘", inline=True)
        signup_embed.add_field(name="👥 已簽到", value="0 人", inline=True)
        signup_embed.add_field(name="⏱️ 剩餘時間", value=f"{signup_time} 分鐘", inline=True)
        signup_embed.set_footer(text=f"半月期: {get_current_half_month()}")
        
        signup_message = await interaction.followup.send(embed=signup_embed, wait=True)
        await signup_message.add_reaction("✅")
        
        class_embed = discord.Embed(
            title=f"🎮 職業選擇：{event_name}",
            description="請選擇你的職業：\n\n🛡️ 坦克\n⚔️ 输出\n💚 治疗\n💛 辅助\n\n**注意：請先完成簽到再選擇職業！**",
            color=discord.Color.green()
        )
        class_embed.set_footer(text="簽到成功後請選擇職業")
        
        class_msg = await interaction.channel.send(embed=class_embed)
        for emoji in ["🛡️", "⚔️", "💚", "💛"]:
            await class_msg.add_reaction(emoji)
        
        signup_end_time = datetime.now() + timedelta(minutes=signup_time)
        
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO evaluation_events (event_name, creator_id, signup_message_id, profession_message_id, channel_id, signup_end_time)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (event_name, interaction.user.id, signup_message.id, class_msg.id, interaction.channel.id, signup_end_time))
        conn.commit()
        conn.close()
        
        print(f"✅ 活動創建成功: {event_name}")
        
        async def signup_countdown():
            remaining_minutes = signup_time
            
            while remaining_minutes > 0:
                await asyncio.sleep(60)
                remaining_minutes -= 1
                
                try:
                    conn = sqlite3.connect(DB_NAME)
                    cursor = conn.cursor()
                    cursor.execute("SELECT participants FROM evaluation_events WHERE signup_message_id = ?", (signup_message.id,))
                    result = cursor.fetchone()
                    
                    participants_count = 0
                    if result and result[0]:
                        participants = json.loads(result[0])
                        participants_count = len(participants)
                    conn.close()
                    
                    updated_embed = discord.Embed(
                        title=f"📋 評核活動：{event_name}",
                        color=discord.Color.blue()
                    )
                    
                    if prize:
                        updated_embed.add_field(name="🎁 獎品", value=prize, inline=False)
                    
                    updated_embed.add_field(
                        name="📝 簽到階段",
                        value=f"請在活動開始後 {signup_time} 分鐘內按 ✅ 簽到",
                        inline=False
                    )
                    
                    updated_embed.add_field(name="⏰ 簽到時間", value=f"{signup_time} 分鐘", inline=True)
                    updated_embed.add_field(name="👥 已簽到", value=f"{participants_count} 人", inline=True)
                    updated_embed.add_field(name="⏱️ 剩餘時間", value=f"{remaining_minutes} 分鐘", inline=True)
                    updated_embed.set_footer(text=f"半月期: {get_current_half_month()}")
                    
                    await signup_message.edit(embed=updated_embed)
                    
                except Exception as e:
                    print(f"更新簽到訊息錯誤: {e}")
            
            try:
                conn = sqlite3.connect(DB_NAME)
                cursor = conn.cursor()
                cursor.execute("SELECT participants FROM evaluation_events WHERE signup_message_id = ?", (signup_message.id,))
                result = cursor.fetchone()
                
                participants = []
                if result and result[0]:
                    participants = json.loads(result[0])
                
                for user_id in participants:
                    update_user_score(user_id, f"用戶{user_id}", SIGNUP_SCORE, f"活動簽到: {event_name}")
                    update_user_activity(user_id, event_name, attended=True)
                    update_user_rating(user_id, "普通")
                
                cursor.execute("UPDATE evaluation_events SET default_rated = ?, is_active = 1 WHERE signup_message_id = ?", 
                             (json.dumps(participants), signup_message.id))
                conn.commit()
                conn.close()
                
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
                
                rating_embed = discord.Embed(
                    title=f"⭐ 評核階段：{event_name}",
                    description="**主持人可以按下方EMOJI調整評級**\n\n"
                              f"所有參與者已獲得預設「普通」評級（{RATING_SCORES['普通']}積分）\n",
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
                
                for emoji in ["⭐", "👍", "👌", "❌", RATING_END_EMOJI]:
                    await rating_msg.add_reaction(emoji)
                
                conn = sqlite3.connect(DB_NAME)
                cursor = conn.cursor()
                cursor.execute("UPDATE evaluation_events SET rating_message_id = ? WHERE signup_message_id = ?", 
                             (rating_msg.id, signup_message.id))
                conn.commit()
                conn.close()
                
                print(f"✅ 評核階段已創建: {event_name}, 評核訊息ID: {rating_msg.id}")
                
            except Exception as e:
                print(f"簽到結束處理錯誤: {e}")
        
        asyncio.create_task(signup_countdown())
        
        success_embed = discord.Embed(
            title="✅ 活動創建成功",
            description=f"**活動名稱：** {event_name}\n**簽到時間：** {signup_time} 分鐘\n**參與方式：** 按 ✅ 反應簽到",
            color=discord.Color.green()
        )
        
        await interaction.followup.send(embed=success_embed, ephemeral=True)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 創建活動失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

# 指令 12: all_profiles (管理員)
@bot.tree.command(name="all_profiles", description="查看所有用戶資料")
@app_commands.describe(
    sort_by="排序方式",
    limit="顯示數量"
)
@app_commands.choices(sort_by=[
    app_commands.Choice(name="現有積分(高到低)", value="current_score"),
    app_commands.Choice(name="總獲得積分(高到低)", value="total_score"),
    app_commands.Choice(name="加入日期(早到晚)", value="join_date"),
    app_commands.Choice(name="最後活躍(近到遠)", value="last_active"),
])
async def all_profiles_slash(
    interaction: discord.Interaction,
    sort_by: Optional[str] = "current_score",
    limit: Optional[int] = 20
):
    """查看所有用戶資料"""
    await interaction.response.defer()
    
    try:
        if not interaction.user.guild_permissions.administrator:
            embed = discord.Embed(
                title="❌ 權限不足",
                description="只有管理員可以查看所有用戶資料",
                color=0xFF0000
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        
        if limit > 50:
            limit = 50
        
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT user_id, username, current_score, total_score, join_date, 
                   last_active, activity_stats
            FROM users
        """)
        
        results = cursor.fetchall()
        conn.close()
        
        if not results:
            embed = discord.Embed(
                title="📊 所有用戶資料",
                description="資料庫中沒有用戶資料",
                color=0xFFFF00
            )
            await interaction.followup.send(embed=embed)
            return
        
        processed_users = []
        current_period = get_current_half_month()
        
        for row in results:
            user_id, username, current_score, total_score, join_date, last_active, activity_str = row
            
            activity_stats = json.loads(activity_str) if activity_str else {}
            
            current_period_data = activity_stats.get(current_period, {})
            total_events = current_period_data.get("total", 0)
            attended_events = current_period_data.get("attended", 0)
            current_attendance_rate = (attended_events / total_events * 100) if total_events > 0 else 0.0
            
            processed_users.append({
                "user_id": user_id,
                "username": username,
                "current_score": current_score,
                "total_score": total_score,
                "join_date": join_date,
                "last_active": last_active,
                "current_attendance_rate": current_attendance_rate,
                "total_events": total_events,
                "attended_events": attended_events,
            })
        
        sort_functions = {
            "current_score": lambda x: x["current_score"],
            "total_score": lambda x: x["total_score"],
            "join_date": lambda x: x["join_date"],
            "last_active": lambda x: x["last_active"],
        }
        
        reverse_order = {
            "current_score": True,
            "total_score": True,
            "join_date": False,
            "last_active": True,
        }
        
        sort_func = sort_functions.get(sort_by, lambda x: x["current_score"])
        reverse = reverse_order.get(sort_by, True)
        
        sorted_users = sorted(processed_users, key=sort_func, reverse=reverse)
        display_users = sorted_users[:limit]
        
        total_users = len(display_users)
        total_current_score = sum(u["current_score"] for u in display_users)
        total_total_score = sum(u["total_score"] for u in display_users)
        avg_current_score = total_current_score / total_users if total_users > 0 else 0
        
        profiles_per_page = 10
        pages = []
        
        for i in range(0, len(display_users), profiles_per_page):
            embed = discord.Embed(
                title="📊 所有用戶資料總覽",
                description=f"顯示 {min(i + profiles_per_page, len(display_users))}/{len(display_users)} 位用戶",
                color=0x43B581
            )
            
            embed.add_field(
                name="📈 統計摘要",
                value=f"**總用戶數：** {total_users} 人\n"
                      f"**總現有積分：** {total_current_score} 分\n"
                      f"**總歷史積分：** {total_total_score} 分\n"
                      f"**平均現有積分：** {avg_current_score:.1f} 分",
                inline=False
            )
            
            user_list = ""
            for user in display_users[i:i + profiles_per_page]:
                user_id = user["user_id"]
                username = user["username"]
                
                discord_user = interaction.guild.get_member(user_id)
                display_name = discord_user.display_name if discord_user else username
                
                user_list += f"**{display_name}**\n"
                user_list += f"  🔹 現有積分：{user['current_score']}分\n"
                user_list += f"  📊 總積分：{user['total_score']}分\n"
                user_list += f"  📊 出席率：{user['current_attendance_rate']:.1f}%\n"
                user_list += "  ⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n"
            
            embed.add_field(
                name="👥 用戶列表",
                value=user_list if user_list else "無用戶資料",
                inline=False
            )
            
            embed.set_footer(text=f"頁面 {i//profiles_per_page + 1}/{(len(display_users)-1)//profiles_per_page + 1}")
            pages.append(embed)
        
        if len(pages) == 1:
            await interaction.followup.send(embed=pages[0])
        else:
            current_page = 0
            
            class ProfilesPaginator(discord.ui.View):
                def __init__(self, pages, timeout=180):
                    super().__init__(timeout=timeout)
                    self.pages = pages
                    self.current_page = 0
                    self.update_buttons()
                
                def update_buttons(self):
                    self.children[0].disabled = self.current_page == 0
                    self.children[1].disabled = self.current_page == len(self.pages) - 1
                
                @discord.ui.button(label="上一頁", style=discord.ButtonStyle.secondary, emoji="⬅️")
                async def previous_page(self, interaction: discord.Interaction, button: discord.ui.Button):
                    if self.current_page > 0:
                        self.current_page -= 1
                        self.update_buttons()
                        await interaction.response.edit_message(embed=self.pages[self.current_page], view=self)
                
                @discord.ui.button(label="下一頁", style=discord.ButtonStyle.secondary, emoji="➡️")
                async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
                    if self.current_page < len(self.pages) - 1:
                        self.current_page += 1
                        self.update_buttons()
                        await interaction.response.edit_message(embed=self.pages[self.current_page], view=self)
            
            view = ProfilesPaginator(pages)
            await interaction.followup.send(embed=pages[0], view=view)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 讀取用戶資料失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

# 指令 13: attendance_stats (管理員)
@bot.tree.command(name="attendance_stats", description="查看用戶出席率統計")
@app_commands.describe(
    period="統計期間",
    min_events="最低活動次數"
)
@app_commands.choices(period=[
    app_commands.Choice(name="當前半月期", value="current"),
    app_commands.Choice(name="所有期間", value="all"),
])
async def attendance_stats_slash(
    interaction: discord.Interaction,
    period: Optional[str] = "current",
    min_events: Optional[int] = 3
):
    """查看用戶出席率統計"""
    await interaction.response.defer()
    
    try:
        if not interaction.user.guild_permissions.administrator:
            embed = discord.Embed(
                title="❌ 權限不足",
                description="只有管理員可以查看出席率統計",
                color=0xFF0000
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT user_id, username, activity_stats
            FROM users
        """)
        
        results = cursor.fetchall()
        conn.close()
        
        if not results:
            embed = discord.Embed(
                title="📊 出席率統計",
                description="資料庫中沒有用戶資料",
                color=0xFFFF00
            )
            await interaction.followup.send(embed=embed)
            return
        
        current_period = get_current_half_month()
        attendance_data = []
        
        for user_id, username, activity_str in results:
            activity_stats = json.loads(activity_str) if activity_str else {}
            
            if period == "current":
                period_data = activity_stats.get(current_period, {})
                total_events = period_data.get("total", 0)
                attended_events = period_data.get("attended", 0)
                
                if total_events >= min_events:
                    attendance_rate = (attended_events / total_events * 100) if total_events > 0 else 0.0
                    attendance_data.append({
                        "user_id": user_id,
                        "username": username,
                        "attendance_rate": attendance_rate,
                        "total_events": total_events,
                        "attended_events": attended_events,
                        "period": current_period
                    })
            
            else:  # "all"
                total_events = 0
                attended_events = 0
                
                for data in activity_stats.values():
                    total_events += data.get("total", 0)
                    attended_events += data.get("attended", 0)
                
                if total_events >= min_events:
                    attendance_rate = (attended_events / total_events * 100) if total_events > 0 else 0.0
                    attendance_data.append({
                        "user_id": user_id,
                        "username": username,
                        "attendance_rate": attendance_rate,
                        "total_events": total_events,
                        "attended_events": attended_events,
                        "period": "所有期間"
                    })
        
        attendance_data.sort(key=lambda x: x["attendance_rate"], reverse=True)
        
        total_users = len(attendance_data)
        if total_users == 0:
            embed = discord.Embed(
                title="📊 出席率統計",
                description=f"沒有找到符合條件的用戶（最低活動次數：{min_events}次）",
                color=0xFFFF00
            )
            await interaction.followup.send(embed=embed)
            return
        
        avg_attendance_rate = sum(d["attendance_rate"] for d in attendance_data) / total_users
        perfect_attendance = sum(1 for d in attendance_data if d["attendance_rate"] == 100)
        
        users_per_page = 15
        pages = []
        
        for i in range(0, len(attendance_data), users_per_page):
            embed = discord.Embed(
                title=f"📊 出席率排行榜 - {attendance_data[0]['period']}",
                description=f"顯示 {min(i + users_per_page, len(attendance_data))}/{len(attendance_data)} 位用戶",
                color=0x3498DB
            )
            
            embed.add_field(
                name="📈 統計摘要",
                value=f"**總用戶數：** {total_users} 人\n"
                      f"**平均出席率：** {avg_attendance_rate:.1f}%\n"
                      f"**全勤用戶：** {perfect_attendance} 人 (100%)",
                inline=False
            )
            
            leaderboard = ""
            for j, data in enumerate(attendance_data[i:i + users_per_page], i + 1):
                medal = "🥇 " if j == 1 else "🥈 " if j == 2 else "🥉 " if j == 3 else f"{j}. "
                
                discord_user = interaction.guild.get_member(data["user_id"])
                display_name = discord_user.display_name if discord_user else data["username"]
                
                leaderboard += f"{medal}**{display_name}**\n"
                leaderboard += f"   出席率：{data['attendance_rate']:.1f}% "
                leaderboard += f"({data['attended_events']}/{data['total_events']}次)\n"
                
                if j % 5 == 0:
                    leaderboard += "  ⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n"
            
            embed.add_field(
                name="🏆 出席率排行榜",
                value=leaderboard,
                inline=False
            )
            
            embed.set_footer(text=f"最低活動次數：{min_events}次 | 頁面 {i//users_per_page + 1}/{(len(attendance_data)-1)//users_per_page + 1}")
            pages.append(embed)
        
        if len(pages) == 1:
            await interaction.followup.send(embed=pages[0])
        else:
            class AttendancePaginator(discord.ui.View):
                def __init__(self, pages, timeout=180):
                    super().__init__(timeout=timeout)
                    self.pages = pages
                    self.current_page = 0
                    self.update_buttons()
                
                def update_buttons(self):
                    self.children[0].disabled = self.current_page == 0
                    self.children[1].disabled = self.current_page == len(self.pages) - 1
                
                @discord.ui.button(label="上一頁", style=discord.ButtonStyle.secondary, emoji="⬅️")
                async def previous_page(self, interaction: discord.Interaction, button: discord.ui.Button):
                    if self.current_page > 0:
                        self.current_page -= 1
                        self.update_buttons()
                        await interaction.response.edit_message(embed=self.pages[self.current_page], view=self)
                
                @discord.ui.button(label="下一頁", style=discord.ButtonStyle.secondary, emoji="➡️")
                async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
                    if self.current_page < len(self.pages) - 1:
                        self.current_page += 1
                        self.update_buttons()
                        await interaction.response.edit_message(embed=self.pages[self.current_page], view=self)
            
            view = AttendancePaginator(pages)
            await interaction.followup.send(embed=pages[0], view=view)
        
    except Exception as e:
        error_embed = discord.Embed(
            title="❌ 讀取出席率失敗",
            description=f"錯誤：{str(e)}",
            color=0xFF0000
        )
        await interaction.followup.send(embed=error_embed)

# 額外指令: ping
@bot.tree.command(name="ping", description="測試機器人延遲")
async def ping_slash(interaction: discord.Interaction):
    """測試延遲"""
    latency = round(bot.latency * 1000)
    
    embed = discord.Embed(
        title="🏓 Pong!",
        description=f"機器人延遲: **{latency}ms**",
        color=discord.Color.green() if latency < 100 else discord.Color.orange() if latency < 300 else discord.Color.red()
    )
    
    await interaction.response.send_message(embed=embed)

# ========== 主程式 ==========

def main():
    """主程式入口"""
    print(f"{'='*50}")
    print(f"🚀 啟動 {BOT_NAME} - 完整功能版本")
    print(f"💡 主要指令: 使用 / 前綴")
    print(f"🔧 擁有者ID: {OWNER_IDS}")
    print(f"📁 資料庫位置: {DB_NAME}")
    print(f"📋 總指令數: 13個")
    print(f"{'='*50}")
    
    # 從環境變數讀取 Token
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

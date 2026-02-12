#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
小雲ALBION機械人 - RPG 系統模組
完全獨立，不影響原有積分、抽獎、評核等任何功能
"""

import discord
from discord import app_commands
import json
import random
from datetime import datetime
import asyncpg

class RPGSystem:
    def __init__(self, bot, db, memory_cache):
        self.bot = bot
        self.db = db
        self.memory_cache = memory_cache
        print("🎮 RPG 系統實例已創建")
    
    async def initialize(self):
        """初始化 RPG 資料庫表格（完全獨立，不影響原有表格）"""
        if not self.db.is_connected:
            print("⚠️ RPG 系統：資料庫未連接，使用記憶體緩存模式")
            return
        
        try:
            async with self.db.pool.acquire() as conn:
                
                # ========== 1. 角色資料表 ==========
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS rpg_characters (
                        user_id BIGINT NOT NULL,
                        guild_id BIGINT NOT NULL DEFAULT 0,
                        
                        -- 基本資訊
                        level INTEGER DEFAULT 1,
                        exp INTEGER DEFAULT 0,
                        exp_next INTEGER DEFAULT 100,
                        
                        -- 當前狀態
                        hp INTEGER DEFAULT 100,
                        max_hp INTEGER DEFAULT 100,
                        mp INTEGER DEFAULT 50,
                        max_mp INTEGER DEFAULT 50,
                        
                        -- 基礎屬性（可分配）
                        base_vit INTEGER DEFAULT 10,  -- 體力
                        base_str INTEGER DEFAULT 10,  -- 力量
                        base_agi INTEGER DEFAULT 10,  -- 速度
                        base_int INTEGER DEFAULT 10,  -- 智慧
                        base_luck INTEGER DEFAULT 10, -- 負重
                        
                        -- 額外屬性（裝備/祝福）
                        bonus_vit INTEGER DEFAULT 0,
                        bonus_str INTEGER DEFAULT 0,
                        bonus_agi INTEGER DEFAULT 0,
                        bonus_int INTEGER DEFAULT 0,
                        bonus_luck INTEGER DEFAULT 0,
                        
                        -- 升級系統
                        unspent_stats INTEGER DEFAULT 0,
                        
                        -- 房屋
                        current_house TEXT DEFAULT '孤兒院',
                        storage_size INTEGER DEFAULT 20,
                        
                        -- 金錢
                        coins INTEGER DEFAULT 100,
                        
                        -- 時間記錄
                        created_at TIMESTAMP DEFAULT NOW(),
                        last_heal TIMESTAMP DEFAULT NOW(),
                        last_adventure TIMESTAMP,
                        
                        PRIMARY KEY (user_id, guild_id)
                    )
                ''')
                
                # ========== 2. 背包物品表 ==========
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS rpg_inventory (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        guild_id BIGINT NOT NULL DEFAULT 0,
                        
                        item_name TEXT NOT NULL,
                        item_type TEXT NOT NULL,  -- material, consumable, equipment
                        quantity INTEGER DEFAULT 1,
                        
                        -- 品質
                        rarity TEXT DEFAULT '綠',  -- 綠,藍,紫,金
                        
                        -- 裝備專用
                        equipped BOOLEAN DEFAULT false,
                        durability INTEGER DEFAULT 100,
                        max_durability INTEGER DEFAULT 100,
                        affixes JSONB DEFAULT '[]',
                        
                        added_at TIMESTAMP DEFAULT NOW(),
                        
                        -- 同一種物品（相同品質、詞條）合併
                        UNIQUE(user_id, guild_id, item_name, rarity, affixes)
                    )
                ''')
                
                # ========== 3. 裝備欄位表 ==========
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS rpg_equipment_slots (
                        user_id BIGINT NOT NULL,
                        guild_id BIGINT NOT NULL DEFAULT 0,
                        
                        -- 7個裝備欄位，儲存的是 inventory.id
                        weapon INTEGER DEFAULT NULL,
                        head INTEGER DEFAULT NULL,
                        body INTEGER DEFAULT NULL,
                        shoes INTEGER DEFAULT NULL,
                        necklace INTEGER DEFAULT NULL,
                        ring INTEGER DEFAULT NULL,
                        backpack INTEGER DEFAULT NULL,
                        
                        PRIMARY KEY (user_id, guild_id)
                    )
                ''')
                
                # ========== 4. 房屋系統表 ==========
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS rpg_houses (
                        user_id BIGINT NOT NULL,
                        guild_id BIGINT NOT NULL DEFAULT 0,
                        
                        house_type TEXT DEFAULT '孤兒院',
                        storage_items JSONB DEFAULT '[]',  -- 存放的物品ID列表
                        crafting_facilities JSONB DEFAULT '[]',  -- 已解鎖的設施
                        upgrade_progress INTEGER DEFAULT 0,
                        
                        PRIMARY KEY (user_id, guild_id)
                    )
                ''')
                
                # ========== 5. 隊伍系統表 ==========
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS rpg_parties (
                        party_id SERIAL PRIMARY KEY,
                        guild_id BIGINT NOT NULL DEFAULT 0,
                        
                        leader_id BIGINT NOT NULL,
                        members JSONB DEFAULT '[]',  -- [user_id1, user_id2, ...]
                        
                        current_map TEXT,
                        current_floor INTEGER DEFAULT 1,
                        
                        is_active BOOLEAN DEFAULT true,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                ''')
                
                # ========== 6. 拍賣行/商店表 ==========
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS rpg_shop_listings (
                        id SERIAL PRIMARY KEY,
                        guild_id BIGINT NOT NULL DEFAULT 0,
                        
                        seller_id BIGINT NOT NULL,
                        item_id INTEGER NOT NULL,  -- rpg_inventory.id
                        price INTEGER NOT NULL,
                        
                        listed_at TIMESTAMP DEFAULT NOW(),
                        expires_at TIMESTAMP DEFAULT NOW() + INTERVAL '1 day',
                        
                        is_sold BOOLEAN DEFAULT false
                    )
                ''')
                
                # ========== 7. 稱號系統表 ==========
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS rpg_titles (
                        user_id BIGINT NOT NULL,
                        guild_id BIGINT NOT NULL DEFAULT 0,
                        
                        title_name TEXT NOT NULL,
                        unlocked_at TIMESTAMP DEFAULT NOW(),
                        equipped BOOLEAN DEFAULT false,
                        
                        PRIMARY KEY (user_id, guild_id, title_name)
                    )
                ''')
                
                # ========== 8. 冒險紀錄表 ==========
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS rpg_adventure_logs (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        guild_id BIGINT NOT NULL DEFAULT 0,
                        
                        map_name TEXT,
                        floor INTEGER,
                        monster_killed TEXT,
                        items_dropped JSONB,
                        exp_gained INTEGER,
                        
                        timestamp TIMESTAMP DEFAULT NOW()
                    )
                ''')
                
                print("✅ RPG 系統：8個獨立資料表初始化完成")
                
                # 檢查表格是否成功建立
                tables = await conn.fetch("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name LIKE 'rpg_%'
                """)
                
                print(f"📊 RPG 系統：已建立 {len(tables)} 個表格")
                for table in tables:
                    print(f"   • {table['table_name']}")
                
        except Exception as e:
            print(f"❌ RPG 系統：資料庫初始化失敗 - {e}")
            import traceback
            traceback.print_exc()
    
  async def register_commands(self, tree):
    """註冊 RPG 專屬指令，並回傳註冊的指令數量"""
    
    # ========== 關鍵修正：確保 Bot 完全就緒 ==========
    await self.bot.wait_until_ready()
    
    # 建立 RPG 指令群組
    rpg_group = app_commands.Group(
        name="rpg", 
        description="🎮 RPG 冒險系統（獨立於原有功能）"
    )
    
    @rpg_group.command(name="version", description="查看 RPG 系統版本")
    async def rpg_version(interaction: discord.Interaction):
        await interaction.response.send_message(
            "🎮 RPG 系統 v0.1 - 資料庫已準備就緒",
            ephemeral=True
        )
    
    @rpg_group.command(name="status", description="檢查 RPG 資料庫狀態")
    async def rpg_status(interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        if not self.db.is_connected:
            await interaction.followup.send("❌ RPG 系統：資料庫未連接")
            return
        
        try:
            tables = await self.db.fetch("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE 'rpg_%'
            """)
            
            embed = discord.Embed(
                title="🎮 RPG 系統狀態",
                color=0x00FF00
            )
            
            embed.add_field(name="📊 RPG 表格數量", value=f"{len(tables)} 個", inline=True)
            embed.add_field(name="🔌 資料庫連接", value="✅ 正常", inline=True)
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            await interaction.followup.send(f"❌ 檢查失敗: {e}")
    
    # 將群組加到指令樹
    tree.add_command(rpg_group)
    print("✅ RPG 系統：指令群組已加入指令樹")
    
    return 1

# ========== 單例模式 ==========
_rpg_instance = None

def get_rpg_system(bot=None, db=None, memory_cache=None):
    global _rpg_instance
    if _rpg_instance is None and bot is not None:
        _rpg_instance = RPGSystem(bot, db, memory_cache)
    return _rpg_instance

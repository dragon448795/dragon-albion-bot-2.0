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

class RPGSystem:
    def __init__(self, bot, db, memory_cache):
        self.bot = bot
        self.db = db
        self.memory_cache = memory_cache
        print("🎮 RPG 系統實例已創建")
    
    async def initialize(self):
        """初始化 RPG 資料庫"""
        print("🔄 RPG 系統初始化中...")
        # 這裡才放 RPG 專屬的資料表創建
        # 不會動到原有 users, prize_pool, giveaways 等表格
        pass
    
    async def register_commands(self, tree):
        """註冊 RPG 專屬指令"""
        
        # 建立 RPG 指令群組
        rpg_group = app_commands.Group(
            name="rpg", 
            description="🎮 RPG 冒險系統（獨立於原有功能）"
        )
        
        @rpg_group.command(name="version", description="查看 RPG 系統版本")
        async def rpg_version(interaction: discord.Interaction):
            await interaction.response.send_message(
                "🎮 RPG 系統 v0.1 - 開發中",
                ephemeral=True
            )
        
        # 註冊群組
        tree.add_command(rpg_group)
        print("✅ RPG 指令註冊完成")

# 單例模式
_rpg_instance = None

def get_rpg_system(bot=None, db=None, memory_cache=None):
    global _rpg_instance
    if _rpg_instance is None and bot is not None:
        _rpg_instance = RPGSystem(bot, db, memory_cache)
    return _rpg_instance

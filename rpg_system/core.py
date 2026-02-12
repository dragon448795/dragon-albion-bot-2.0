"""
RPG 系統核心類別
"""
import discord
from discord import app_commands
import asyncpg

class RPGSystem:
    def __init__(self, bot, db, memory_cache):
        self.bot = bot
        self.db = db
        self.memory_cache = memory_cache
        print("🎮 RPG 系統核心已初始化")
    
    async def initialize(self):
        """初始化 RPG 資料庫"""
        if not self.db.is_connected:
            print("⚠️ RPG 系統：資料庫未連接")
            return
        
        try:
            async with self.db.pool.acquire() as conn:
                # === 角色資料表 ===
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS rpg_characters (
                        user_id BIGINT NOT NULL,
                        guild_id BIGINT NOT NULL DEFAULT 0,
                        level INTEGER DEFAULT 1,
                        exp INTEGER DEFAULT 0,
                        exp_next INTEGER DEFAULT 100,
                        hp INTEGER DEFAULT 100,
                        max_hp INTEGER DEFAULT 100,
                        mp INTEGER DEFAULT 50,
                        max_mp INTEGER DEFAULT 50,
                        base_vit INTEGER DEFAULT 10,
                        base_str INTEGER DEFAULT 10,
                        base_agi INTEGER DEFAULT 10,
                        base_int INTEGER DEFAULT 10,
                        base_luck INTEGER DEFAULT 10,
                        bonus_vit INTEGER DEFAULT 0,
                        bonus_str INTEGER DEFAULT 0,
                        bonus_agi INTEGER DEFAULT 0,
                        bonus_int INTEGER DEFAULT 0,
                        bonus_luck INTEGER DEFAULT 0,
                        unspent_stats INTEGER DEFAULT 0,
                        current_house TEXT DEFAULT '孤兒院',
                        storage_size INTEGER DEFAULT 20,
                        coins INTEGER DEFAULT 100,
                        created_at TIMESTAMP DEFAULT NOW(),
                        last_heal TIMESTAMP DEFAULT NOW(),
                        last_adventure TIMESTAMP,
                        PRIMARY KEY (user_id, guild_id)
                    )
                ''')
                
                # === 背包物品表 ===
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS rpg_inventory (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        guild_id BIGINT NOT NULL DEFAULT 0,
                        item_name TEXT NOT NULL,
                        item_type TEXT NOT NULL,
                        quantity INTEGER DEFAULT 1,
                        rarity TEXT DEFAULT '綠',
                        equipped BOOLEAN DEFAULT false,
                        durability INTEGER DEFAULT 100,
                        max_durability INTEGER DEFAULT 100,
                        affixes JSONB DEFAULT '[]',
                        added_at TIMESTAMP DEFAULT NOW(),
                        UNIQUE(user_id, guild_id, item_name, rarity, affixes)
                    )
                ''')
                
                # === 裝備欄位表 ===
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS rpg_equipment (
                        user_id BIGINT NOT NULL,
                        guild_id BIGINT NOT NULL DEFAULT 0,
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
                
                # === 房屋表 ===
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS rpg_houses (
                        user_id BIGINT NOT NULL,
                        guild_id BIGINT NOT NULL DEFAULT 0,
                        house_type TEXT DEFAULT '孤兒院',
                        storage_items JSONB DEFAULT '[]',
                        crafting_facilities JSONB DEFAULT '[]',
                        upgrade_progress INTEGER DEFAULT 0,
                        PRIMARY KEY (user_id, guild_id)
                    )
                ''')
                
                # === 隊伍表 ===
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS rpg_parties (
                        party_id SERIAL PRIMARY KEY,
                        guild_id BIGINT NOT NULL DEFAULT 0,
                        leader_id BIGINT NOT NULL,
                        members JSONB DEFAULT '[]',
                        current_map TEXT,
                        current_floor INTEGER DEFAULT 1,
                        is_active BOOLEAN DEFAULT true,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                ''')
                
                print("✅ RPG 系統：資料表初始化完成")
                
        except Exception as e:
            print(f"❌ RPG 系統：資料庫初始化失敗 - {e}")

# === 單例模式 ===
_rpg_instance = None

def get_rpg_system(bot=None, db=None, memory_cache=None):
    global _rpg_instance
    if _rpg_instance is None and bot is not None:
        _rpg_instance = RPGSystem(bot, db, memory_cache)
    return _rpg_instance

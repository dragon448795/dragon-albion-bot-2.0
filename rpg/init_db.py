"""
RPG 資料庫初始化腳本
"""
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

async def init_rpg_tables():
    """初始化 RPG 資料庫表格"""
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        print("❌ 找不到 DATABASE_URL 環境變數！")
        return False
    
    try:
        # 修正URL格式
        if database_url.startswith('postgres://'):
            database_url = database_url.replace('postgres://', 'postgresql://', 1)
        
        conn = await asyncpg.connect(database_url)
        
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
        
        print("✅ RPG 資料庫表格初始化完成")
        await conn.close()
        return True
        
    except Exception as e:
        print(f"❌ RPG 資料庫初始化失敗: {e}")
        return False

if __name__ == "__main__":
    import asyncio
    asyncio.run(init_rpg_tables())
